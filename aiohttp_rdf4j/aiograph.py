import asyncio
import logging
import aiohttp.client
from multidict import MultiDict
from rdflib import Literal, URIRef, BNode, Graph
from rdflib.plugin import Store
from rdflib.plugins.serializers.nt import _quoteLiteral
from aiohttp_rdf4j.utils import Statement
from aiohttp_rdf4j.parser import BinaryRDFParser
from aiohttp_rdf4j.parser import BinaryQueryResultParser
from typing import (Optional)
from functools import wraps


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())




NULL = "null"  # the 'null' graph in an rdf4j repository


def _trig_fragment(triple, context=None):
    return f'{context.n3() if context else ""} {{\n{triple[0].n3()} {triple[1].n3()} {_quoteLiteral(triple[2]) if isinstance(triple[2], Literal) else triple[2].n3()} .\n}}'.encode()


async def trig_generator(stream, context=None):
    async for s in stream:
        if not isinstance(s, Statement):
            s = Statement._make(s)
        triple, ctx = s
        x = _trig_fragment(triple, ctx if context is None else context)
        #logger.debug(x)
        yield x


async def async_gen(iterable, interupt=100):
    """
    Is this non blocking?
    :param iterable: a iterable without __aiter__
    :return: an async generator for the iterable
    """
    interupt = 1 if interupt < 1 else interupt
    for n, i in enumerate(iterable):
        if n%interupt==0:
            await asyncio.sleep(0) # not sure. but otherwise the loop does not get interrupted.
            # perhaps is is not needed because the shit is so fast.
        yield i


def runner(coro):
    """Function execution decorator."""

    @wraps(coro)
    def inner(self, *args, **kwargs):
        if self.mode == 'async':
            return coro(self, *args, **kwargs)
        return self._loop.run_until_complete(coro(self, *args, **kwargs))

    return inner


def repair_context(context, as_n3=False):
    """
    Because of some wonky mechanics in rdflib, context can either be a graph,
    a URIRef or a BNode. This makes sure that the return value is a URIRef, Bnode or the rdf4j special param "null"
    :param context:  the context to check
    :param as_n3: returns the the context identifer as a n3 string for use in GET Params.
                    This is needed, because of the special 'null' param, we need to address the default graph.
    """
    if context is None:
        return None  # check
    elif context == "null":
        return "null" if as_n3 else None
    elif isinstance(context, Graph):
        return repair_context(context.identifier)
    elif isinstance(context, (URIRef, BNode)):
        return context.n3() if as_n3 else context
    else:
        raise TypeError(f"{context} is not neither None, 'null' nor an URIRef, BNode")


class AioRDF4jServer():
    """
    unfinished
    """

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 7200,
                 location: str = "",
                 full_url: str = "",
                 *,
                 init_rep=True,
                 executor=None,
                 loop: Optional[asyncio.BaseEventLoop] = None,
                 session=None,
                 ssl: bool = False,
                 username: Optional[str] = None,
                 password: Optional[str] = None,

                 ):

        self._location = location
        self._host = host
        self._port = port
        self._ssl = ssl
        if not full_url:
            self._base_url = f'{"https" if ssl else "http"}://{host}:{port}{"/"+location if location else ""}'
        else:
            self._base_url = full_url
        logger.debug(f"Set Rdf4J Server at {self._base_url}")

        self._loop = asyncio.get_event_loop() if loop is None else loop
        self._session = aiohttp.ClientSession(loop=self._loop) if session is None else session
        self._executor = executor
        self.version = self._loop.run_until_complete(self._version())
        logger.debug(f"Rdf4J protocol version: {self.version}")
        if init_rep:
            self.repositories = self._init_repositories()
            logger.debug(f"repositories loaded: {', '.join(i for i in self.repositories)}")
        self._active_repositories = {}

    async def _version(self):
        url = f"{self._base_url}/protocol"
        resp = await  self._session.get(url)
        return int(await resp.text())

    def _init_repositories(self):
        r = {}
        reps = self._loop.run_until_complete(self.fetch_repositories())
        for i in reps:
            r[str(i.id)] = AioRDF4jStore(self._host, self._port, self._location, i.id,
                                         loop=self._loop, session=self._session, executor=self._executor,
                                         ssl=self._ssl
                                         )
        return r

    def repository(self, id, **kwargs):

        xkwargs = dict(host=self._host, port=self._port,
                       location=self._location, repository=id,
                       loop=self._loop, session=self._session, executor=self._executor,
                       ssl=self._ssl)
        xkwargs.update(kwargs)
        return AioRDF4jStore(**xkwargs)

    async def fetch_repositories(self):
        url = f"{self._base_url}/repositories"
        header = {"ACCEPT": "application/x-binary-rdf-results-table"}
        resp = await  self._session.get(url, headers=header)
        bqr = BinaryQueryResultParser(resp.content)
        await bqr._init()
        q = set()
        async for i in bqr:
            q.add(i)
            logger.debug(f"Repository with ID: {i.id} available at {i.uri}")
        return q


    def ping(self):
        return self._loop.run_until_complete(self._version())


class AioRDF4jStore(Store):

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 7200,
                 location: str = "",
                 repository: str = "dummy",
                 mode: str = 'async',
                 infer: bool = True,
                 *,
                 executor=None,
                 session=None,
                 ssl: bool = False,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 loop: Optional[asyncio.BaseEventLoop] = None
                 ):

        self._location = location
        self._host = host
        self._port = port
        self._repository = repository
        self._mode = mode
        self._infer = infer

        self._base_url = f'{"https" if ssl else "http"}://{host}:{port}{"/"+location if location else ""}'
        self._loop = asyncio.get_event_loop() if loop is None else loop
        self._session = aiohttp.ClientSession(loop=self._loop)
        self._executor = executor
        self._init_rest_interface()
        logger.info("Service Points available under:\n{}" \
                    .format("\n".join(f"{k}: {v}" for k, v in self.rest_services.items())))

        _size = None
        _namespaces = {}

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, mode):
        if mode not in ('async', 'blocking'):
            raise ValueError('Invalid running mode')
        self._mode = mode

    def _init_rest_interface(self):
        self.rest_services = {}
        # self.rest_services["protocol"] = base_url + "/protocol"
        repo = self._base_url + "/repositories"
        self.rest_services["repository"] = repo + "/" + self._repository
        self.rest_services["statements"] = self.rest_services["repository"] + "/statements"
        self.rest_services["contexts"] = self.rest_services["repository"] + "/contexts"
        self.rest_services["size"] = self.rest_services["repository"] + "/size"
        self.rest_services["transaction"] = self.rest_services["repository"] + "/transactions"
        self.rest_services["namespaces"] = self.rest_services["repository"] + "/namespaces"

    async def size(self):
        async with self._session.get(self.rest_services["size"])  as resp:
            return int(await resp.text())

    async def subjects(self, p=None, o=None, context=None, infer=None, **kwargs):
        triple = (None, p, o)
        async for i in self.statements(triple, context, infer, **kwargs):
            yield i.s

    async def predicates(self, s=None, o=None, context=None, infer=None, **kwargs):
        triple = (s, None, o)
        async for i in self.statements(triple, context, infer, **kwargs):
            yield i.p

    async def objects(self, s=None, p=None, context=None, infer=None, **kwargs):
        triple = (s, p, None)
        async for i in self.statements(triple, context, infer, **kwargs):
            yield i.o

    async def quads(self, quads=(None, None, None, None), infer=None, **kwargs):
        stm = Statement._make(quads)
        async for i in self.statements(stm.triple, stm.c, infer, **kwargs):
            yield i.quad

    async def triples(self, triple=(None, None, None), context=None, infer=None, **kwargs):
        async for i in self.statements(triple, context, infer, **kwargs):
            yield i.triple

    async def statements(self, triple=(None, None, None), context=None, infer=None, **kwargs):
        """
        An async generator over statements and contexts in a RDF4J Triple store.
        This returns the triples in a streaming matter. Which allows direct processing of the incoming data,
        but does not allow sorting or slicing.
        THIS IS AN ASYNC GENERATOR
        :param triple_pattern: an rdflib triple pattern for (S, P, O)
        :param context: the context. If None (default) the default graph is used.
        Which usually means the union of all graphs is iterated.
        Context should be an URI or but can also accept a rdflib.Graph.
        List/set/tuple of URIs/Graphs are also accepted.
        :param infer: Overwrites the default infer parameter.
        :param kwargs: Get merged with the POST kwargs. Overwrites already set kwargs.
        Uses this to add a custom timeout=X (X=0 by default), for example.
        The keyword "headers" is removed from kwargs.
        :return: An async generator of :class Statements ((S,P,O), C)
        """
        #_ = kwargs.pop("params", None)
        _ = kwargs.pop("header", None)
        url = self.rest_services["statements"]
        header = {"ACCEPT": "application/x-binary-rdf"}
        infer = self._infer if infer is None else infer
        assert type(infer) == bool, f"infer ist not a boolean: {infer}"
        s, p, o = triple
        params = MultiDict(dict(infer=str(infer).lower()))
        if s:
            assert isinstance(s, (URIRef, BNode)), f"{s}: {type(s)} -> not a valid subject"
            params["subj"] = s.n3()
        if p:
            assert isinstance(p, (URIRef, BNode)), f"{p}: {type(p)} -> not a valid predicate"
            params["pred"] = p.n3()
        if o:
            assert isinstance(o, (URIRef, BNode, Literal)), f"{o}: {type(o)} -> not a valid object"
            params["obj"] = o.n3()
        if context:
            if isinstance(context, (set, list, tuple)):
                params.update([("context", repair_context(i, as_n3=True)) for i in context])
            else:
                params.update([("context", repair_context(context, as_n3=True))])
        # params.update([("context", 'null')])
        xkwargs = dict(params=params, timeout=0, headers=header)
        xkwargs.update(kwargs)
        logger.debug(f"GET params are: {xkwargs}")
        resp = await self._session.get(url, **xkwargs)
        async for i in BinaryRDFParser(resp.content):
            yield i

    async def query(self, query, initNs=None, initBindings=None, queryGraph=None, infer=None, **kwargs):
        """
        Queries an Rdf4J Endpoint via a post request. The results are returned as
        <application/x-binary-rdf-results-table> for select queries,
        <application/x-binary-rdf> for construct queries or
        <text/boolean> for ask queries.
        THIS IS A ASYNC GENERATOR
        Construct and Select queries are parsed by an python conversation of the corresponding RIO parsers.
        ASK yields either TRUE or FALSE
        :param query: a valid sparql query.
        :param initNs: ignored.
        :param initBindings: bindings for variables in query as a dict (keys are without ?, values are URIRef or Literal)
        :param queryGraph: ignored right now.
        :param infer: Overwrites the default infer parameter.
        :param kwargs: get merged with the POST kwargs. Overwrites already set kwargs.
        Uses this to add a custom timeout=X (X=0 by default) for example.
        The keyword "data" and "headers" are removed from kwargs.
        :return: ASK: TRUE or FALSE. QUERY: async generator of ResultRow for variables,
        CONSTRUCT: async generator of  ((S,P;O), C) <- need to be improved
        """
        _ = kwargs.pop("data", None)
        _ = kwargs.pop("header", None)
        url = self.rest_services["repository"]
        infer = self._infer if infer is None else infer
        assert type(infer) == bool, f"infer ist not a boolean: {infer}"
        # timeout = kwargs.get('timeout',"0")
        data = {}
        if isinstance(initBindings, dict):
            data = {"$" + k: v.n3() for k, v in initBindings.items()}
        elif initBindings is not None:
            logger.warning(f"Bindings could not get passed to query data. {initBindings} is not a dict")
        data["infer"] = str(infer).lower()
        data["query"] = query
        if initNs:
            logger.info("keyword initNS is not yet supported. Passed values are ignored.")
        header = {"ACCEPT": "application/x-binary-rdf-results-table,application/x-binary-rdf,text/boolean"}
        logger.debug(f"Query Data: {data}")
        xkwargs = dict(data=data, timeout=0, headers=header)
        xkwargs.update(kwargs)
        logging.debug(f"POST params are: {xkwargs}")

        resp = await self._session.post(url, **xkwargs)

        logger.debug(f"Query respond headers: {resp.headers}")
        if resp.headers['Content-Type'].startswith('application/x-binary-rdf-results-table'):
            bqr = BinaryQueryResultParser(resp.content)
            await bqr._init()
            async for i in bqr:
                yield i
        elif resp.headers['Content-Type'].startswith('application/x-binary-rdf'):
            async for i in BinaryRDFParser(resp.content):
                yield i
        elif resp.headers['Content-Type'].startswith('text/boolean'):
            yield bool(await  resp.text())
        else:
            ar = await resp.text()
            raise ValueError(f"Response content type not parseable {ar}")

    async def _new_transaction(self):
        """
        Starts a new transaction
        :return: the transaction url
        """

        url = self.rest_services["transaction"]
        resp = await self._session.post(url)
        trx = resp.headers["location"]
        return trx

    async def _rollback(self, trx):
        """
        TODO this need to be expanded.
        Why did the transaction rolled back
        :param trx: the transaction id
        :return: raise an IO Error (placeholder)
        """
        resp = await self._session.delete(trx)
        err = await resp.text()
        logger.exception(err)
        raise IOError(f"transaction aborted, rolled back: {err}")  # placeholder

    async def _commit(self, trx):
        """
        Commits the transaction with id= trx
        :param trx: the transaction id
        :return: should return the reponse code, I guess?
        """
        resp = await self._session.put(trx, params={"action": "COMMIT"})
        return resp

    async def addBatch(self, quads, context=None, batch_size=10000):
        """
        adds triple in a batch to the store. This is needed, because if you make use of heavy inference
        the store might not be able to process a huge (1mil+) amount of added statments.
        :param quads: the quad stream, iterable
        :param context: the optional context, overwrites context in the quads
        :param batch_size: the batch size, defaults to 10000. for owl2 rl inference 1000 or even 100 might be better
        :return: 200 if every thing is ok, otherwise it raise an IOError (placeholder)
        """

        if not hasattr(quads, "__aiter__"):
            quads = async_gen(quads)

        async def gen_batch():
            batch = []
            n = 0
            async for i in quads:
                n = n + 1
                batch.append(i)
                if n % batch_size == 0:
                    # print("i yield", n, context)
                    yield n, batch
                    batch = []
            yield n, batch


        async for n, i in gen_batch():
            resp = await self.addN(i, context)
            if resp != 200:
                raise IOError(f"batch add aborted at batch {n}, rolled back: {resp}")  # placeholder
        return 200

    async def addN(self, quads, context=None):
        """
        Adds an async stream of triple / quads to an RDF4J Triple Store.
        the format for the input stream is ((S,P,O), C) right now.
         If C is None the triples are added to the default update graph. <- this could be solved better
        :param quads: a async iterable of the form ((S,P,O), C)
        :param context: either a URIRef or a BNode, overwrites any context given in the quad stream including the None context
        :return: the http status: 200 if the commit was successful, else the error response code (and does a rollback)
        """

        params = {"action": "ADD"}
        headers = {"Content-Type": "application/x-trig"}
        if context:
            assert isinstance(context, (URIRef, BNode)), f"context is neither a URIRef nor Bnode: {context}"
        if not hasattr(quads, "__aiter__"):
            quads = async_gen(quads)

        trx = await  self._new_transaction()
        resp = await self._session.put(trx, data=trig_generator(quads, context), headers=headers, params=params, timeout=0)
        if resp.status == 200:
            cm = await self._commit(trx)
            if cm.status == 200:
                return cm.status
            else:
                err = await cm.text()
                logging.debug(f"transaction rolled back during commit: {err}")
                await self._rollback(trx)
                return cm.status
        else:
            err = await resp.text()
            logging.debug(f"transaction rolled back: {err}")
            await self._rollback(trx)
            return resp.status

    async def add(self, spo, context, quoted=False):
        """
        Adds a single triple to a context in a RDF4J triple store with a transaction
        :param spo: The triple
        :param context: The context, a URIRef or a Graph
        :param quoted: ignored
        :return:
        """
        # s, p, o = spo
        params = {"action": "ADD", "context": repair_context(context, as_n3=True)}
        headers = {"Content-Type": "application/x-trig"}
        # data = f"{s.n3()} {p.n3()} {o.n3()} ."
        data = _trig_fragment(spo)
        # data = data.encode("utf8")
        trx = await self._new_transaction()
        resp = await self._session.put(trx, data=data, headers=headers, params=params, timeout=0)
        if resp.status == 200:
            cm = await self._commit(trx)
            if cm.status == 200:
                return cm.status
            else:
                await self._rollback(trx)
                return cm.status
        else:
            await self._rollback(trx)
            return resp.status

    def update(self, update, initNs, initBindings, queryGraph, **kwargs):
        raise NotImplementedError

    @runner
    async def ping(self):
        query = "ask where {?s ?p ?o} limit 1"
        return await self.query(query)

    def __len__(self):
        """
        This is a blocking len(self) operation
        :return:
        """
        return self._loop.run_until_complete(self.size())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __del__(self):
        if not self._loop.is_closed() and self._session:
            asyncio.ensure_future(self._session.close(), loop=self._loop)

    def __repr__(self):
        return f'{type(self).__name__}'

    @runner
    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None


#############################################
