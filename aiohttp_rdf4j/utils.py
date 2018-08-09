
import collections
from functools import partial
from rdflib import Graph, ConjunctiveGraph, Dataset

class Statement(collections.namedtuple("StatementBase", "s p o c")):
    """
    Statement wrapper class. A convenient class to mask the graph <- > dataset question somewhat
    This class is based on a namedtuple (s p o c), whereas c = context is optional (None by default)
    you can access the variables via attribute, tuple or dictionary style access.
    or you can get a triple (s, p, o) (ignoring context) or quad (s, p, o, c)
    you can also get a wrapped ((s, p, o), c) representation for legacy reason, this is also the default unwrap
    ((s, p, o),c) = statement. Perhaps this will change.
    the len(statement) is 3 if context is None, otherwise 4
    For construction it needs valid rdflib objects
    (URIRef or BNode for subject, predicate or context, URIRef, BNode, Literal for object)
    There is an assert in __new__, but perhaps i will remove it und just assume everything is ok.
    I mean it should not get a wrong param in the first place and will fail down the road.

    """
    __slots__ = ()

    def __new__(cls, s, p, o, c=None):
        #assert isinstance(s, (URIRef, BNode)), f"{s}: {type(s)} -> not a valid subject"
        #assert isinstance(p, (URIRef, BNode)), f"{p}: {type(p)} -> not a valid predicate"
        #assert isinstance(o, (URIRef, BNode, Literal)), f"{o}: {type(o)} -> not a valid object"
        #if c is not None:
        #    assert isinstance(c, (URIRef, BNode)), f"{c}: {type(c)} -> not a valid context"
        return super().__new__(cls, s, p, o, c)

    @classmethod
    def _make(cls, iterable):
        if len(iterable)==2:
            return cls(*iterable[:2])
        elif len(iterable) == 3:
            return cls(*iterable+(None,))
        elif len(iterable) == 4:
            return cls(*iterable)
        else:
            raise TypeError("Iterable not in a parseable format")


    @property
    def subject(self):
        return self.s

    @property
    def predicate(self):
        return self.p

    @property
    def object(self):
        return self.o

    @property
    def context(self):
        return self.c

    @property
    def triple(self):
        return (self.s, self.p, self.o)

    @property
    def quad(self):
        return (self.s, self.p, self.o, self.c)

    @property
    def wrapped(self):
        return ((self.s, self.p, self.o), self.c)

    def __iter__(self):
        return (i for i in self.wrapped)

    def __len__(self):
        if not self.context:
            return 3
        else:
            return 4

    def __repr__(self):
        return str(self.wrapped)

    def __str__(self):
        if len(self) == 3:
            return f"({self.s}, {self.p}, {self.o})"
        else:
            return f"({self.s}, {self.p}, {self.o}, {self.c})"


    def __getitem__(self, name):
        """
        :param name: either the interger (0..2) position, s,p,o,c or subject, predicate, object, context
        similar to the rdflib.query.ResultSet
        :return: the accessed element of the statement or KeyError
        """
        if isinstance(name, int) and name < len(self._fields):
            return super().__getitem__(name)
        elif isinstance(name, str) and (name in self._fields or name in ("subject", "predicate", "object", "context")):
            return getattr(self, name)
        else:
            raise KeyError(name)


QUAD = 2
TRIPLE = 1
async def async_fill_graph(graph, stmt_gen, loop=None, executor=None):
    """
    Convinient function to fill a graph/Dataset with the statements returned from the async generator
    The add method itself is still blocking, because it is the default rdflib method.
    This function is mainly meant to work with the default iomemory store,
    as an easy way to access small rdf fragments created with a construct query for a "page" view
    I dont know hwo this will interact with an blocking store backend. Perhaps i check for this case and put it in
    an thread executor on this case
    :param graph: the memory graph, concunctive graph or dataset to be filled
    :param stmt_gen: the aiostore statement generator or construct query
    :return: a filled graph
    """
    if isinstance(graph, (ConjunctiveGraph, Dataset)):
        flag = QUAD
    elif isinstance(graph, Graph):
        flag = TRIPLE
    else:
        raise TypeError(f"{graph} is neither a Dataset, ConjunctiveGraph or Graph")

    def add_one(statment):
        if flag == QUAD:
            graph.add(statment.quad)
        elif flag == TRIPLE:
            graph.add(statment.triple)
    if loop is not None:
        async for stmt in stmt_gen:
            await loop.run_in_executor(executor, add_one, stmt)
    else:
        async for stmt in stmt_gen:
            add_one(stmt)

    return graph





