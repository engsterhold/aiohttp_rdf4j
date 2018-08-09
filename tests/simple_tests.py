import logging
import collections
import datetime
import asyncio
import unittest
from subprocess import call
from pathlib import Path
from rdflib import URIRef, Literal, Graph, ConjunctiveGraph, Dataset
from aiohttp_rdf4j.aiograph import AioRDF4jStore, AioRDF4jServer
from aiohttp_rdf4j.utils import async_fill_graph
logger = logging.getLogger(__name__)
logger.setLevel(1)
logger.addHandler(logging.StreamHandler())

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logger.info("uvloop activated")
except ImportError:
    logger.info("uvloop not activated")

THIS_DIR = Path(__file__).parent


from contextlib import contextmanager
import os

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

def setup_vagrant():
    with cd(os.path.join(THIS_DIR, "rdf4j-vagrant")) as va:
        #call(["vagrant", "provision"])
        call(["vagrant", "up"])
        #AioRDF4jServer(host="192.168.33.10", port=8080, location="rdf4j-server")
    return AioRDF4jServer()


class SimpleAIOServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.vagrant_server = setup_vagrant()
        cls.aiotest = cls.vagrant_server.repository("aiotest")
        cls.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    def test_simple(self):
        a = datetime.datetime.now()
        seed = [(URIRef(f"urn:example.com/mock/id{i}"), URIRef(f"urn:example.com/mock/rel{i}"),
                 Literal(f"mock-val{i}"), URIRef(f"urn:example.com/mock/context{j}")) for i in range(100) for j in
                range(100)]

        async def seed_store():
            await self.aiotest.addN(seed)

        g, cg, ds = Graph(), ConjunctiveGraph(), Dataset(default_union=True)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(seed_store()))

        b = datetime.datetime.now()
        print("seed time ->", b - a)

        async def f():
            for i in (g, cg, ds):
                await async_fill_graph(i, self.aiotest.statements())

        loop.run_until_complete(asyncio.gather(f()))

        for i in (g, cg, ds):
            print(len(i))

        # print("g", [i for i in g])
        # print("cg", [i for i in cg])
        # print("ds", [(i, g.identifier) for i in g for g in ds.graphs()])

        c = datetime.datetime.now()
        print("graph time ->", c - b)

        print("complete time ->", c - a)

    #@unittest.skip("Bla")
    def test_load_from_file(self):

        ds = Dataset()
        ds.parse("geoStatements.trig", format="trig")
        async def f():
            await self.aiotest.addN((i for i in ds.quads((None, None, None, None))))

        print("ds loaded")
        self.loop.run_until_complete(asyncio.gather(f()))



