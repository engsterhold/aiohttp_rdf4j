import logging
import collections
import datetime
import asyncio
import unittest
import os
from subprocess import call
from pathlib import Path
from rdflib import URIRef, Literal, Graph, ConjunctiveGraph, Dataset
from aiohttp_rdf4j.aiograph import  AioRDF4jServer
from aiohttp_rdf4j.utils import async_fill_graph
from contextlib import contextmanager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logger.info("uvloop activated")
except ImportError:
    logger.info("uvloop not activated")

THIS_DIR = Path(__file__).parent

STATEMENTS, TYPE = os.path.join(THIS_DIR, "geoStatements.trig"), "trig"
REPO = "aiotest"


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

def setup_vagrant():
    try:
        aios =  AioRDF4jServer(host="192.168.33.10", port=8080, location="rdf4j-server")
        aios.ping()
        return aios
    except Exception as why:
        logger.exception(why)
        with cd(os.path.join(THIS_DIR, "rdf4j-vagrant")) as va:
            call(["vagrant", "box", "update"])
            call(["vagrant", "provision"])
            call(["vagrant", "up"])
        return AioRDF4jServer(host="192.168.33.10", port=8080, location="rdf4j-server")



def setup_graphdb():
    aios = AioRDF4jServer()
    try:
        aios.ping()
        return aios
    except Exception as why:
        logger.exception(why)
        raise Exception


def setdown_vagrant():
    with cd(os.path.join(THIS_DIR, "rdf4j-vagrant")) as va:
        call(["vagrant", "halt"])



def load_statements():

    a = datetime.datetime.now()
    logger.info(f"start loading ds at: {a}")
    ds = Dataset()
    ds.parse(STATEMENTS, format=TYPE)
    b = datetime.datetime.now()
    logger.info(f"finished loading ds at: {b}")
    logger.info(f"ds loaded: {ds}")
    logger.info(f"ds loaded in {b - a}")
    return ds

def fill_repository(server, ds, repo):
    a = datetime.datetime.now()
    repo = server.repository(repo)

    def it():
        for i in ds.quads((None, None, None, None)):
            #print(i)
            yield i

    async def f():
        await repo.addBatch(it(), batch_size=1000)

    loop = asyncio.get_event_loop()

    logger.info(f"start filling repository at: {a}")

    loop.run_until_complete(asyncio.gather(f()))

    logger.info(f"finished filling repository at: {a}")

    loop.close()
    b = datetime.datetime.now()
    logger.info(f"loaded in {b - a}")


if __name__ == "__main__":

    server = setup_vagrant()
    #server = setup_graphdb()

    ds = load_statements()
    fill_repository(server, ds, REPO)


