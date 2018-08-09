import logging
import collections
import datetime
import asyncio
import unittest
import os
from subprocess import call
from pathlib import Path
from rdflib import URIRef, Literal, Graph, ConjunctiveGraph, Dataset
from aiohttp_rdf4j.aiograph import AioRDF4jStore, AioRDF4jServer
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


async def async_gen(iterable):
    """
    Is this non blocking?
    :param iterable: a iterable without __aiter__
    :return: an async generator for the iterable
    """

    for i in  iterable:
        await asyncio.sleep(0)
        yield i



def ml(l):
 return [(i) for i in range(l)]

l1 =ml(1000)
l2 =ml(10000)
l3 = ml(500)

async def f(l, id):
    async for i in async_gen(l):
         if i%10 == 0:
             print(i, id)
    #print("finished", id)
    print(i, id)





loop = asyncio.get_event_loop()

loop.run_until_complete(asyncio.gather(f(l1,1), f(l2, 2), f(l3, 3)))

loop.close()

