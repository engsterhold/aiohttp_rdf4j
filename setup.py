from setuptools import setup, find_packages
import sys
import os

version = '0.5.1'


def readme():
    dirname = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(dirname, "README.txt")
    return open(filename).read()

setup(name='aiohttp_rdf4j',
      version=version,
      description="An async (aiohttp) rdflib like Warpper for a RDF4J Server",
      long_description=readme(),
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[],
      keywords='rdf, aiohttp, rdf4j',
      author='Robert Engsterhold',
      author_email='engsterhold@me.com',
      url='',
      license='No idea FIXME',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      #tests_require=["nose"],
      requires=[
          'aiohttp'
          'rdflib'
      ],
    # not correct
    #  entry_points="""
    #      [rdf.plugins.store]
    #      AioRDF4J = aiograph.AioRDF4jStore
    # """,
      )
