

weh = """PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX voc: <http://issg.de/base/vocab#>
PREFIX luc: <http://www.ontotext.com/owlim/lucene#>
PREFIX mrs: <http://issg.de/mappings/historic#>
PREFIX sesame: <http://www.openrdf.org/schema/sesame#>
PREFIX phon: <http://issg.de/ontologies/phonetic#>
PREFIX maps: <http://issg.de/maps/vocab#>
PREFIX spif: <http://spinrdf.org/spif#>
PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>
construct { ?s a <http://issg.de/maps/vocab#Map> .
 ?obs rdfs:label ?obl; <http://issg.de/base/vocab#at> ?feat;
  <http://issg.de/maps/vocab#legendItem> ?oo .
   ?oo rdfs:label ?ol .
    ?feat rdfs:label ?fl .
    ?feat <urn:dummy#wkt> ?wkt }
     where { ?s a <http://issg.de/maps/vocab#Map>.
      ?obs <http://purl.org/linked-data/cube#dataSet> ?s .
       ?obs <http://issg.de/base/vocab#at> ?feat ;
        a <http://issg.de/maps/vocab#LinguisticObservation> .
        optional { ?feat rdfs:label ?fl } .
         #?feat geo:hasGeometry/geo:asWKT ?wkt .
          #?obs <http://issg.de/maps/vocab#legendItem> ?oo .
           optional {?obs rdfs:label ?obl } .
           # optional { ?oo rdfs:label ?ol } 
           }"""


basic_query = "Select distinct * where {?s rdfs:label ?o . ?s ?p ?r} limit 20"