#!/usr/bin/env python

import sys
from elasticsearch import Elasticsearch

es = Elasticsearch()

last_key = None
running_totals = {}
idx = 1
counter = 0
limit = float("inf")


# Check de que elastisearch funciona
import requests
res = requests.get('http://localhost:9200')
print(res.content)


def process_term(term, id):
    # print(term)
    # print(running_totals)
    # store in ES
    if term != "": #Remover caracteres que conflictuan con el armado del indice
        print(term)
        """
        Esta es la estructura que planteamos armar para el ES
        # index = palabra,
        # body = dict con conteo de la palabra por brand
        # id = contador incremental
        """
        res = es.index(index=term, doc_type='tweet', id=id, body=running_totals)
        print(res['result'])

def collect(brand, count):
    if brand in running_totals:
        running_totals[brand] += count
    else:
        running_totals[brand] = count


if __name__ == '__main__':
    inputfile = open(sys.argv[1], 'r', encoding='utf-8')
    for input_line in inputfile:
        input_line = input_line.strip()

        #Chequeo tener los argumentos: [Palabra] [Brand] [Cantidad]
        if len(input_line.split("\t", 2)) == 3: # and counter < limit:
            this_key, brand, count = input_line.split("\t", 2)
            count = int(count)

            if last_key == this_key:
                collect(brand, count)
            else:
                if last_key:
                    process_term(last_key, idx)
                    idx += 1

                running_totals = {}
                collect(brand, count)
                last_key = this_key
            counter += 1

    if last_key == this_key:
        process_term(last_key, idx)
        idx += 1

    #"""
    #Ejemplo de como buscar una palabra en Elastic search
    search_word = 'new'
    res = es.search(index=search_word, body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print("%(control)s %(playstation)s: %(xbox)s" % hit["_source"])
    #"""