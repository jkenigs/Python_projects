#!/usr/bin/env python

import sys
from elasticsearch import Elasticsearch

es = Elasticsearch()

last_key = None
running_totals = {}
idx = 1
counter = 0
limit = float("inf")
chars_to_remove = [" ",",","*",".","..","<","|",">","/","?",'"',"+","-",":"]
chars_to_remove.extend(["0","1","2","3","4","5","6","7","8","9"])

def process_term(term, id):
    #print(term)
    term = ''.join([c for c in term if c not in set(chars_to_remove)])
    print(term)
    print(running_totals)
    # store in ES
    if term != "":
        res = es.index(index=term, doc_type='tweet', id=id, body=running_totals)
        print(res['result'])
    #return res


def collect(brand, count):
    if brand in running_totals:
        running_totals[brand] += count
    else:
        running_totals[brand] = count


if __name__ == '__main__':
    inputfile = open(sys.argv[1], 'r', encoding='utf-8')
    for input_line in inputfile:
        input_line = input_line.strip()
        if len(input_line.split("\t", 2)) == 3 and counter < limit:
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
    res = es.search(index="slthigh", body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print("%(control)s %(playstation)s: %(xbox)s" % hit["_source"])
    #"""