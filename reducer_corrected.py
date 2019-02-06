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
#print(res.content)


def process_term(term):
    # print(term)
    # print(running_totals)
    # store in ES
    index = 'twitter'
    dict_keys = list(running_totals.keys())
    if len(running_totals) == 3:
        brand_1 = dict_keys[0]
        brand_2 = dict_keys[1]
        brand_3 = dict_keys[2]

        counter_1 = running_totals[brand_1]
        counter_2 = running_totals[brand_2]
        counter_3 = running_totals[brand_3]

        total = counter_1 + counter_2 + counter_3

        es.index(index=index, doc_type='tweets', id=last_key,
                 body={'key': last_key, str(brand_1): counter_1, str(brand_2): counter_2,
                       str(brand_3): counter_3, 'total': total})
        
    elif len(running_totals) == 2:
        brand_1 = dict_keys[0]
        print(str(brand_1))

        brand_2 = dict_keys[1]
        print(str(brand_2))

        counter_1 = running_totals[brand_1]
        counter_2 = running_totals[brand_2] 

        total = counter_1 + counter_2

        # agrego en elasticsearch la keyword, el count para brand_1, brand_2 y total como suma de las dos
        es.index(index=index, doc_type='tweets', id=last_key,
                 body={'key': last_key, str(brand_1): counter_1, str(brand_2): counter_2, 'total': total})

    else:
        brand_1 = dict_keys[0]
        counter_1 = running_totals[brand_1]

        es.index(index=index, doc_type='tweets', id=last_key,
                 body={'key': last_key, str(brand_1): counter_1, 'total': counter_1})
   
   
def collect(brand, count):
    if brand in running_totals:
        running_totals[brand] += count
    else:
        running_totals[brand] = count


if __name__ == '__main__':
    inputfile = open(sys.argv[1], 'r', encoding='utf-8')
    counter = 0
    for input_line in inputfile:
        #if counter <= 10000:
        input_line = input_line.strip()

        #Chequeo tener los argumentos: [Palabra] [Brand] [Cantidad]
        if len(input_line.split("\t", 2)) == 3: # and counter < limit:
            this_key, brand, count = input_line.split("\t", 2)
            count = int(count)

            if last_key == this_key:
                collect(brand, count)
            else:
                if last_key:
                    process_term(last_key)#, idx)
                    #process_term(last_key, idx)
                    idx += 1

                running_totals = {}
                collect(brand, count)
                last_key = this_key
            counter += 1
    if last_key == this_key:
        process_term(last_key)#, idx)
        idx += 1

    res = es.search(index='twitter', body={"query": {"match_all": {}}})
    max_value = max([x['_source']['total'] for x in res['hits']['hits']])
    for i in range(max_value-5,max_value+1):
        output = [x['_source'] for x in res['hits']['hits'] if x['_source']['total']==i]
        print(output)
