#Es un ejemplo de searching: necesita que le pases un argumento de busqueda

from elasticsearch import Elasticsearch
es = Elasticsearch()
import sys

# """
if len(sys.argv) == 2:
    search = str(sys.argv[1])
else:
    search = "a"
res = es.search(index=search, body={"query": {"match_all": {}}})
print("Got %d Hits for search %s:" % (res['hits']['total'],search))
if res['hits']['total'] > 0:
    for hit in res['hits']['hits']:
        print(hit)
        print(hit["_index"])
        print(hit["_source"])
        #print("%(control)s %(playstation)s: %(xbox)s" % hit["_source"])
    # """