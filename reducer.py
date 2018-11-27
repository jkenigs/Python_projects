#!/usr/bin/env python
 
import sys
from elasticsearch import Elasticsearch

es = Elasticsearch()

last_key = None
running_totals = {} 

def process_term(term):
   print(term)
   print(running_totals)
   # store in ES 
   pass

def collect(brand, count):
   if brand in running_totals:
      running_totals[brand] += count
   else:
      running_totals[brand] = count

if __name__ == '__main__':
   inputfile = open(sys.argv[1], 'r')
   for input_line in inputfile:
      input_line = input_line.strip()
      this_key, brand, count = input_line.split("\t", 2)
      count = int(count)

      if last_key == this_key:
         collect(brand, count)
      else:
         if last_key:
            process_term(last_key)
         running_totals = {}
         collect(brand, count) 
         last_key = this_key
   if last_key == this_key:
      process_term(last_key)
