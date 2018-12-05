#!/usr/bin/env python
import sys
import os

if __name__ == '__main__':
    lines = []
    for filename in os.listdir(sys.argv[1]):
       inputfile = open(os.path.join(sys.argv[1], filename), "r",  encoding = 'utf8')
       lines.extend(inputfile.readlines())
       inputfile.close()
    lines = sorted(lines)
    # utf-8 resuelve problema de encoding
    outputfile = open(sys.argv[2], "w",   encoding = 'utf8')
    outputfile.writelines(lines)
    outputfile.close()