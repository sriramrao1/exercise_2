#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals
import sys
import psycopg2

def main():

    try:
        conn = psycopg2.connect(database="tcount", user="w205", password="postgres", host="localhost", port="5432")
        #conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
        cur = conn.cursor()

        total = len(sys.argv)
        #print ("The total numbers of args passed to the script: %d " % total)

        # Get the arguments list 
        #cmdargs = str(sys.argv)
        #print ("Args list: %s " % cmdargs)

        #Running finalresults.py without an argument returns all the words in the stream and their total count of occurrences,
        #sorted alphabetically in an ascending order, one word per line.
        if int(total) == 1:
            cur.execute("SELECT word, count from tweetwordcount ORDER BY word ASC;")
            records = cur.fetchall()
            print("Count of records in tcount = %d" % len(records))
            for rec in records:
                print("word = %s" % str(rec[0]))
                print("count = %d" % (rec[1]), "\n")
            conn.commit()

        #This section gets a word as an argument and returns the total number of word occurrences in the stream.
        else:
            searchword = str(sys.argv[1])
            query = "SELECT word, count from tweetwordcount WHERE word='" + searchword + "';"
            cur.execute(query)
            records = cur.fetchall()
            print("Count of records in tcount = %d" % len(records))
            for rec in records:
                print("word = %s" % str(rec[0]))
                print("count = %d" % (rec[1]), "\n")
            conn.commit()

    except Exception as inst:
        print(inst.args)
        print(inst)
   

if __name__ == '__main__':
  main() 
