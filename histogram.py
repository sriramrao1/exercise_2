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

        if int(total) == 3:
            minlimit = sys.argv[1]
            maxlimit = sys.argv[2]
            cur.execute("SELECT word, count from tweetwordcount WHERE count between (%s) and (%s)", (minlimit, maxlimit))
            records = cur.fetchall()
            print("Count of records = %d" % len(records))
            for rec in records:
                print("%s: %d" % (str(rec[0]), (rec[1])) )
              
            conn.commit()

        else:
            print("Incorrect number of arguments")
            
    except Exception as inst:
        print(inst.args)
        print(inst)
   

if __name__ == '__main__':
  main() 
