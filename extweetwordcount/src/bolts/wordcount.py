from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2
import os


class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        try:
            conn = psycopg2.connect(database="tcount", user="w205", password="postgres", host="localhost", port="5432")
            #conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
            cur = conn.cursor()
            cur.execute('''DROP TABLE IF EXISTS tweetwordcount''')
            cur.execute('''CREATE TABLE tweetwordcount (word TEXT PRIMARY KEY NOT NULL,count INT NOT NULL);''')
            conn.commit()
            conn.close()
        except Exception as inst:
            #print(type(inst))
            print(inst.args)
            print(inst)
            #self.conn.commit()
            #self.conn.close()
        

    def process(self, tup):
        word = tup.values[0]

        # Write code to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        try:
                  
        # Increment the local count
            self.counts[word] += 1
            self.emit([word, self.counts[word]])

            conn = psycopg2.connect(database="tcount", user="w205", password="postgres", host="localhost", port="5432")
            #conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
            cur = conn.cursor()
            if self.counts[word]==1:
                query = "INSERT INTO tweetwordcount (word,count) VALUES ('" + word + "',1);"
                self.log("Query: " + query)
                cur.execute(query)
		conn.commit()
                #self.cur.execute("INSERT INTO tweetwordcount (word,count) VALUES ('%s', %d)", (word, self.counts[word]))
            else:
                uCount = self.counts[word]
		query = "UPDATE tweetwordcount SET count=" + str(uCount) + " WHERE word='" + word + "';"
		cur.execute(query)
		conn.commit()
		#self.cur.execute("UPDATE tweetwordcount SET count=%d WHERE word=%s", (self.counts[word], word))

                                
            #self.conn.close()

            # Log the count - just to see the topology running
            self.log('%s: %d' % (word, self.counts[word]))
            
        except:
            print("Errored out")
            #self.conn.commit()
            #self.conn.close()
