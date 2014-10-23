'''
Created on 23 Oct 2014

@author: marksp
'''

import feedparser
import psycopg2


#set up postgres connection - changed to my db connetions
dbname = "****"
user = "****"
password = "****"
host = "****"
port = "****"
table = "pirate" # change the table name as required - its gets new appended later
connection = psycopg2.connect(dbname = dbname, user = user, password = password, host = host, port = port)
cursor = connection.cursor()

#drop table and create new table named above
cursor.execute("DROP TABLE IF EXISTS "+table +";")
create_query = "CREATE TABLE public."+table +" (id character varying, title text, description text)"
cursor.execute(create_query)
connection.commit()

pirate = "http://www.shipping.nato.int/_layouts/listfeed.aspx?List=77c1e451-15fc-49db-a84e-1e8536ccc972&View=721a920c-538a-404e-838b-30635159e886#1"

d = feedparser.parse(pirate)
print (d['feed']['link'])
print (d.headers)

count_id = 0

for post in d.entries:
    print (post.title + ": " + post.description + "\n")
    count_id = count_id + 1
    title = post.title
    desc = post.description + "\n"
    # print (title)
    #insert into table the id, lat, lon scraped from site   
    insert_title_query = "INSERT INTO public."+table+" (id,title,description) VALUES ('" +str(count_id) +"','" +str(title) +"','"+str(desc) +"');"
    # print ("sql=" + insert_title_query)
    cursor.execute(insert_title_query)
    connection.commit()

print ("======Process Complete======")
