'''
Created on 23 Oct 2014

@author: marksp
'''

import feedparser
import psycopg2
from bs4 import BeautifulSoup
import re

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
create_query = "CREATE TABLE public."+table +" (id character varying, title text, description text, summary text, category text, lat double precision, lon double precision)"
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
    soup = BeautifulSoup(post.description)
    trlat = soup.find(text= re.compile('Latitude')).parent.parent
    lat = str(trlat)
    lat2 = lat.rstrip('</div>')
    lat3 = lat2[22:]
    
    trlon = soup.find(text= re.compile('Longitude')).parent.parent
    lon = str(trlon)
    lon2 = lon.rstrip('</div>')
    lon3 = lon2[23:]
    
    trsum = soup.find(text= re.compile('Summary')).parent.parent
    sum1 = str(trsum)
    sum2 = sum1.replace('</div>','')
    sum3 = sum2[21:]
    
    trcat = soup.find(text= re.compile('Category')).parent.parent
    cat1 = str(trcat)
    cat2 = cat1.replace('</div>','')
    cat3 = cat2[22:]
    
    #insert into table the id, lat, lon scraped from site   
    insert_title_query = "INSERT INTO public."+table+" (id,title,description,summary,category,lat,lon) VALUES ('" +str(count_id) +"','" +str(title) +"','"+str(desc) +"','"+str(sum3) +"','"+str(cat3) +"','"+str(lat3) +"','"+str(lon3) +"');"
    # print ("sql=" + insert_title_query)
    cursor.execute(insert_title_query)
    connection.commit()
    
#add geometry column
add_geo = "alter table "+table+" add column geo geometry(Point, 4326)"
cursor.execute(add_geo)
connection.commit()

#create geometry from lon, lat
set_geo = "UPDATE "+table+" SET geo = ST_SetSRID(ST_MakePoint(lon,lat),4326);"
cursor.execute(set_geo)
connection.commit()

print ("======Process Complete======")

