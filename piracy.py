'''
Edited on Oct 15, 2014

@author: marksp
'''
#imports - had to change the urllib2 to below as I have a different python (I think)
import urllib.request
import psycopg2
import re
import csv

#set up postgres connection - changed to my db connetions
dbname = "****"
user = "****"
password = "****"
host = "****"
port = "****"
table = "piracy_all" # change the table name as required - its gets new appended later
connection = psycopg2.connect(dbname = dbname, user = user, password = password, host = host, port = port)
cursor = connection.cursor()

#drop table and create new table named above
cursor.execute("DROP TABLE IF EXISTS "+table +";")
create_query = "CREATE TABLE public."+table +" (id character varying, time_stamp timestamp without time zone, the_geom geometry, lat double precision, lon double precision)"
cursor.execute(create_query)
connection.commit()

pirate = "http://www.shipping.nato.int/Pages/LargeAlertMap.aspx?Paged=TRUE&PagedPrev=TRUE&p_Date=20120106%2014%3a15%3a00&p_ID=219&PageFirstRow=1&&View={82E9DF30-7643-487D-89CD-DDE6112F0B3F}"
            #added a print to check it found the url
print ("url=" + pirate)
response = urllib.request.urlopen(pirate)
content = response.read()
print (content)
             
            # use a regex to extract the lat/lons where they have added/changed values
regex = re.compile("\d+-\d+.\d+[N,S,E,W]")
lls = regex.findall(content.decode("utf-8"))
            # added a print line to check if it was doing anything here
            # print (lls)
            # for the latitudes
            
lat_list = []
lats = lls[0::][::2]
for latitude in lats:
    deg = int(latitude[:3])  # first 3 characters for decimal degrees
    decmin = float(latitude[4:9]) / 60  # dec.min to float and divide by 60 for decimal deg
    lat = deg + decmin
    nsew = latitude[-1]  # sign either NSEW
    if nsew == "S":
        lat = lat * -1
    lat_list.append(lat)  # add to new lat_list
             
            # for the longitudes
    lon_list = []
    lons = lls[1::][::2]
    for longitude in lons:
        deg = int(longitude[:3])  # first 3 characters for decimal degrees
        decmin = float(longitude[4:9]) / 60  # dec.min to float and divide by 60 for decimal deg
        lon = deg + decmin
        nsew = longitude[-1]  # sign either NSEW
        if nsew == "W":
            lon = lon * -1
        lon_list.append(lon)  # add to new lon_list
             
            # set the count for the rows to table
        count_id = 1
            # iterate through lat_list and lon_list and insert into table, then alter table
        for lat, lon in zip(lat_list, lon_list): 
                 
                # insert into table the id, lat, lon scraped from site   
                insert_ll_query = "INSERT INTO public." + table + " (id,lat,lon) VALUES ('" + str(count_id) + "','" + str(lat) + "','" + str(lon) + "')"
                print ("sql=" + insert_ll_query)
                cursor.execute(insert_ll_query)
                connection.commit()
                 
                # alter table by updating the_geom with lat lon
                alter_hash_query = "UPDATE public." + table + " SET the_geom = ST_SetSRID(ST_MakePoint(lon,lat),4326);"
                cursor.execute(alter_hash_query)
                connection.commit()
                 
                # now add a count and keep going
                count_id = count_id + 1
                print (count_id)
        
#create geohash from the_geom as precision 4, then group these and create a new_geom for the precision cell based    
hash_count_query = "select count (id) as count, ST_GeoHash(the_geom, 4) as geohash, ST_GeomFromGeoHash(ST_GeoHash(the_geom, 4)) as new_geom from public."+table+" group by geohash order by count desc;"
cursor.execute(hash_count_query)
result = cursor.fetchall()

#make new table for qgis
cursor.execute("DROP TABLE IF EXISTS "+table +"_new;")
new_table_query = "select count (id) as count, ST_GeoHash(the_geom, 4) as geohash, ST_GeomFromGeoHash(ST_GeoHash(the_geom, 4)) as new_geom into public."+table+"_new from public."+table+" group by geohash order by count desc;"
cursor.execute(new_table_query)
connection.commit()

#its all done
print ("================Process Complete: Now Load to QGIS from PostgreSQL DB================")
