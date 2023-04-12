import scrapy, os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import scrapy
import json
from scrapy.http import Request
import time

load_dotenv()
HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB_DATABASE")
SCHEMA = os.getenv("DB_SCHEMA")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
PORT = os.getenv("DB_PORT")
TABLE = os.environ.get("DB_TABLE")

class ShazamSpider(scrapy.Spider):
    name = 'shazamspider'
    song_list = []

    formatted_date = datetime.now().strftime("%Y.%m.%d %H:%M:%S")

    def __init__(self, region_input, *args, **kwargs):
        super(ShazamSpider, self).__init__(*args, **kwargs)
        self.region_input = region_input

    def start_requests(self):
        
        urls = [
            'https://www.shazam.com/services/amapi/v1/catalog/TR/playlists/pl.1764c36585b6411aaf4aacf688ed8464/tracks?limit=200&l=en-US&relate[songs]=artists,music-videos',
            'https://www.shazam.com/services/amapi/v1/catalog/TR/playlists/pl.92d704ba99a3411289a34fab82866a62/tracks?limit=200&l=en-US&relate[songs]=artists,music-videos'
        ]

        for i in range(len(urls)):
            yield scrapy.Request(urls[i], callback = self.parse)
    
    def parse(self, response, **kwargs):
        time.sleep(0.18)
        region_index = {"pl.1764c36585b6411aaf4aacf688ed8464":"turkey", "pl.92d704ba99a3411289a34fab82866a62":"global"}
        region = region_index[response.url.split("/")[9]]
        jsonresponse = json.loads(response.text)
        all_songs = jsonresponse["data"]
        song_rank = 1
        for song in all_songs:
            #song_attributes = song["attributes"]
            '''
            try:
                song_id = song["id"]
            except:
                song_id = None
            '''
            try:
                id_type = song["type"]
            except:
                id_type = None
            
            try:
                song_href = "https://www.shazam.com/services/amapi" + song["href"]
            except:
                song_href = None

            try:
                song_albumName = song["attributes"]["albumName"]
            except:
                song_albumName = None

            try:
                song_genreNames = song["attributes"]["genreNames"]
            except:
                song_genreNames = None
            
            try:
                url = song["attributes"]["artwork"]["url"]
                image_width = str(song["attributes"]["artwork"]["width"])
                image_height = str(song["attributes"]["artwork"]["height"])
                song_image_url = url.replace("{w}", image_width).replace("{h}", image_height)
            except:
                song_image_url = None

            try:
                song_durationInMillis = song["attributes"]["durationInMillis"]
            except:
                song_durationInMillis = None

            try:
                song_releaseDate = song["attributes"]["releaseDate"]
            except:
                song_releaseDate = None

            try:
                song_name = song["attributes"]["name"]
            except:
                song_name = None

            try:
                song_artistName = song["attributes"]["artistName"]
            except:
                song_artistName = None

            #print("song_id: ",song_id)
            print("song_type", id_type)
            print("song_href", song_href)
            #print("song_attributes", song_attributes)
            print("song_albumName", song_albumName)
            print("song_genreNames", song_genreNames)
            #print("song_trackNumber", song_trackNumber)
            print("song_releaseDate", song_releaseDate)
            print("song_name", song_name)
            print("song_artistName", song_artistName)
            print("formatted_date ", self.formatted_date)
            print("---------------------------------------------")
            
            self.song_list.append([song_rank, id_type, song_href, song_albumName, song_image_url, song_genreNames, song_name, song_artistName, song_durationInMillis, song_releaseDate, region, self.formatted_date])
            song_rank += 1
          
    def db_insert(self):
        df = pd.DataFrame(self.song_list, columns = ["song_rank", "id_type", "href", "album_name", "album_image_url", "genre_name", "song_name", "artist_name", "duration_InMillis", "release_date", "region", "scrape_date"])
        #df.to_excel('shazamtop200.xlsx', index=False)

        conn = None
        # Connect to the database
        print("before connection")
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
        print("after connection")
        print("connection is created")

        # Create a cursor object
        cur = conn.cursor()
        print("cursor is created")

        # Create schema if not exists
        cur.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')

        # Create table if not exists
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} 
                (
                id serial4 NOT NULL,
                song_rank text, 
                id_type text, 
                href text, 
                album_name text,  
                album_image_url text, 
                genre_name text,
                song_name text, 
                artist_name text,  
                duration_InMillis integer, 
                release_date date, 
                region text,
                scrape_date timestamp,
                CONSTRAINT {TABLE}_pkey PRIMARY KEY (id))''')

        # Add products to db
        for product in self.song_list:
            sql = f"INSERT INTO {SCHEMA}.{TABLE} (song_rank, id_type, href, album_name, album_image_url, genre_name, song_name, artist_name, duration_InMillis, release_date, region, scrape_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values = (product[0], product[1], product[2], product[3], product[4], product[5], product[6], product[7], product[8], product[9], product[10], product[11])
            cur.execute(sql, values)
        print("products are inserted to specified db on postgre sql")

        # Commit the changes to the database
        conn.commit()
        print("changes are commited")

        # Close the cursor and connection
        cur.close()
        conn.close()
        print("cursor and connection are closed")