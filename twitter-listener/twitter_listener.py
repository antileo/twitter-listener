
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
# import MySQLdb
import mysql.connector
from mysql.connector import Error
import time
import os
import json

	
connection = mysql.connector.connect(host='localhost',
                             database='test',
                             user='root',
                             password='root')


    
db_Info = connection.get_server_info()
print("Connected to MySQL database... MySQL Server version on ",db_Info)
c = connection.cursor()

print("||||||||||||||||||||||||||||||||||||||||||")

print("Escuchando...")

print("||||||||||||||||||||||||||||||||||||||||||")


#consumer key, consumer secret, access token, access secret.
ckey="nuV2q5BTphOnVfYlFpuJCFi3l"
csecret="rqwkOUz9keBcnPEkaA6qJQ0YwybhJVQV3yGvoiQjTdAtg46VhJ"
atoken="1116077439585857536-qsOrsxk1lVTmYsOYzUzkTBHNx6Cvkz"
asecret="emvFKuI6qOReVyEKv1eQ4pJo8gSWlhF9PfARVw8CbhRgl"

class listener(StreamListener):
    def __init__(self, path=None):
     self.path = path

    def on_data(self, data):

        tweet = data.split(',"text":"')[1].split('","source":"')[0]
        print(time.strftime("%Y%m%d_%H%M%S"), tweet)

        all_data = json.loads(data)
        
        created_at = all_data["created_at"]

        tweet = all_data["text"]
        
        username = all_data["user"]["screen_name"]

        source = all_data["source"]

        coordinates = all_data["coordinates"]

        truncated= all_data["truncated"]

        if coordinates is not None:
               print (coordinates["coordinates"])
               lon = coordinates["coordinates"][0]
               lat = coordinates["coordinates"][1]     
        data_string = json.dumps(data)
        
        c.execute("INSERT INTO taula (time, username, tweet,created_at,coordinates,source,truncated,data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (time.time(), username, tweet,created_at,coordinates,source,truncated,data_string))

        connection.commit()

        #print((username,tweet))

        str = json.dumps(data)

        # Open, Write then Close the file
        savefile = open(self.path, 'a', encoding='utf-8')
        #tweet = data.split(',"text":"')[1].split('","source":"')[0]
		# Decode the JSON from Twitter
        data_string = json.loads(str)
        savefile.write(data_string)
        savefile.close()
        #print ('ENCODED:', data_string)
        
        return True

    def on_error(self, status):
        print (status)

filename = "tweets_collected"
 
script_dir = os.path.dirname(__file__) 
rel_path = filename + ".json"
file_path = os.path.join(script_dir, rel_path)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener(path = file_path,))
twitterStream.filter(track=['gtd manquehue,grupo gtd,grupo_gtd,gtd_manquehue'], stall_warnings=True)