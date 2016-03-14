from pymongo import MongoClient
import unicodedata
import re

client = MongoClient()
db = client.news
cursor = db.newswebdump.find({"crawled":True,"meta":""})
i=1
for document in cursor:
    data = document['data']
    link = document['link']
    if len(data) < 45:
        print(" CAME ",i, " data ",data)
        i += 1
        db.newswebdump.update({"link": link}, {"$set": {"meta":"ignore"}})