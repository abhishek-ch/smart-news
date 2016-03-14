# smart-news
This is an experiment to develop an app which could display news based on user preference.
The main Data repo is based on MongoDB


### Working 
User preference will be decide automatically and that will be obsereved based on User social network clicks and browing
history
There will be a manual way where user can select specific topics
This piece of code is still not exposed.

Currently migrating or researching for better scalabale DB like Cassandra or HBase.

Developing a better data model for building k nearest neighbours and for streaming considering `Apache Flink`


There is a Twitter crawler which will read tweets from multiple account details from all mentioned Place
```
cd pyWebCrawler/pyWebCrawler/tweets
python TweetsHandler.py
```

Crawling Text from various News links 
```
python scripts/MongoSnippet.py "02/20/2016”
```

News Crawler
```
cd pyWebCrawler 
scrapy crawl dump
```


Run Kafka Engine for process data
```
java -jar news-1.0-SNAPSHOT-jar-with-dependencies.jar
```