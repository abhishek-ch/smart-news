import ConfigParser
import tweepy
from pymongo import MongoClient
import sys

config = ConfigParser.ConfigParser()
config.read('twitter_keys.ini')
config_itr = iter(config.sections())
# print(config.get('Section1','access_token'))
# print(config.get('Section1','access_token_secret'))





'''
read tweets as json
http://stackoverflow.com/questions/24002536/get-tweepy-search-results-as-json
'''

# If the authentication was successful, you should
# see the name of the account print out
#print(api.me().name)
#india WOEID 23424848
#http://www.whatthetrend.com/?woeid=23424848
'''
to find the list of all countried trends supported by twitter
https://dev.twitter.com/docs/api/1/get/trends/available
'''




from datetime import datetime

def extract_tweets(api,trend, count = 400,WOEID=1,source_id=None):
    client = MongoClient()
    db = client.news
    "extract the tweets from hashtag and update the database"
    i = 1
    try:
        for tweet in tweepy.Cursor(api.search, q=(trend),lang='en').items(count):

            tweet_data = {"id":tweet.id,"trend":trend,"name":tweet.author.name.encode('utf8'),"screenname":tweet.author.screen_name.encode('utf8'),
                           "tweetcreated":tweet.created_at,"tweet":tweet.text.encode('utf8'),"retweeted":tweet.retweeted,
                           "favourited":tweet.favorited,"location":tweet.user.location.encode('utf8'),"timezone":tweet.user.time_zone,
                           "geo":tweet.geo,"sourceurl":tweet.source_url,"favorite_count":tweet.favorite_count,"in_reply_to_user_id":tweet.in_reply_to_user_id,
                           "retweet_count":tweet.retweet_count,"in_reply_to_screen_name":tweet.in_reply_to_screen_name,
                           "followers_count":tweet.user.followers_count,"description":tweet.user.description.encode('utf8'),"friends_count":tweet.user.friends_count,
                           "created_at":tweet.created_at,"date":datetime.now().strftime('%Y-%m-%d'),"time":datetime.now().strftime('%H:%M:%S.%f'),"WOEID":WOEID,"SOURCE_ID":source_id}
            db.tweets.insert(tweet_data)
            print("Successfully inserted ID : %s and serial : %s hashtag %s " % (tweet.id,i,trend))
            i += 1
    except Exception,e:
        print("error ",e)
        pass


    client.close()


def get_twitter_auth_token():

    section = config_itr.next()
    print("fetching API withs section: ",section)
    auth = tweepy.OAuthHandler(config.get(section,'consumer_key'), config.get(section,'consumer_secret'))
    auth.secure = True
    auth.set_access_token(config.get(section,'access_token'), config.get(section,'access_token_secret'))

    api = tweepy.API(auth)
    print("Api authentication Successfull with section ",section)
    #just update status
    # result = api.update_status("Well its gonna be fun !!! :-) http://github.com/abhishek-ch")
    # print("update status ",result)
    return (api,section)


from itertools import cycle

def tweet_crawl_bot():
    "crawl through each section and extract tweets whatever is required"
    api,section = get_twitter_auth_token()
    # for section in config.sections():
    print(section)

    index = 0
    #https://blog.twitter.com/2010/woeids-in-twitters-trends
    for woeid in [23424848,1,23424977,23424803,24554868]:
        try:
            trends1 = api.trends_place(woeid)
            trends = set([trend['name'] for trend in trends1[0]['trends']])
            print("All trends available are ",trends)

            for hashtag in trends:
                print "Trend is ",trends
                if index > 16:
                    api,section = get_twitter_auth_token()
                    index = 0

                if index <= 16:
                    extract_tweets(api,hashtag,150,woeid,section)
                    index += 1
        except (tweepy.TweepError) as e:
            api,section = get_twitter_auth_token()
            print("Tweepy 429 error, restored with new APi")
        except Exception as e:
            print(e.message)





tweet_crawl_bot()





# trends1 = api.trends_place(1)
# trends = set([trend['name'] for trend in trends1[0]['trends']])
#
#
# for hashtag in trends:
#     print "Trend is ",trends
#     extract_tweets(hashtag)

# for tweet in tweepy.Cursor(api.search, q=('"#FrndQ"'),lang='en').items(1):
#     # print(tweet)
#     # print(type(tweet))
#     # for k,v in tweet.__dict__.items():  #same thing as `vars(status)`
#     #     print k,type(v),v
#
#     # for attr in dir(tweet):
#     # print attr, getattr(tweet,attr)
#
#     print "Name:", tweet.author.name.encode('utf8')
#     print "Screen-name:", tweet.author.screen_name.encode('utf8')
#     print "Tweet created:", tweet.created_at
#     print "Tweet:", tweet.text.encode('utf8')
#     print "Retweeted:", tweet.retweeted
#     print "Favourited:", tweet.favorited
#     print "Location:", tweet.user.location.encode('utf8')
#     print "Time-zone:", tweet.user.time_zone
#     print "Geo:", tweet.geo
#     print "source_url: ",tweet.source_url
#     print "favorite_count: ",tweet.favorite_count
#     print "in_reply_to_user_id: ",tweet.in_reply_to_user_id
#     print "retweet_count: ",tweet.retweet_count
#
#     print "in_reply_to_screen_name: ",tweet.in_reply_to_screen_name
#     print "place: ",tweet.place
#     print "id: ",tweet.id
#
#
#     print "followers_count: ",tweet.user.followers_count
#     print "description: ",tweet.user.description
#     print "friends_count: ",tweet.user.friends_count
#
#
#
#     print "created_at: ",tweet.created_at
#     print "created_at: ",tweet.created_at
#     print "user details: ",tweet.user
#     print "//////////////////////// /n/n/n"



