import datetime
import time
import requests
import pprint
import pymongo
import unicodedata
from bs4 import BeautifulSoup
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# connection for mongo
clientLocal = MongoClient('mongodb://localhost:27017')
clientCloud = MongoClient('mongodb://sailee:sailee123@ds221405.mlab.com:21405/db_newsfeed')

# accesss he db and collection
dbL = clientLocal.db_newsfeed
dbC = clientCloud.db_newsfeed

urls = ["https://www.hindustantimes.com/rss/topnews/rssfeed.xml",
        "https://www.hindustantimes.com/rss/world/rssfeed.xml",
        "https://www.hindustantimes.com/rss/analysis/rssfeed.xml",
        "https://www.hindustantimes.com/rss/india/rssfeed.xml",
        "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        "https://timesofindia.indiatimes.com/rssfeeds/1221656.cms",
        "http://feeds.reuters.com/reuters/INtopNews",
        "https://www.thehindu.com/news/feeder/default.rss",
        "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en",
        "https://www.reddit.com/r/worldnews/.rss",
        "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms"]


def sentiment_analyzer_scores(sentence):
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(sentence)
    return score


# split string ino date format
def date_format(pubDate):  # Tue ,05 Feb 2019 05:17:17 GMT
    dt = datetime.datetime.strptime(pubDate,' %d %b %Y %H:%M:%S %Z ')
    return dt

for url in range(len(urls)):
    # url = "https://www.hindustantimes.com/rss/topnews/rssfeed.xml"
    resp = requests.get(urls[url])
    soup = BeautifulSoup(resp.content, features="xml")
    # print(soup.prettify())
    items = soup.findAll('item')
    count_item = dbL.collection_rssNews.count()
    news_item = {}
    if (count_item == 0):
        for item in items:
            news_item['title'] = item.title.text
            news_item['description'] = item.description.text
            news_item['pubDate'] = item.pubDate.text
            temp = news_item['pubDate'].split(",")
            news_item['day'] = temp[0]
            dtf = date_format(temp[1])
            news_item['month'] = dtf.month
            news_item['date'] = dtf.day
            news_item['year'] = dtf.year
            news_item['hour'] = dtf.hour
            news_item['minute'] = dtf.minute
            news_item['second'] = dtf.second
                #			print(type(pubDate))
                # mongocollection
            dbL.collection_rssNews.insert(news_item)
            dbC.collection_rssNews.insert(news_item)
    else:
        for item in items:
            # calculate the score
            score = sentiment_analyzer_scores(item.description.text)
            temp = (item.pubDate.text).split(",")
            dtf = date_format(temp[1])
                # data in local mongodb mongocollection
            dbL.collection_rssNews.update_one({'title': item.title.text},
                                    {"$set": {'pubDate': item.pubDate.text, 'day': temp[0], 'date': dtf.day,
                                                 'month': dtf.month, 'year': dtf.year, 'hour': dtf.hour,
                                                 'minute': dtf.minute, 'second': dtf.second,
                                                 'description': item.description.text, 'pos': score['pos'],
                                                 'neg': score['neg'], 'neu': score['neu'], 'compound': score['compound']},},
                                       upsert=True)

                # data in cloud mongocollection
            dbC.collection_rssNews.update_one({'title': item.title.text},
                                       {"$set": {'pubDate': item.pubDate.text, 'day': temp[0], 'date': dtf.day,
                                                 'month': dtf.month, 'year': dtf.year, 'hour': dtf.hour,
                                                 'minute': dtf.minute, 'second': dtf.second,
                                                 'description': item.description.text, 'pos': score['pos'],
                                                 'neg': score['neg'], 'neu': score['neu'], 'compound': score['compound']},},
                                       upsert=True)