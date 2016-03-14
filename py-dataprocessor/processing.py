import re
from pymongo import MongoClient
from collections import Counter
from bs4 import BeautifulSoup
import operator
import os
import requests
import re, string
import mechanize
import nltk
from html2text import html2text
from nltk.corpus import stopwords
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import WordCloud
from wordcloud import STOPWORDS
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt


# https://gist.github.com/artieziff/7224756


def clean_html(html):
    """
    Copied from NLTK package.
    Remove HTML markup from the given string.

    :param html: the HTML string to be cleaned
    :type html: str
    :rtype: str
    """

    # First we remove inline JavaScript/CSS:
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", "", html.strip())
    # Then we remove html comments. This has to be done before removing regular
    # tags since comments can contain '>' characters.
    cleaned = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", cleaned)
    # Next we can remove the remaining tags:
    cleaned = re.sub(r"(?s)<.*?>", " ", cleaned)
    # Finally, we deal with whitespace
    cleaned = re.sub(r"&nbsp;", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    return cleaned.strip()

# builds the data model which maintains the data structure
# as it will be reused again and again
#
#
#Make it customizable for category & other field take
def buildModel(date):
    output_data_type = {}
    client = MongoClient()
    db = client.news  # use db
    urls = db.libraries.find({"date": date})

    allText = ""
    entireTextList = []
    for document in urls:
        data = document['data']
        entireTextList.append(data)
        allText += data
    output_data_type["data_as_list"] = entireTextList
    output_data_type["data_as_value"] = allText

    client.close()
    return output_data_type

def process(time, crawl_website=True):
    extract_date = time.split()
    date = extract_date[0]
    all_values = buildModel(date)
    if crawl_website:
        updateCrawledData(date, "main")
    else:
        buildN_Grams(all_values, 3)
        buildWordCloud(all_values)


def readHTML(url):
    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.addheaders = [('User-agent', 'Firefox')]
    html = br.open(url).read().decode('utf-8')
    cleanhtml = clean_html(html)
    text = html2text(cleanhtml)
    return text
    # print("THIS IS ONE ",text)


def updateCrawledData(date, category="don't know"):
    client = MongoClient()
    db = client.news  # use db
    print("DBDB ", db," category ",category," date ",date)
    #urls = db.libraries.find({"category": category, "date": date})
    urls = db.libraries.find({"$and": [{'category': category}, {'date': date}]})
    print(urls)
    for document in urls:
        if not document['crawled']:
            try:
                url_request = requests.get(document['link'])
            except:
                print("Unable to get URL. Please make sure it's valid and try again.")
                continue
            if url_request:
                cleanData = readHTML(document['link'])
                print("url_requesturl_request ", document['link'])
                db.libraries.update({"link": document['link']}, {"$set": {"crawled": True, "data": cleanData}})
                # db.libraries.update({"username":"janedoe"}, new_user_doc, safe=True)

    client.close()





def buildN_Grams(all_values, n=2):
    entireText = all_values["data_as_list"]

    cv = CountVectorizer(ngram_range=(n, n), max_features=500, stop_words='english')
    cv.fit(entireText)

    X = cv.transform(entireText)
    counts = X.sum(axis=0)

    df = pd.DataFrame({'Statements': cv.get_feature_names(), 'Count': counts.tolist()[0]})
    # print(df)

    # df.sort_values(by='Count', ascending=False, inplace=True)
    df.sort(['Count'], ascending=[False], inplace=True)
    print(df.head(50))


# https://www.kaggle.com/groe0029/d/kaggle/hillary-clinton-emails/clinton-email-graph-with-pageranks/files
# https://www.kaggle.com/redahe/d/kaggle/hillary-clinton-emails/brief-political-landscape-in-verse

def buildWordCloud(all_values):
    all_text = all_values["data_as_value"]
    current_path = os.path.realpath(__file__)
    current_path = current_path[0:current_path.rfind('/') + 1]
    img = Image.open(current_path + "pres.png")
    img = img.resize((980, 1080), Image.ANTIALIAS)
    hcmask = np.array(img)
    wc = WordCloud(background_color="white", max_words=2000, mask=hcmask, stopwords=STOPWORDS)
    wc.generate(all_text)
    wc.to_file(current_path + "today.png")
    '''
    plt.imshow(wc)
    plt.axis("off")
    plt.figure()
    plt.imshow(hcmask, cmap=plt.cm.gray)
    plt.axis("off")
    plt.show()
    '''


def fetchUrl(time):
    extract_date = time.split()
    date = extract_date[0]
    client = MongoClient()
    db = client.news  # use db
    urls = db.libraries.find({"date": date})

    for document in urls:
        print(document['link'])
        try:
            url_request = requests.get(document['link'])
        except:
            print(
                "Unable to get URL. Please make sure it's valid and try again.")
        if url_request:
            readHTML(document['link'])

            # text processing
            raw = BeautifulSoup(url_request.text).get_text()

            # nltk.data.path.append('./nltk_data/')  # set the path
            tokens = nltk.word_tokenize(raw)
            text = nltk.Text(tokens)
            # remove punctuation, count raw words
            nonPunct = re.compile('.*[A-Za-z].*')
            raw_words = [w for w in text if nonPunct.match(w)]

            raw_words = readHTML(document['link'])
            stop = stopwords.words('english')

            cleaned_and_no_stop_words = [w for w in raw_words if w.lower() not in stop and (len(w) > 2)]
            # clean further
            # exclude = set(string.punctuation)
            # cleaned_and_no_stop_words = ' '.join(ch for ch in cleaned_and_no_stop_words if ch not in exclude)
            print("THIS IS TWO ", cleaned_and_no_stop_words)
            break

    client.close()
