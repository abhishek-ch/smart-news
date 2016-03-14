from pymongo import MongoClient
import eatiht
import re,sys


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
    cleaned = re.sub('[\W_]+', ' ', cleaned)
    return cleaned.strip()


def readHTML(url,db):
    try:
        # html =  urllib.urlopen(url).read()
        # soup = BeautifulSoup(html, 'html.parser')

        # cleanhtml = clean_html(html)
        # text = clean_html(soup.getText())
        text = clean_html(eatiht.extract(url))
        return (text,True)
    except :
        print("problem with loading ",url)
        db.newswebdump.update({"link": url}, {"$set": {"crawled": False, "meta": "ignore"}})

    return ("",False)


'''
cursor = db.webdump.find({"crawled":True})

for document in cursor:

    link = document['link']
    data = document['data']
    data = unicodedata.normalize('NFKD', data).encode('ascii','ignore')
    data = clean_html(data)
    compressed_data = zlib.compress(data)
    currVal = db.newswebdump.find({"link":link},{"crawled":1})
    for document in currVal:

        if document['crawled']:
            db.newswebdump.update({"link": link}, {"$set": {"crawled": True, "data": compressed_data}})
            print(link," = ",data)



'''

def crawl_link_with_values(i=1, dates = ['']):
    client = MongoClient()
    db = client.news
    for day in dates:
        cursor = db.newswebdump.find({"date":day,"crawled":False,"meta":""},{"timeout":False})
        try:
            for document in cursor:
                link = document['link']
                # rd = randint(1,45)
                # print("waiting...",rd)
                # time.sleep(rd)

                content,crawled = readHTML(link,db)

                if crawled and len(content) > 65 and not content.startswith('Copyright'):
                   db.newswebdump.update({"link": link}, {"$set": {"crawled": True, "data": content}})
                   print(i," ",link," ",content)

                else:
                    db.newswebdump.update({"link": link}, {"$set": {"crawled": True, "data": content, "meta":"ignore"}})
                    print("COPYRIGHT ISSUE ")

                i += 1

        except TypeError,e:
            print("Type Error ",e)
            crawl_link_with_values(i)
        except Exception,e:
            print("Main Exception ",e," link ",link)
            crawl_link_with_values(i)

        client.close()

# crawl_link_with_values(1)


def ok(argv):
    print argv

if __name__ == "__main__":
    print("Started...")
    crawl_link_with_values(1,sys.argv[1:])
    # ok(str(sys.argv[1:]))


  #http://stackoverflow.com/questions/24199729/pymongo-errors-cursornotfound-cursor-id-not-valid-at-server
#   cursor not found error aises because of MongoDB server timeout
# cursor = db.newswebdump.find({"crawled":False,"meta":""},{"timeout":False})
# try:
#     for document in cursor:
#         link = document['link']
#         # rd = randint(1,45)
#         # print("waiting...",rd)
#         # time.sleep(rd)
#
#         content,crawled = readHTML(link)
#
#         if crawled:
#            db.newswebdump.update({"link": link}, {"$set": {"crawled": True, "data": content}})
#            print(i," ",link," ",content)
#            i += 1
#
# except TypeError:
#     print("Type Error")
# except Exception,e:
#     print("Main Exception ",e," link ",link)

# for document in cursor:
#     link = document['link']
#     rd = randint(1,45)
#     print("waiting...",rd)
    # time.sleep(rd)

    #
    # try:
    #     content,crawled = readHTML(link)
    #     # content = unicodedata.normalize('NFKD', content).encode('ascii')
    # except TypeError:
    #     print("Cant Convert")
    # except:
    #     print("Some Error")
    #
    # print("CRAWKED ",crawled)
    # if crawled:
    #     try:
    #         db.newswebdump.update({"link": link}, {"$set": {"crawled": True, "data": content}})
    #         print(i," : ",rd," ",link," ",content)
    #         i += 1
    #     except Exception,e:
    #         print(e)

