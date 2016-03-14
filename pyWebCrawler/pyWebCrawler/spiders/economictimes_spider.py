from scrapy import Spider
from scrapy.selector import HtmlXPathSelector
from scrapy.spiders import Spider
from scrapy.http import Request
from pyWebCrawler.items import PywebcrawlerItem
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import mechanize
import nltk
from html2text import html2text
from nltk.corpus import stopwords
import re
from datetime import datetime
import zlib
import time
from random import randint
import urllib
from bs4 import BeautifulSoup

# https://www.kaggle.com/amirhamini/d/benhamner/nips-2015-papers/find-similar-papers-knn/notebook


class EconomicsTimesSpider(Spider):

    name = "dump"
    allowed_domains = ["indiatimes.com","ndtv.com","firstpost.com","india.com","zeenews.india.com"
                       ,"indianexpress.com","hindustantimes.com","ibnlive.com","intoday.in","dnaindia.com",
                       "abplive.in","google.ie","google.com","news.google.ie","indianexpress.com","bbc.com",
                       "rediff.com","reuters.com","yahoo.com"]
    start_urls = [
        "http://www.bbc.com/news/world/asia/india",
        "http://www.rediff.com/news/headlines",
        "http://in.reuters.com/news/top-news",
        "https://in.news.yahoo.com/"
        "http://timesofindia.indiatimes.com/",
        "http://economictimes.indiatimes.com",
        "http://www.ndtv.com/",
        "http://www.firstpost.com/",
        "http://zeenews.india.com/",
        "http://www.thehindu.com/",
        "http://www.hindustantimes.com/",
        "http://www.ibnlive.com/",
        "http://indiatoday.intoday.in/",
        "http://www.dnaindia.com/",
        "http://www.abplive.in/",
        "http://indianexpress.com/",

    ]


    def clean_html(self,html):
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



    def extract_Text_From_HTML(self,url):
        try:
            html =  urllib.urlopen(url).read()
            soup = BeautifulSoup(html, 'html.parser')

            # cleanhtml = clean_html(html)
            text = self.clean_html(soup.getText())
            return (text,True)
        except :
            print("problem with loading ",url)

        return ("",False)

    def readHTML(self,url):
        text = ""
        crawled = False
        try:
            br = mechanize.Browser()
            br.set_handle_robots(False)
            br.addheaders = [('User-agent', 'Firefox')]
            html = br.open(url).read().decode('utf-8','ignore')
            cleanhtml = self.clean_html(html)
            text = html2text(cleanhtml)
            crawled = True
        except :
            print("problem with loading ",url)

        return (text, crawled)

    # rules = (Rule(SgmlLinkExtractor(), callback='parse', follow=False), )
    rules = (Rule(LxmlLinkExtractor(allow=()), callback='parse', follow=True),)






    def parse(self, response):
        # filename = response.url.split("/")[-2] + '.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        #
        #
        # hxs = HtmlXPathSelector(response)
        # for url in hxs.select('//a/@href').extract():
        #     if not ( url.startswith('http://') or url.startswith('https://') ):
        #         url= URL + url
        #     print url
        #     yield Request(url, callback=self.parse)


        # extractor = LinkExtractor(allow_domains=self.allowed_domains)
        # links = extractor.extract_links(response)
        # for link in links:
        #     print link.url

        for link in LxmlLinkExtractor(allow=self.allowed_domains).extract_links(response):
            item = PywebcrawlerItem()
            url = link.url

            url_split = url.split("/")
            item['source'] = url_split[2]

            category = ''
            title = ''
            i = 0

            for word in url_split:
                if i > 2:
                    if len(word) < 20:
                        category += word
                        category += " "
                    elif len(word) > 20:
                        title = word
                        break
                i += 1

            # rd = randint(9,45)

            # time.sleep(rd)

            content,crawled = self.extract_Text_From_HTML(link)
            print("----------------------------- > >> >>>>>>>>>>>>>>>>>>>>>>>>>>>  ",url," index ",i," crawled ",crawled)
            item['link'] = url
            item['title'] = title
            item['category'] = category
            item['data'] = content
            item['crawled'] = crawled
            item['date'] = datetime.now().strftime('%m/%d/%Y')
            item['time'] = datetime.now().strftime('%H:%M:%S.%f')
            item['meta'] = ""
            yield item