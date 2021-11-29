# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from requests.models import Response
from scrapy import http, signals
from scrapy.http import TextResponse
import tweepy
import json
import re
import datetime

from tweet_spider.spiders.Parrotlet import print_info

def format_tweet(tweet): 
    # a dict to contain information about single tweet
    tweet_information=dict()

    # text of tweet
    tweet_information['text']=tweet.text #.encode('utf-8')

    # date and time at which tweet was created
    tweet_information['created_at']=tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")

    # id of this tweet
    tweet_information['id_str']=tweet.id_str

    # retweet count
    tweet_information['retweet_count']=tweet.retweet_count

    # favourites count
    tweet_information['favorite_count']=tweet.favorite_count

    # screename of the user to which it was replied (is Nullable)
    tweet_information['in_reply_to_screen_name']=tweet.in_reply_to_screen_name

    #geo location
    tweet_information['geo']=tweet.geo

    tweet_information['source']=tweet.source

    tweet_information['language']=tweet.lang


    #######################################
    # user information in user dictionery #
    #######################################
    user_dictionery=tweet._json['user']

    # no of followers of the user
    tweet_information['followers_count']=user_dictionery['followers_count']

    # screename of the person who tweeted this
    tweet_information['id']=user_dictionery['id']

    # name of the person who tweeted this
    tweet_information['name_at_tweet_time']=user_dictionery['name']

    # screename of the person who tweeted this
    tweet_information['screen_name']=user_dictionery['screen_name']

    #user location    
    tweet_information['user_location']=user_dictionery['location']

    #user location    
    tweet_information['user_created_at']=user_dictionery['created_at']

    # return formated tweet
    return tweet_information

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
import scrapy


class TweetSpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class TweetSpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.
        if 'robots.txt' in request.url:
            return TextResponse(url="robots", encoding='utf-8', body="I don't care about it. It's and API")
        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        _search = re.search(r"twitter:\/\/([\w\W\s]*)", request.url)[1]                
        _cold_newest_id = int(request.meta['cold_newest_id'])
        _oldest_id = int(request.meta['oldest_id'])
        _newest_id = int(request.meta['newest_id'])
        #faz a busca usando a API autenticada no spider (evitar multiplas autenticação)
        api = spider.api
        tweets = api.search_tweets(q=f'{_search}', lang='pt', count=100, max_id=_oldest_id, since_id=_cold_newest_id)

        # Prepara o retorno
        real_response = dict()
        real_response['search_term_group'] = request.meta['search_term_group']
        real_response['search_term']       = _search
        real_response['newest_id']         = _newest_id
        real_response['newest_date']       = request.meta['newest_date']
        real_response['cold_newest_id']    = request.meta['cold_newest_id']
        real_response['cold_newest_date']  = request.meta['cold_newest_date']

        
        # formata o tweet, somente campos que iremos utilizar
        formated_tweets = []
        for tweet in tweets:
            formated_tweet = format_tweet(tweet)
            formated_tweet['search_term_group'] = request.meta['search_term_group']
            formated_tweet['search_term'] = _search
            formated_tweets.append(formated_tweet)
            if _newest_id <= int(formated_tweet['id_str']):
                _newest_id = int(formated_tweet['id_str']) + 1
                real_response['newest_id'] = _newest_id
                real_response['newest_date'] = formated_tweet['created_at']                    
        
        # Atualiza a quantidade de tweets retornados
        real_response['returned_tweets'] = len(formated_tweets)

        if(len(formated_tweets) == 0):
            real_response['data'] = []
            real_response['oldest_id'] = 1            
            real_response['oldest_date'] = datetime.datetime.fromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S')
            real_response['total_returned_tweets'] = int(request.meta['total_returned_tweets'])
            real_response['total_returned_tweets'] = int(request.meta['total_returned_tweets'])            
        else:
            try:            
                real_response['data'] = formated_tweets
                real_response['oldest_id'] = int(formated_tweets[-1]['id_str']) - 1
                real_response['oldest_date'] = formated_tweets[-1]['created_at']            
                real_response['total_returned_tweets'] = int(request.meta['total_returned_tweets']) + len(formated_tweets)            
            except:
                print_info("Erro ")
        
        # Retorna o dado trabalhado
        response = TextResponse(url=f"{request.url}", encoding='utf-8', body=json.dumps(real_response, ensure_ascii=False).encode('utf8'))
        return response
        #return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
