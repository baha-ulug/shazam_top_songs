from scrapy.crawler import CrawlerProcess
from shazamspider import ShazamSpider
import json
import sys

'''
def lambda_handler(event, context):
    given_argument = event['region']
    print(f"Scraping request for the {given_argument} is received")
    process = CrawlerProcess(settings={'LOG_LEVEL': 'INFO'})
    process.crawl(AmazonSpider, page = given_argument)
    print("Amazon spider starts to run")
    process.start()
    print("All categories are collected")
    AmazonSpider(page=given_argument).db_insert()
    print("All data is inserted to db")
   
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
'''
def main(arg):    
    process = CrawlerProcess(settings={'LOG_LEVEL': 'INFO'})
    process.crawl(ShazamSpider, region_input=arg)
    print("Amazon spider starts to run")
    process.start()
    print("All categories are collected")
    ShazamSpider(region_input=arg).db_insert()
    print("All data is inserted to db")

    return "Success!"

if __name__=='__main__':
    arg = sys.argv[1]
    print(arg)
    main(arg)