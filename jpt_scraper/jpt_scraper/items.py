# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class JptScraperItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    excerpt = scrapy.Field()
    published_date = scrapy.Field()
    topics = scrapy.Field()
    tags = scrapy.Field()
    scraped_at = scrapy.Field()
    refresh_existing = scrapy.Field()      # ISO timestamp
    pass
