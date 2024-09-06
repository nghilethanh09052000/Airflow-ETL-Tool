import scrapy
from datetime import datetime


class IndexSpider(scrapy.Spider):
    name = 'steam'
    base_url = 'https://api.gamalytic.com/steam-games/list'
    
    def start_requests(self):
        self.current_date = datetime.now().strftime("%Y%m%d")
        self.current_page = 0
        self.total_pages = 1
        
        yield scrapy.Request(url=self.base_url, callback=self.parse, meta={'page': self.current_page})
    
    def parse(self, response):
        data = response.json()
        self.total_pages = data.get('pages', self.total_pages)
        results = data.get('result', [])
        
        if results:
            for game in results:
                yield {
                    'steamId': game.get('steamId'),
                    'id': game.get('id'),
                    'name': game.get('name'),
                    'price': game.get('price'),
                    'reviews': game.get('reviews'),
                    'followers': game.get('followers'),
                    'reviewScore': game.get('reviewScore'),
                    'copiesSold': game.get('copiesSold'),
                    'revenue': game.get('revenue'),
                    'avgPlaytime': game.get('avgPlaytime'),
                    'tags': game.get('tags'),
                    'genres': game.get('genres'),
                    'features': game.get('features'),
                    'developers': game.get('developers'),
                    'publishers': game.get('publishers'),
                    'unreleased': game.get('unreleased'),
                    'earlyAccess': game.get('earlyAccess'),
                    'releaseDate': self.format_date(game.get('releaseDate')),
                    'EAReleaseDate': self.format_date(game.get('EAReleaseDate')),
                    'publisherClass': game.get('publisherClass')
                }

            # Go to the next page if there are more pages
            if self.current_page < self.total_pages:
                self.current_page += 1
                next_page = f'{self.base_url}?page={self.current_page}'
                yield scrapy.Request(url=next_page, callback=self.parse, meta={'page': self.current_page})
        else:
            self.logger.error(f'Failed to retrieve data for page {self.current_page + 1}')

    def format_date(self, timestamp):
        if timestamp:
            return datetime.fromtimestamp(int(timestamp) / 1000).strftime('%Y-%m-%d')
        return None
