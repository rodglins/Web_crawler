import scrapy


class SpiderMagaluSpider(scrapy.Spider):
    name = 'spider_magalu'
    start_urls = [f'https://www.magazineluiza.com.br/notebook-e-macbook/informatica/s/in/ntmk?page={i}' for i in range(1,34)]
    # start_urls = [f'https://www.magazineluiza.com.br/notebook-e-macbook/informatica/s/in/ntmk?page=1']
    

    def parse(self, response, **kwargs):
        for i in response.xpath('//a[@name="linkToProduct"]'):
            price = i.xpath('.//span[@data-css-lz0zr]//text()').getall()
            price = i.xpath('.//span[@data-css-1ay1ynh]//text()').getall()
            #price = i.xpath('.//div[@data-css-lz0zr]//text()').getall()
            title = i.xpath('.//h3[@data-css-1ve3vkk]//text()').get()
            # link = i.xpath('./a/@href').get()

            yield{
            'price' : price,
            'title' : title,
            # 'link' : link
            }

            # Alternativa para carregar as páginas automaticamente:
            # next_page = response.xpath('//a[contains(@title, "Próxima")]/@href').get()
            # if next_page:
            #    yield scrapy.Request(url=next_page, callback=self.parse)