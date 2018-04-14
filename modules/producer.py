import asyncio
import aiohttp
import traceback
import logging
import re
import urllib
import motor.motor_asyncio
import time
import aioredis
from spider_filter import BloomFilter
from logging.handlers import TimedRotatingFileHandler
import multiprocessing
from spider_queue import *
from db_config import DB
from lxml import etree

basic_info_url = 'https://item.jd.com/{}.html'
price_url = 'https://p.3.cn/prices/mgets?skuIds=J_{}'
comment_url = 'https://club.jd.com/comment/skuProductPageComments.action?&productId={}&score=0&sortType=5&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1'
headers = {
        'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36',
        }
l = logging.getLogger('run')


class Producer:
    def __init__(self):
        self._session = None

    def __enter__(self):
        return self

    @property
    def session(self):
        if self._session is None:
            conn = aiohttp.TCPConnector(verify_ssl=False,
                                        limit=100,  # 连接池不能太大
                                        use_dns_cache=True)
            self._session = aiohttp.ClientSession(connector=conn)
        return self._session

    async def request_page(self,url,data=None,params=None,headers=None,proxy=False,tag=None):
        if not proxy:
            retry = 0
            while retry < 3:
                try:
                    if data:
                        async with self.session.post(url,data=data,headers=headers,timeout=3) as response:
                            return await response.json() if tag=='json' else await response.text()
                    if params:
                        async with self.session.get(url,params=params,headers=headers,timeout=3) as response:
                            return await response.json() if tag=='json' else await response.text()
                    else:
                        async with self.session.get(url, headers=headers, timeout=3) as response:
                            return await response.json() if tag == 'json' else await response.text()
                except Exception as e:
                    l.info(e)
                    print(e)
                    retry += 1
                    continue
        retry = 0
        while True:
            retry += 1
            if retry == 30:
                return None
            tag, ip = await self.mq.get('proxy_queue')
            try:
                if data:
                    async with self.session.post(url, data=data, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # l.info("proxy:{} is valid".format(ip))
                        await self.mq.put('proxy_queue',ip)
                        return await response.json() if tag == 'json' else await response.text()

                if params:
                    async with self.session.get(url, params=params, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # l.info("proxy:{} is valid".format(ip))
                        await self.mq.put('proxy_queue',ip)
                        return await response.json() if tag == 'json' else await response.text()

                else:
                    async with self.session.get(url, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # l.info("proxy:{} is valid".format(ip))
                        await self.mq.put('proxy_queue',ip)
                        return await response.json() if tag == 'json' else await response.text()
            except Exception as e:
                l.info(e)
                print(e)
                await asyncio.sleep(3)
                # l.info("proxy:{} is not valid".format(ip))
                continue

    async def crawler_entry(self):#初始从Task_Gernator中随机选取一个种子用户
        task = await self.redis.spop('jd:task')
        if task:
            task = json.loads(task)
            l.info("get task:{}".format(task))
            id, page = task['id'], task['page']
            basic_res = await self.request_page(url=basic_info_url.format(id), headers=headers,proxy=True,tag='text')
            tree = etree.HTML(basic_res)
            name = tree.xpath('//div[@class="sku-name"]/text()')[0].strip()
            store = tree.xpath('//div[@class="J-hove-wrap EDropdown fr"]/div[@class="item"]/div/a/text()')[0].strip()
            price = await self.request_page(url=price_url.format(id), headers=headers,proxy=True,tag='json')
            if type(price) == str:
                price = json.loads(price)
            try:
                price =  price and price[0]['p'] or None
                l.info('get price: {}'.format(price))
            except Exception as e:
                l.info(e)
                l.info('price: {}'.format(price))
                price = None
            comments = await self.request_page(url=comment_url.format(id, page), headers=headers,proxy=True,tag='text')
            message = {
                'name': name,
                'store': store,
                'price': price,
                'page': page,
                'id': id,
                'comments': json.loads(comments)
            }
            l.info('get message: {}'.format(message))
            await self.mq.put('jd_phone',json.dumps(message))

    async def tasks(self):
        self.mq = await AsyncMqSession()
        self.redis = await aioredis.create_redis((DB['redis']['host'], DB['redis']['port']),password=DB['redis']['password'],encoding='utf-8')
        while True:
            tasks = [self.crawler_entry() for _ in range(10)]
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                l.info(e)

    def worker(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tasks())
        loop.close()

    def run(self):
        ps = list()
        for i in range(5):
            p = multiprocessing.Process(target=self.worker, args=())
            ps.append(p)
            p.start()
        for p in ps:
            p.join()


    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_tb:

            msg = f'exc type: {exc_type}, val: {exc_val}'
            l.info(msg)
            tb_list = traceback.format_exception(exc_type, exc_val, exc_tb)
            tb_str = ''.join(tb_list)
            l.error(tb_str)
            return False
        else:
            l.info("No exception")
            return True


if __name__ == '__main__':
    with Producer() as up:
        up.run()

