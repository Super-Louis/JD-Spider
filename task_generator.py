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
import sys
import re

#  按销量排行
basic_url = 'https://list.jd.com/list.html?cat=9987,653,655' \
                 '&page={}&sort=sort_totalsales15_desc&trans=1&JL=4_2_0#J_main'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

headers = {
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36',
        }


class Task_Producer():
    def __init__(self):
        self._session = None
        self.client = motor.motor_asyncio.AsyncIOMotorClient(f"mongodb://{DB['mongo']['user']}:{DB['mongo']['password']}@{DB['mongo']['host']}:{DB['mongo']['port']}")
        self.collection = self.client.weibo.user_detail_info
        self.bf = BloomFilter()

    def __enter__(self):
        return self

    @property
    def session(self):
        if self._session is None:
            conn = aiohttp.TCPConnector(verify_ssl=False,
                                        limit=1000,  # 连接池不能太大
                                        use_dns_cache=True)
            self._session = aiohttp.ClientSession(connector=conn)
        return self._session

    async def request_page(self,url,data=None,params=None,headers=None,proxy=False):
        if not proxy:
            retry = 0
            while retry < 3:
                try:
                    if data:
                        async with self.session.post(url,data=data,headers=headers,timeout=3) as response:
                            return await response.text()
                    if params:
                        async with self.session.get(url,params=params,headers=headers,timeout=3) as response:
                            return await response.text()
                    else:
                        async with self.session.get(url,headers=headers,timeout=3) as response:
                            return await response.text()
                except Exception as e:
                    logging.info(e)
                    retry += 1
                    continue
        retry = 0
        while True:
            retry += 1
            if retry == 11:
                return ''
            # print('aaa')
            tag, ip = await self.proxy_mq.get('proxy_queue')
            # print("aaa")
            try:
                if data:
                    async with self.session.post(url, data=data, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        return await response.text()

                if params:
                    async with self.session.get(url, params=params, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        return await response.text()

                else:
                    async with self.session.get(url,headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        return await response.text()
            except Exception as e:
                logging.info(e)
                # logging.info("proxy:{} is not valid".format(ip))
                continue

    async def crawler_entry(self,page=None):#初始从Task_Gernator中随机选取一个种子用户
        # id = await self.redis.spop('user_id')
        self.mq = await AsyncMqSession()
        self.proxy_mq = await AsyncMqSession()
        self.redis = await aioredis.create_redis((DB['redis']['host'], DB['redis']['port']),
                                                 password=DB['redis']['password'], encoding='utf-8')
        page = await self.redis.get('jd:index:page')
        logging.info("get page:{}".format(page))
        index_url = basic_url.format(page)
        basic_res = await self.request_page(url=index_url, headers=headers, proxy=False)
        mess = re.findall('slaveWareList =(.+?)\n', basic_res)[0]
        ids1 = re.findall('(\d+):\[{', mess)
        ids2 = re.findall('"(\d+)":{', mess)
        ids = ids1 + ids2
        tasks = [self.insert_redis(id) for id in ids]
        await asyncio.gather(*tasks)

    async def insert_redis(self, id):

        for p in range(1, 100): # 评论最多显示100页
            task = {
                'id': id,
                'page': str(p)
            }
            await self.redis.sadd('jd:task', json.dumps(task))

    # async def task(self, page):
    #
    #     tasks = [self.crawler_entry(page)]
    #     await asyncio.gather(*tasks)

    def worker(self):

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.crawler_entry())
        loop.close()


    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_tb:

            msg = f'exc type: {exc_type}, val: {exc_val}'
            logging.info(msg)
            tb_list = traceback.format_exception(exc_type, exc_val, exc_tb)
            tb_str = ''.join(tb_list)
            logging.error(tb_str)
            return False
        else:
            logging.info("No exception")
            return True


if __name__ == '__main__':
    with Task_Producer() as up:
        up.worker()

