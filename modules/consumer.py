import asyncio
import aiohttp
import traceback
import logging
import re
import urllib
import motor.motor_asyncio
import time
import json
from spider_queue import *
import multiprocessing
from db_config import DB
import hashlib

l = logging.getLogger('run')


class Consumer:
    def __init__(self):
        self._session = None
        self.client = motor.motor_asyncio.AsyncIOMotorClient(f"mongodb://{DB['mongo']['user']}:{DB['mongo']['password']}@{DB['mongo']['host']}:{DB['mongo']['port']}")
        self.collection = self.client.jd.phones

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

    def check_exists(self,res,key):
        try:
            return res[key]
        except:
            return ''

    async def get_details(self):#初始从Task_Gernator中随机选取一个种子用户
        tag, message = await self.mq.get('jd_phone')
        message = json.loads(message.decode('utf-8'))
        l.info("get task id:{}, page:{}".format(message['id'], message['page']))
        item = dict()
        comments = message['comments']
        item['product_id'] = message['id']
        item['product_name'] = message['name']
        item['product_store'] = message['store']
        item['product_price'] = message['price']
        item['comment_num'] = comments["productCommentSummary"]['commentCount']
        item['good_rate'] = comments["productCommentSummary"]["goodRate"]
        item['poor_rate'] = comments["productCommentSummary"]["poorRate"]
        item['hotCommentTag'] = [{'count': t['count'], 'tag': t['name']} for t in comments['hotCommentTagStatistics']]
        for detail in comments['comments']:
            comment_detail = dict()
            comment_detail['page'] = message['page']
            comment_detail['produt_referenceName'] = detail['referenceName']  # 产品名称的补充
            comment_detail['user_id'] = detail['id']
            comment_detail['nickname'] = detail['nickname']
            comment_detail['userLevelName'] = detail['userLevelName']
            comment_detail['userClientShow'] = detail['userClientShow']
            comment_detail['creationTime'] = detail['creationTime']
            comment_detail['days'] = detail['days']
            comment_detail['score'] = detail['score']
            comment_detail['content'] = detail['content']
            item['comment_detail'] = comment_detail
            await self.insert_mongo(item)

    async def insert_mongo(self, item):
        hashed_key = hashlib.md5(json.dumps(item, sort_keys=True, ensure_ascii=False)
                                 .encode('utf8')).hexdigest()[8:-8]
        hashed_id = int(hashed_key, 16)
        item['id'] = str(hashed_id)
        try:
            await self.collection.insert_one(item.copy())
            l.info("insert id :{} to mongo".format(item['id']))
        except Exception as e:
            l.info(e)
            pass


    async def tasks(self):
        self.mq = await AsyncMqSession()
        self.proxy_mq = await AsyncMqSession()
        while True:
            tasks = [self.get_details() for _ in range(100)]
            print(len(tasks))
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                l.info(e)
                pass

    def worker(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tasks())
        loop.close()

    def run(self):
        ps = list()
        for i in range(3):
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
    with Consumer() as uc:
        uc.run()

