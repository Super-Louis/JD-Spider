import requests, json
from pprint import pprint
from lxml import etree
import re
#
headers = {
    'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36'
}
# url = 'https://list.jd.com/list.html?cat=9987,653,655&page=1&sort=sort_totalsales15_desc&trans=1&JL=4_2_0#J_main'
# detail_url = 'https://item.jd.com/4207732.html'
comment_url = 'https://club.jd.com/comment/skuProductPageComments.action?&productId=6773559&score=0&sortType=5&page=1&pageSize=10&isShadowSku=0&rid=0&fold=1'
s = requests.Session()

s.headers = headers

r = s.get(comment_url)
pprint(json.loads(r.text))
# r = s.get(detail_url)
# print(r.text)
# res = r.text
# tree = etree.HTML(res)
# name = tree.xpath('//div[@class="sku-name"]/text()')[0].strip()
# store = tree.xpath('//div[@class="J-hove-wrap EDropdown fr"]/div[@class="item"]/div/a/text()')[0].strip()
# price_url = 'https://p.3.cn/prices/mgets?skuIds=J_6773559'
# r = s.get(price_url)
# print(r.text)
# price = json.loads(r.text)[0]['p']
#
# print(price)
# mess = re.findall('slaveWareList =(.+?)\n', res)[0]
# print(mess)
# ids1 = re.findall('(\d+):\[{', mess)
# print(len(ids1))
# ids2 = re.findall('"(\d+)":{', mess)
# print(len(ids2))
# ids = ids1+ids2
# print(len(ids))
# tree = etree.HTML(r.text)
# ids = tree.xpath('//li[@class="gl-item"]//li[@class="ps-item"]//img/@data-sku')
# print(len(ids))
# pprint(json.loads(r.text))
# import asyncio
#
# async def d(i):
#     a = dict()
#     a['value'] = i
#     a['value5'] = i + 5
#     await s(a)
#
# async def s(a):
#     await asyncio.sleep(2)
#     print(a)
#
# async def task():
#     tasks = [d(i) for i in range(1000)]
#     await asyncio.gather(*tasks)
#
# def run():
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(task())
#     loop.close()
#
# run()