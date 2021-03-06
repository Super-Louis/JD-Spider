import logging
import os
from logging.handlers import TimedRotatingFileHandler
import multiprocessing
from modules import Producer, Consumer
from fetch_proxy import CrawlerProxy
from db_config import DB
import redis
from task_generator import Task_Producer as TP
import time, random


class Run():

    def __init__(self,crawler):
        self.crawler = crawler
        self.logger = self.init_logger()
        self.redis = redis.Redis(host=DB['redis']['host'], port=DB['redis']['port'], password=DB['redis']['password'], decode_responses=True)
        self.tp = TP()

    def init_logger(self):
        logger = logging.getLogger('run')  # 创建一个logger
        logger.setLevel(logging.INFO)  # logger等级总开关
        # 创建一个handler,用于写入日志文件(timed rotating file handler)
        logfile = os.path.abspath('./modules/logs/run.log')
        fh = TimedRotatingFileHandler(
            filename=logfile,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        fh.setLevel(logging.INFO)  # 输出到file的等级总开关
        # 创建一个handler,用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # 定义handler的输出格式
        formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # 将logger添加到handler里面
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    def generate_ids(self):
        l = self.logger
        while True:
            redis_len = self.redis.scard('jd:task')
            if redis_len < 5000:
                l.info("id set is not enough, generate task...")
                try:
                    traversal_id = int(self.redis.get('jd:index:page'))
                    l.info("current index_page is is {}".format(traversal_id))
                    self.tp.worker()
                    traversal_id += 1
                    self.redis.set('jd:index:page', str(traversal_id))
                except Exception as e:
                    l.info(e)
                    time.sleep(random.uniform(1,3))
            else:
                time.sleep(1800)

    def run(self):
        task_list = [self.generate_ids, CrawlerProxy().run, Producer().run, Consumer().run]
        ps = []
        for t in task_list:
            p = multiprocessing.Process(target=t,args=())
            ps.append(p)
            p.start()
        for p in ps:
            p.join()


if __name__ == '__main__':
    run = Run('JD')
    run.run()