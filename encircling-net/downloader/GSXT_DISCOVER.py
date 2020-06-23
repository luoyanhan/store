#coding=utf-8
import json
import math
import os
import sys
from functools import partial
from multiprocessing import Pool


import asyncio
import redis
import dawg
import logging
from kafka import KafkaProducer


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'downloader', ''))
sys.path.append(basedir)


from base import Base
from util.addition import level1_dawg_word, set_of_pre


class DownloadItem(Base):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.KAFKA_TOPIC = config['KAFKA']['KAFKA_TOPIC']
        self.dawg_loader_limit = int(config['DOWNLOADER']['DAWG_LOADER_LIMIT'])
        self.process_num = int(config['DOWNLOADER']['PROCESS_NUM'])
        self.cut_threshold = int(config['DOWNLOADER']['CUT_THRESHOLD'])
        self.REDIS_DAWG_LIST_NAME = config['REDIS_TAG']['REDIS_DAWG_LIST_NAME']
        self.dawg_dir = config['DAWG']['DIR']
        self.dawg_redis_conn = redis.StrictRedis(self.REDIS_TAG_HOST, self.REDIS_TAG_PORT, self.REDIS_TAG_DB)
        self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS, api_version=(0, 10))
        self.dawg_loader_dict = dict()
        logging.basicConfig(level=logging.INFO,
                            filename=basedir + self.config['LOG']['DIR'],
                            filemode='a',
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                            )


    def init_redis(self):
        pool = Pool(self.process_num)
        results = list()
        for i in range(len(level1_dawg_word)):
            # file_name = os.path.join('/'.join(basedir.split('/')[:-2]), 'names/') + level1_dawg_word[i] + '.dawg'
            file_name = self.dawg_dir + '/' + level1_dawg_word[i] + '.dawg'
            keyword = level1_dawg_word[i]
            result = pool.apply_async(set_of_pre, args=(file_name, keyword, self.cut_threshold, 2))
            results.append(result)
        pool.close()
        pool.join()
        for each in results:
            companies = each.get()
            if companies:
                self.dawg_redis_conn.lpush(self.REDIS_DAWG_LIST_NAME, *companies)


    def request_code_tuple(self):
        if not self.dawg_redis_conn:
            self.dawg_redis_conn = redis.StrictRedis(self.REDIS_TAG_HOST, self.REDIS_TAG_PORT, self.REDIS_TAG_DB)
        redis_result = self.dawg_redis_conn.rpop(self.REDIS_DAWG_LIST_NAME)
        if not redis_result:
            return False
        return redis_result.decode()

    def callback_result(self, future, keyword, tag=None):
        li = self.get_dict_values(keyword)
        if False not in li:
            item = self.result_li[keyword]
            result = self.add_to_mq({keyword: item}, self.KAFKA_TOPIC)
            del self.result_li[keyword]
            self.number -= 1
            if self.number < self.threshold:
                code = self.request_code_tuple()
                if code:
                    self.result_li[code] = {'search_1': False}
                    task = self.loop.create_task(self.search(code))
                    task.add_done_callback(partial(self.callback_result, keyword=code))
                    self.number += 1
                else:
                    if len(list(self.result_li.keys())) == 0:
                        for task in asyncio.Task.all_tasks():
                            task.cancel()
                        self.loop.stop()

    def do_batch(self):
        length = self.dawg_redis_conn.llen(self.REDIS_DAWG_LIST_NAME)
        if not length:
            self.init_redis()
        while self.number < self.threshold:
            code = self.request_code_tuple()
            logging.info(code)
            if code:
                self.result_li[code] = {'search_1': False}
                task = self.loop.create_task(self.search(code))
                task.add_done_callback(partial(self.callback_result, keyword=code))
                self.number += 1
            else:
                if len(list(self.result_li.keys())) == 0:
                    for task in asyncio.Task.all_tasks():
                        task.cancel()
                    self.loop.stop()
        self.loop.run_forever()

    def set_of_pre_use_loader_cache(self, loader, keyword, index):
        companies = loader.keys(keyword)
        pre_dict = dict()
        for company in companies:
            pre = company[:index]
            num = pre_dict.get(pre, 0)
            num += 1
            pre_dict[pre] = num
        return pre_dict

    async def search(self, keyword, page=0):
        logging.info('in search code is:' + keyword)
        post_json = {
            "searchword": keyword,
            "conditions": {
                "excep_tab": "0",
                "ill_tab": "0",
                "area": self.config['PARM']['AREA'],
                "eYear": self.config['PARM']['EYEAR'],
                "cStatus": "0",
                "xzxk": "0",
                "xzcf": "0",
                "dydj": "0"
            }
        }
        url = self.domain_uri(self._search_page['url'])
        data = json.dumps(post_json, ensure_ascii=False).encode('utf-8')
        async with self.sem:
            result = await self.async_pages_download('POST', url, {'page': page}, keyword, 'search_1', data)
        if result[2]:
            try:
                items = json.loads(result[3].text)
            except Exception as e:
                self.result_li[keyword]['search'] = 'FAIL'
                return
            num = items['data']['result']['recordsTotal']
            logging.info(str(keyword) + ' 条目数: ' + str(num))
            if num == 0:
                self.result_li[keyword]['search'] = 'FAIL'
                self.result_li[keyword]['message'] = '条目数为0'
                return
            elif num >= 100:
                logging.info(str(keyword) + ' 条目数大于100')
                if not self.dawg_redis_conn:
                    self.dawg_redis_conn = redis.StrictRedis(self.REDIS_TAG_HOST, self.REDIS_TAG_PORT,
                                                             self.REDIS_TAG_DB)
                file_name = os.path.join('/'.join(basedir.split('/')[:-2]), 'names/') + keyword[0] + '.dawg'
                # (d which loaded dawg, using number, size of dawg)
                while True:
                    if keyword[0] in self.dawg_loader_dict:
                        loader = self.dawg_loader_dict[keyword[0]][0]
                        self.dawg_loader_dict[keyword[0]][1] = self.dawg_loader_dict[keyword[0]][1] + 1
                        result = self.set_of_pre_use_loader_cache(loader, keyword, len(keyword)+1)
                        self.dawg_loader_dict[keyword[0]][1] = self.dawg_loader_dict[keyword[0]][1] - 1
                        break
                    else:
                        if len(self.dawg_loader_dict.keys()) >= self.dawg_loader_limit:
                            temp = [[key, self.dawg_loader_dict[key][2]] for key in self.dawg_loader_dict\
                                    if self.dawg_loader_dict[key][1] == 0]
                            if not temp:
                                continue
                            temp.sort(key=lambda x: x[1], reverse=True)
                            del self.dawg_loader_dict[temp[0][0]]
                            loader = dawg.CompletionDAWG()
                            loader.load(file_name)
                            self.dawg_loader_dict[keyword[0]] = [loader, 1, level1_dawg_word.index(keyword[0])]
                            result = self.set_of_pre_use_loader_cache(loader, keyword, len(keyword) + 1)
                            self.dawg_loader_dict[keyword[0]][1] = self.dawg_loader_dict[keyword[0]][1] - 1
                            break
                        else:
                            loader = dawg.CompletionDAWG()
                            loader.load(file_name)
                            self.dawg_loader_dict[keyword[0]] = [loader, 1, level1_dawg_word.index(keyword[0])]
                            result = self.set_of_pre_use_loader_cache(loader, keyword, len(keyword) + 1)
                            self.dawg_loader_dict[keyword[0]][1] = self.dawg_loader_dict[keyword[0]][1] - 1
                            break
                for each in result:
                    if result[each] >= self.cut_threshold:
                        self.dawg_redis_conn.rpush(self.REDIS_DAWG_LIST_NAME, each)
            # 开始翻页
            elif num > 10:
                logging.info(str(keyword) + ' 开始翻页')
                page_num = math.ceil(num/10)
                for page in range(2, page_num+1):
                    tag = 'search_' + str(page)
                    self.result_li[keyword][tag] = False
                    async with self.sem:
                        result = await self.async_pages_download('POST', url, {'page': page}, keyword, tag, data)

