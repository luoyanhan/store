import os
import sys
import json
import time
import random
import datetime
from urllib.parse import urlparse


import ast
import asyncio
import requests
import redis
import ujson
import logging
import netaddr, netaddr.core
from kafka import KafkaProducer


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'downloader', ''))
from util.addition import retry_POOL_resp


class Base(object):
    SEARCH_DOMAIN = 'app.gsxt.gov.cn'
    HOST = 'app.gsxt.gov.cn'
    DOMAIN = 'app.gsxt.gov.cn'
    _search_page = {
        'url': "/gsxt/cn/gov/saic/web/controller/PrimaryInfoIndexAppController/search"
    }

    _base_page = {
        'url': "/gsxt/corp-query-entprise-info-primaryinfoapp-entbaseInfo-%s.html"
    }

    _shareholder_page = {
        'url': "/gsxt/corp-query-entprise-info-shareholder-%s.html"
    }

    _employe_page = {
        'url': "/gsxt/corp-query-entprise-info-KeyPerson-%s.html"
    }

    _branch_page = {
        'url': "/gsxt/corp-query-entprise-info-branch-%s.html"
    }

    _nerecitempub_page = {
        'url': "/gsxt/corp-query-entprise-info-getNeRecItemPubListByPripId-%s.html"
    }

    _liquidation_page = {
        'url': "/gsxt/corp-query-entprise-info-liquidation-%s.html"
    }

    _changerecord_page = {
        'url': "/gsxt/corp-query-entprise-info-alter-%s.html"
    }

    _mortreginfo_page = {
        'url': "/gsxt/corp-query-entprise-info-mortreginfo-%s.html"
    }

    _equitypledge_page = {
        'url': "/gsxt/corp-query-entprise-info-stakqualitinfo-%s.html"
    }

    _trademark_page = {
        'url': "/gsxt/corp-query-entprise-info-trademark-%s.html"
    }

    _checkinformation_page = {
        'url': "/gsxt/corp-query-entprise-info-spotCheckInfo-%s.html"
    }

    _drraninsres_page = {
        'url': "/gsxt/corp-query-entprise-info-getDrRaninsRes-%s.html"
    }

    _administrativelicensing_page = {
        'url': "/gsxt/corp-query-entprise-info-licenceinfoDetail-%s.html"
    }

    _administrativepenalty_page = {
        'url': "/gsxt/corp-query-entprise-info-punishmentdetail-%s.html"
    }

    _exceptioninformation_page = {
        'url': "/gsxt/corp-query-entprise-info-entBusExcep-%s.html"
    }

    _judicialinformation_page = {
        'url': "/gsxt/Affiche-query-info-assistInfo-%s.html"
    }

    _illegalinformation_page = {
        'url': "/gsxt/corp-query-entprise-info-illInfo-%s.html"
    }

    _self_administrativelicensing_page = {
        'url': "/gsxt/corp-query-entprise-info-insLicenceinfo-%s.html"
    }

    _self_shareholder_page = {
        'url': "/gsxt/corp-query-entprise-info-insinvinfo-%s.html"
    }

    _self_insalterstockinfo_page = {
        'url': "/gsxt/corp-query-entprise-info-insAlterstockinfo-%s.html"
    }

    _self_propledgereginfo_page = {
        'url': "/gsxt/corp-query-entprise-info-insProPledgeRegInfo-%s.html"
    }

    _self_appsimplecancelobjection_page = {
        'url': "/gsxt/corp-query-entprise-info-app-simple-cancel-objection-%s.html"
    }

    _self_epubgroupmenberinfo_page = {
        'url': "/gsxt/corp-query-entprise-info-getEPubGroupMenberInfo.html"
    }

    _self_abolishmentlicenseinfo_page = {
        'url': "/gsxt/corp-query-entprise-info-getElicenseNullfy.html"
    }

    _self_anchange_page = {
        'url': "/gsxt/corp-query-entprise-info-primaryinfoapp-annualReportAltInfo-%s.html"
    }

    _self_ancheyear_page = {
        'url': "/gsxt/corp-query-entprise-info-anCheYearInfo-%s.html"
    }

    _self_annualreportinfo_page = {
        'url': "/gsxt/corp-query-entprise-info-primaryinfoapp-annualReportInfo-%s.html"
    }

    def __init__(self, config):
        self.number = int(config['DOWNLOADER']['NUMBER'])
        self.sem_num = int(config['DOWNLOADER']['SEM_NUM'])
        self.threshold = int(config['DOWNLOADER']['THRESHOLD'])
        self.threshold_429 = float(config['DOWNLOADER']['THRESHOLD_429'])
        self.threshold_403 = float(config['DOWNLOADER']['THRESHOLD_403'])
        self.REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
        self.REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
        self.REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
        self.REDIS_TAG_EX = int(config['REDIS_TAG']['REDIS_TAG_EX'])
        self.KAFKA_BOOTSTRAP_SERVERS = config['KAFKA']['KAFKA_BOOTSTRAP_SERVERS']
        self.PROXY_API = config['API']['PROXY_API']
        self._base_headers = {'Content-Type': 'application/json;charset=utf-8',
                              'Accept': 'application/json',
                              'X-Requested-With': 'XMLHttpRequest',
                              'User-Agent': config['DOWNLOADER']['UA'],
                              'Host': self.DOMAIN,
                              'Connection': 'keep-alive',
                              'Accept-Encoding': 'gzip'}
        self.sem = asyncio.Semaphore(self.sem_num)
        self.result_li = dict()
        self.loop = asyncio.get_event_loop()
        with open(basedir.replace(os.sep + 'downloader', '') + os.sep + os.sep.join(['configs', 'source.json']), 'r') \
                as f:
            self.proxies_list = json.load(f)

    def domain_uri(self, uri=None):
        if uri is None:
            uri = '/'
        return "http://" + self.SEARCH_DOMAIN + uri

    def _fix_url(self, url):
        netloc = urlparse(url).netloc
        try:
            if netaddr.IPAddress(netloc.split(':')[0]).is_private():
                url = url.replace(netloc, self.SEARCH_DOMAIN)
        except netaddr.core.AddrFormatError:
            pass
        return url

    def build_post_url(self, method, url, params):
        if method != 'GET':
            url += '?'
            for i in params:
                url += str(i) + '=' + str(params[i]) + '&'
            url = url.strip('&')
        return url

    def use_abu_redis(self):
        if not hasattr(self, 'conn'):
            self.conn = redis.StrictRedis(host=self.REDIS_TAG_HOST, port=self.REDIS_TAG_PORT, db=self.REDIS_TAG_DB,
                                          max_connections=20, decode_responses=True)
        if hasattr(self, 'conn'):
            now = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%H:%M:%S")
            minute = now.split(':')[1]
            old_tuple = self.conn.get(minute)
            if old_tuple:
                old_tuple = ast.literal_eval(old_tuple)
                total = old_tuple[0]
                count_429 = old_tuple[1]
                count_403 = old_tuple[2]
                if count_403 / total <= self.threshold_403:
                    return True
                return False
            return True

    def tag_redis(self, resp_code):
        if not hasattr(self, 'conn'):
            self.conn = redis.StrictRedis(host=self.REDIS_TAG_HOST, port=self.REDIS_TAG_PORT, db=self.REDIS_TAG_DB,
                                          max_connections=20, decode_responses=True)
        if hasattr(self, 'conn'):
            now = datetime.datetime.now().strftime("%H:%M:%S")
            minute = now.split(':')[1]
            old_tuple = self.conn.get(minute)
            if old_tuple:
                # total, 429, 403
                old_tuple = ast.literal_eval(old_tuple)
                if resp_code == 429:
                    total = old_tuple[0] + 1
                    count_429 = old_tuple[1] + 1
                    new_tuple = (total, count_429, old_tuple[2])
                elif resp_code == 403:
                    total = old_tuple[0] + 1
                    count_403 = old_tuple[2] + 1
                    new_tuple = (total, old_tuple[1], count_403)
                else:
                    total = old_tuple[0] + 1
                    new_tuple = (total, old_tuple[1], old_tuple[2])
                self.conn.set(minute, str(new_tuple), ex=self.REDIS_TAG_EX)
                return (old_tuple[0], '%.2f' % (old_tuple[1] / old_tuple[0]), '%.2f' % (old_tuple[2] / old_tuple[0]))
            else:
                if resp_code == 429:
                    new_tuple = (1, 1, 0)
                elif resp_code == 403:
                    new_tuple = (1, 0, 1)
                else:
                    new_tuple = (1, 0, 0)
                self.conn.set(minute, str(new_tuple), ex=self.REDIS_TAG_EX)
                return None

    def is_legal_resptext(self, resp):
        text = resp.text
        if 'recordsTotal' in resp.json() and resp.json()['recordsTotal'] == 0:
            return True
        if 'NGIDERRORCODE' in text or '[]' == text or ('data' in json.loads(resp.text) and not
        json.loads(resp.text)['data']):
            return False
        return True

    def loopable_download(self, method, url, params, keyword, tag, data, retry=20, timeout=10):
        url = self._fix_url(url)
        url = self.build_post_url(method, url, params)
        while retry:
            try:
                time.sleep(random.randrange(0, 1))
                if self.use_abu_redis():
                    proxies = random.choice(self.proxies_list)
                else:
                    proxy = retry_POOL_resp(self.PROXY_API)
                    proxies = {'all': proxy}
                if method == 'GET':
                    resp = requests.get(url, headers=self._base_headers, params=params, timeout=timeout,
                                        proxies=proxies)
                else:
                    resp = requests.post(url, headers=self._base_headers, data=data, timeout=timeout, proxies=proxies)
                http_code = resp.status_code
                result = self.tag_redis(http_code)
                if result:
                    logging.info('总次数：' + str(result[0]) + ' 429百分比：' + str(result[1]) + ' 403百分比：' +
                                 str(result[2]))
                if http_code in [429, 403]:
                    time.sleep(random.randrange(0, 1))
                    proxy = retry_POOL_resp(self.PROXY_API)
                    proxies = {'all': proxy}
                    if method == 'GET':
                        resp = requests.get(url, headers=self._base_headers, params=params, timeout=timeout,
                                            proxies=proxies)
                    else:
                        resp = requests.post(url, headers=self._base_headers, data=data, timeout=timeout,
                                             proxies=proxies)
                    http_code = resp.status_code
                    result = self.tag_redis(http_code)
                resp.encoding = 'utf-8'
                if not self.is_legal_resptext(resp):
                    raise Exception
                else:
                    self.result_li[keyword][tag] = resp.text
                    logging.info('finished:' + str(keyword) + ' ' + str(tag) + ' ' + str(retry))
                    return resp
            except Exception as err:
                retry -= 1
        try:
            self.result_li[keyword][tag] = 'FAIL'
        except Exception as e:
            self.number -= 1

    async def async_pages_download(self, method, url, params, keyword, tag, data):
        resp = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword, tag, data)
        if resp:
            self.result_li[keyword][tag] = resp.text
            return (keyword, tag, True, resp)
        else:
            return (keyword, tag, False)

    def add_to_mq(self, item, topic):
        retry = 0
        if not self.producer:
            self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS, api_version=(0, 10))
        new_item = ujson.dumps(item)
        new_item = bytes(new_item, encoding='utf-8')
        future = self.producer.send(topic=topic, value=new_item)
        self.producer.flush()
        while not bool(future.get()):
            retry += 1
            if retry > 20:
                return False
            time.sleep(1)
            if not self.producer:
                self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS, api_version=(0, 10))
            future = self.producer.send(topic=topic, value=new_item)
        return True

    def get_dict_values(self, keyword):
        li = []
        li += list(self.result_li[keyword].values())
        return list(set(li))
