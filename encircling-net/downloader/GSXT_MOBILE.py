#coding=utf-8
import requests
import time
import bs4
import os
import sys
import json
from urllib import parse
from functools import partial
from kafka import KafkaProducer
import logging


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'downloader', ''))
sys.path.append(basedir)


from base import Base
from util.sbcdbc import SBC2DBC


class DownloadItem(Base):
    def __init__(self, config, detail_types):
        super().__init__(config)
        self.config = config
        self.detail_types = detail_types
        self.KAFKA_TOPIC = self.config['KAFKA']['KAFKA_TOPIC']
        self.DETAIL_COMPANY_URL = self.config['API']['DETAIL_COMPANY_URL']
        self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS, api_version=(0, 10))
        logging.basicConfig(level=logging.INFO,
                            filename=basedir + self.config['LOG']['DIR'],
                            filemode='a',
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                            )


    def request_code(self):
        code = requests.get(self.DETAIL_COMPANY_URL)
        if not code:
            return False
        code = code.content.decode("unicode_escape")
        if '[' in code:
            temp = json.loads(code.strip())
            code = temp[0]
            years = [str(year) for year in temp[1]]
            return code, years
        else:
            code = code.replace('"', '')
            code = code.strip()
            return code

    def callback_result(self, future, keyword, tag=None):
        li = self.get_dict_values(keyword)
        if False not in li:
            item = self.result_li[keyword]
            result = self.add_to_mq({keyword: item}, self.KAFKA_TOPIC)
            del self.result_li[keyword]
            self.number -= 1
            while self.number < self.threshold:
                code = self.request_code()
                if isinstance(code, tuple):
                    code = code[0]
                    years = code[1]
                    self.result_li[code] = {'search': False}
                    task = self.loop.create_task(self.search(code, years))
                    task.add_done_callback(partial(self.callback_result, keyword=code))
                    self.number += 1
                elif code != 'null':
                    self.result_li[code] = {'search': False}
                    task = self.loop.create_task(self.search(code))
                    task.add_done_callback(partial(self.callback_result, keyword=code))
                    self.number += 1

    async def get_annual(self, keyword, search_item, info_page, years):
        params = {'nodeNum': search_item['nodeNum'],
                  'entType': search_item['entType']}
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        async with self.sem:
            first_page = await self.async_pages_download('GET', url, params, keyword, 'firstpage', None)
        if not first_page[2]:
            return
        else:
            first_page = first_page[3]
            self.result_li[keyword]['firstpage'] = first_page.text
            annual_items = json.loads(first_page.text)
            for page in range(len(annual_items)):
                item = annual_items[page]
                if item['anCheYear'] not in years:
                    self.result_li[keyword][item['anCheYear']] = False
            for page in range(len(annual_items)):
                item = annual_items[page]
                if item['anCheYear'] not in years:
                    item_url_par = parse.parse_qs(parse.urlparse(item['annRepDetailUrl']).query)
                    item_params = {'nodeNum': int(item_url_par['nodeNum'][0]),
                                   'entType': int(item_url_par['entType'][0]),
                                   'anCheYear': item['anCheYear'],
                                   'anCheId': item['anCheId']}
                    url = self.domain_uri(self._self_annualreportinfo_page['url'] % (
                        search_item['pripid']))
                    task = self.loop.create_task(self.async_pages_download(
                        'GET', url, item_params, keyword, item['anCheYear'], None))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword, tag=item['anCheYear']))

    async def get_base_info(self, keyword, search_item, info_page):
        params = {'nodeNum': search_item['nodeNum'],
                  'entType': search_item['entType']}
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        async with self.sem:
            base_info = await self.async_pages_download('GET', url, params, keyword, 'base_info', None)
        if not base_info[2]:
            return
        else:
            base_info = base_info[3]
            self.result_li[keyword]['base_info'] = base_info.text

    async def get_shareholder(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            sharehold_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                              method, url, params, keyword, 'sharehold_0page', None)
        if sharehold_0page is None:
            return None
        else:
            self.result_li[keyword]['sharehold_0page'] = sharehold_0page.text
        sharehold_items = json.loads(sharehold_0page.text)
        if sharehold_items['recordsTotal'] > 0:
            for index, each in enumerate(sharehold_items['data']):
                sharehold_name = 'sharehold_0page' + each['inv']
                url = each['url']
                url = self._fix_url(url)
                self.result_li[keyword][sharehold_name] = False
                task = self.loop.create_task(self.async_pages_download(
                    'POST', url, params, keyword, sharehold_name, None))
                task.add_done_callback(partial(self.callback_result, keyword=keyword))
        # 开始翻页
        totalPage = sharehold_items['totalPage']
        perPage = sharehold_items['perPage']
        for page in range(1, totalPage):
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword]['sharehold_{}page'.format(page)] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download,
                                                            method, url, params, keyword,
                                                            'sharehold_{}page'.format(page), None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword]['sharehold_{}page'.format(page)] = innerpage.text
            sharehold_items = json.loads(innerpage.text)
            if sharehold_items['recordsTotal'] > 0:
                for index, each in enumerate(sharehold_items['data']):
                    sharehold_name = 'sharehold_{}page'.format(page) + each['inv']
                    url = each['url']
                    url = self._fix_url(url)
                    self.result_li[keyword][sharehold_name] = False
                    task = self.loop.create_task(self.async_pages_download(
                        'POST', url, params, keyword, sharehold_name, None))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

    async def get_keyPerson(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            keyPerson_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                              method, url, params, keyword, 'keyPerson_0page', None)
        if keyPerson_0page is None:
            return None
        else:
            self.result_li[keyword]['keyPerson_0page'] = keyPerson_0page.text
        keyPerson_items = json.loads(keyPerson_0page.text)
        # 开始翻页
        totalPage = keyPerson_items['totalPage']
        perPage = keyPerson_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'keyPerson_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_branch(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            branch_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                           method, url, params, keyword, 'branch_0page', None)
        if branch_0page is None:
            return None
        else:
            self.result_li[keyword]['branch_0page'] = branch_0page.text
        branch_items = json.loads(branch_0page.text)
        # 开始翻页
        totalPage = branch_items['totalPage']
        perPage = branch_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'branch_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_nerecitempub(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            nerecitempub_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                 method, url, params, keyword,
                                                                 'nerecitempub_0page', None)
        if nerecitempub_0page is None:
            return None
        else:
            self.result_li[keyword]['nerecitempub_0page'] = nerecitempub_0page.text
        nerecitempub_items = json.loads(nerecitempub_0page.text)
        # 开始翻页
        totalPage = nerecitempub_items['totalPage']
        perPage = nerecitempub_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'nerecitempub_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_liquidation(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            liquidation_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                method, url, params, keyword, 'liquidation_0page', None)
        if liquidation_0page is None:
            return None
        else:
            self.result_li[keyword]['liquidation_0page'] = liquidation_0page.text
        liquidation_items = json.loads(liquidation_0page.text)
        # 开始翻页
        totalPage = liquidation_items['totalPage']
        perPage = liquidation_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'liquidation_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_changerecord(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            changerecord_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                 method, url, params, keyword,
                                                                 'changerecord_0page', None)
        if changerecord_0page is None:
            return None
        else:
            self.result_li[keyword]['changerecord_0page'] = changerecord_0page.text
        changerecord_items = json.loads(changerecord_0page.text)
        # 开始翻页
        totalPage = changerecord_items['totalPage']
        perPage = changerecord_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'changerecord_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_mortreginfo(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            mortreginfo_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                method, url, params, keyword, 'mortreginfo_0page', None)
        if mortreginfo_0page is None:
            return None
        else:
            self.result_li[keyword]['mortreginfo_0page'] = mortreginfo_0page.text
        mortreginfo_items = json.loads(mortreginfo_0page.text)
        # 开始翻页
        totalPage = mortreginfo_items['totalPage']
        perPage = mortreginfo_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'mortreginfo_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_equitypledge(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            equitypledge_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                 method, url, params, keyword,
                                                                 'equitypledge_0page', None)
        if equitypledge_0page is None:
            return None
        else:
            self.result_li[keyword]['equitypledge_0page'] = equitypledge_0page.text
        equitypledge_items = json.loads(equitypledge_0page.text)
        # 开始翻页
        totalPage = equitypledge_items['totalPage']
        perPage = equitypledge_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'equitypledge_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_trademark(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            trademark_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                              method, url, params, keyword, 'trademark_0page', None)
        if trademark_0page is None:
            return None
        else:
            self.result_li[keyword]['trademark_0page'] = trademark_0page.text
        trademark_items = json.loads(trademark_0page.text)
        # 开始翻页
        totalPage = trademark_items['totalPage']
        perPage = trademark_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'trademark_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_checkinformation(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            checkinformation_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                     method, url, params, keyword,
                                                                     'checkinformation_0page', None)
        if checkinformation_0page is None:
            return None
        else:
            self.result_li[keyword]['checkinformation_0page'] = checkinformation_0page.text
        checkinformation_items = json.loads(checkinformation_0page.text)
        # 开始翻页
        totalPage = checkinformation_items['totalPage']
        perPage = checkinformation_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'checkinformation_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_drraninsres(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            drraninsres_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                method, url, params, keyword, 'drraninsres_0page', None)
        if drraninsres_0page is None:
            return None
        else:
            self.result_li[keyword]['drraninsres_0page'] = drraninsres_0page.text
        drraninsres_items = json.loads(drraninsres_0page.text)

        # 开始翻页
        totalPage = drraninsres_items['totalPage']
        perPage = drraninsres_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'drraninsres_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_administrativelicensing(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            administrativelicensing_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                            method, url, params, keyword,
                                                                            'administrativelicensing_0page', None)
        if administrativelicensing_0page is None:
            return None
        else:
            self.result_li[keyword]['administrativelicensing_0page'] = administrativelicensing_0page.text
        administrativelicensing_items = json.loads(administrativelicensing_0page.text)

        # 开始翻页
        totalPage = administrativelicensing_items['totalPage']
        perPage = administrativelicensing_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'administrativelicensing_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_administrativepenalty(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            administrativepenalty_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                          method, url, params, keyword,
                                                                          'administrativepenalty_0page', None)
        if administrativepenalty_0page is None:
            return None
        else:
            self.result_li[keyword]['administrativepenalty_0page'] = administrativepenalty_0page.text
        administrativepenalty_items = json.loads(administrativepenalty_0page.text)
        # 开始翻页
        totalPage = administrativepenalty_items['totalPage']
        perPage = administrativepenalty_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'administrativepenalty_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_exceptioninformation(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            exceptioninformation_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                         method, url, params, keyword,
                                                                         'exceptioninformation_0page', None)
        if exceptioninformation_0page is None:
            return None
        else:
            self.result_li[keyword]['exceptioninformation_0page'] = exceptioninformation_0page.text
        exceptioninformation_items = json.loads(exceptioninformation_0page.text)
        # 开始翻页
        totalPage = exceptioninformation_items['totalPage']
        perPage = exceptioninformation_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'administrativepenalty_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_judicialinformation(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            judicialinformation_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                        method, url, params, keyword,
                                                                        'judicialinformation_0page', None)
        if judicialinformation_0page is None:
            return None
        else:
            self.result_li[keyword]['judicialinformation_0page'] = judicialinformation_0page.text
        judicialinformation_items = json.loads(judicialinformation_0page.text)
        # 开始翻页
        totalPage = judicialinformation_items['totalPage']
        perPage = judicialinformation_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'judicialinformation_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_illegalinformation(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            illegalinformation_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                       method, url, params, keyword,
                                                                       'illegalinformation_0page', None)
        if illegalinformation_0page is None:
            return None
        else:
            self.result_li[keyword]['illegalinformation_0page'] = illegalinformation_0page.text
        illegalinformation_items = json.loads(illegalinformation_0page.text)
        # 开始翻页
        totalPage = illegalinformation_items['totalPage']
        perPage = illegalinformation_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'illegalinformation_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_administrativelicensing(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_administrativelicensing_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                                 method, url, params, keyword,
                                                                                 'self_administrativelicensing_0page',
                                                                                 None)
        if self_administrativelicensing_0page is None:
            return None
        else:
            self.result_li[keyword]['self_administrativelicensing_0page'] = self_administrativelicensing_0page.text
        self_administrativelicensing_items = json.loads(self_administrativelicensing_0page.text)
        # 开始翻页
        totalPage = self_administrativelicensing_items['totalPage']
        perPage = self_administrativelicensing_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_administrativelicensing_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_shareholder(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_shareholder_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                     method, url, params, keyword,
                                                                     'self_shareholder_0page', None)
        if self_shareholder_0page is None:
            return None
        else:
            self.result_li[keyword]['self_shareholder_0page'] = self_shareholder_0page.text
        self_shareholder_items = json.loads(self_shareholder_0page.text)
        # 开始翻页
        totalPage = self_shareholder_items['totalPage']
        perPage = self_shareholder_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_shareholder_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_insalterstockinfo(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_insalterstockinfo_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                           method, url, params, keyword,
                                                                           'self_insalterstockinfo_0page', None)
        if self_insalterstockinfo_0page is None:
            return None
        else:
            self.result_li[keyword]['self_insalterstockinfo_0page'] = self_insalterstockinfo_0page.text
        self_insalterstockinfo_items = json.loads(self_insalterstockinfo_0page.text)
        # 开始翻页
        totalPage = self_insalterstockinfo_items['totalPage']
        perPage = self_insalterstockinfo_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_insalterstockinfo_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_propledgereginfo(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_propledgereginfo_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                          method, url, params, keyword,
                                                                          'self_propledgereginfo_0page', None)
        if self_propledgereginfo_0page is None:
            return None
        else:
            self.result_li[keyword]['self_propledgereginfo_0page'] = self_propledgereginfo_0page.text
        self_propledgereginfo_items = json.loads(self_propledgereginfo_0page.text)
        # 开始翻页
        totalPage = self_propledgereginfo_items['totalPage']
        perPage = self_propledgereginfo_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_propledgereginfo_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_appsimplecancelobjection(self, keyword, search_item, info_page):
        params = {'nodeNum': search_item['nodeNum'],
                  'entType': search_item['entType']}
        method = 'POST'
        url = self.domain_uri(info_page['url'] % (search_item['pripid']))
        async with self.sem:
            self_appsimplecancelobjection_0page = await self.loop.run_in_executor(None, self.loopable_download, method,
                                                                                  url, params, keyword,
                                                                                  'self_appsimplecancelobjection_0page',
                                                                                  None)
        if self_appsimplecancelobjection_0page is None:
            return None
        else:
            self.result_li[keyword]['self_appsimplecancelobjection_0page'] = self_appsimplecancelobjection_0page.text

    async def get_self_epubgroupmenberinfo(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType'],
                       'pripId': search_item['pripid']
                       }
        method = 'POST'
        url = self.domain_uri(info_page['url'])
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_epubgroupmenberinfo_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                             method, url, params, keyword,
                                                                             'self_epubgroupmenberinfo_0page', None)
        if self_epubgroupmenberinfo_0page is None:
            return None
        else:
            self.result_li[keyword]['self_epubgroupmenberinfo_0page'] = self_epubgroupmenberinfo_0page.text
        self_epubgroupmenberinfo_items = json.loads(self_epubgroupmenberinfo_0page.text)
        # 开始翻页
        totalPage = self_epubgroupmenberinfo_items['totalPage']
        perPage = self_epubgroupmenberinfo_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_epubgroupmenberinfo_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    async def get_self_abolishmentlicenseinfo(self, keyword, search_item, info_page):
        base_params = {'nodeNum': search_item['nodeNum'],
                       'entType': search_item['entType'],
                       'pripId': search_item['pripid']
                       }
        method = 'POST'
        url = self.domain_uri(info_page['url'])
        params = base_params.copy()
        params['start'] = 0
        async with self.sem:
            self_abolishmentlicenseinfo_0page = await self.loop.run_in_executor(None, self.loopable_download,
                                                                                method, url, params, keyword,
                                                                                'self_abolishmentlicenseinfo_0page',
                                                                                None)
        if self_abolishmentlicenseinfo_0page is None:
            return None
        else:
            self.result_li[keyword]['self_abolishmentlicenseinfo_0page'] = self_abolishmentlicenseinfo_0page.text
        self_abolishmentlicenseinfo_items = json.loads(self_abolishmentlicenseinfo_0page.text)
        # 开始翻页
        totalPage = self_abolishmentlicenseinfo_items['totalPage']
        perPage = self_abolishmentlicenseinfo_items['perPage']
        for page in range(1, totalPage):
            inner_key_name = 'self_abolishmentlicenseinfo_{}page'.format(page)
            method = 'POST'
            params = base_params.copy()
            params['start'] = page * perPage
            self.result_li[keyword][inner_key_name] = False
            url = self.domain_uri(info_page['url'] % (search_item['pripid']))
            async with self.sem:
                innerpage = await self.loop.run_in_executor(None, self.loopable_download, method, url, params, keyword,
                                                            inner_key_name, None)
            if innerpage is None:
                return None
            else:
                self.result_li[keyword][inner_key_name] = innerpage.text

    def do_batch(self):
        while self.number < self.threshold:
            code = self.request_code()
            if isinstance(code, tuple):
                years = code[1]
                code = code[0]
                self.result_li[code] = {'search': False}
                task = self.loop.create_task(self.search(code, years))
                task.add_done_callback(partial(self.callback_result, keyword=code))
                self.number += 1
            elif code != 'null':
                self.result_li[code] = {'search': False}
                task = self.loop.create_task(self.search(code))
                task.add_done_callback(partial(self.callback_result, keyword=code))
                self.number += 1
        self.loop.run_forever()

    async def search(self, keyword, years=list(), page=0):
        logging.info('开始请求 ' + str(keyword) + str(time.time()))
        post_json = {
            "searchword": keyword,
            "conditions": {
                "excep_tab": "0",
                "ill_tab": "0",
                "area": "0",
                "cStatus": "0",
                "xzxk": "0",
                "xzcf": "0",
                "dydj": "0"
            }
        }
        url = self.domain_uri(self._search_page['url'])
        data = json.dumps(post_json, ensure_ascii=False).encode('utf-8')
        async with self.sem:
            result = await self.async_pages_download('POST', url, {'page': page}, keyword, 'search', data)
        if not result[2]:
            return
        else:
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
            for item in items['data']['result']['data']:
                entname = bs4.BeautifulSoup(item['entName'], 'lxml').text if item['entName'] else None
                historyname = bs4.BeautifulSoup(item['historyName'], 'lxml').text if item['historyName'] else None
                regno = bs4.BeautifulSoup(item['regNo'], 'lxml').text if item['regNo'] else None
                uniscid = bs4.BeautifulSoup(item['uniscId'], 'lxml').text if item['uniscId'] else None
                for match in [SBC2DBC(entname), SBC2DBC(historyname), SBC2DBC(regno), SBC2DBC(uniscid)]:
                    if not match:
                        continue
                    match_flag = False
                    for m in match.split('；'):
                        if SBC2DBC(keyword) == m:
                            match_flag = True
                            logging.info('success match ' + str(keyword))
                            break
                    if match_flag:
                        break
                else:
                    continue
                logging.info('create task ' + str(keyword))

                if 'annual' in self.detail_types:
                    self.result_li[keyword]['firstpage'] = False
                    task = self.loop.create_task(self.get_annual(keyword, item, self._self_ancheyear_page, years=years))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'baseinfo' in self.detail_types:
                    self.result_li[keyword]['base_info'] = False
                    task = self.loop.create_task(self.get_base_info(keyword, item, self._base_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'shareholder' in self.detail_types:
                    self.result_li[keyword]['sharehold_0page'] = False
                    task = self.loop.create_task(self.get_shareholder(keyword, item, self._shareholder_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'employe' in self.detail_types:
                    self.result_li[keyword]['keyPerson_0page'] = False
                    task = self.loop.create_task(self.get_keyPerson(keyword, item, self._employe_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'branch' in self.detail_types:
                    self.result_li[keyword]['branch_0page'] = False
                    task = self.loop.create_task(self.get_branch(keyword, item, self._branch_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'nerecitempub' in self.detail_types:
                    self.result_li[keyword]['nerecitempub_0page'] = False
                    task = self.loop.create_task(self.get_nerecitempub(keyword, item, self._nerecitempub_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'liquidation' in self.detail_types:
                    self.result_li[keyword]['liquidation_0page'] = False
                    task = self.loop.create_task(self.get_liquidation(keyword, item, self._liquidation_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'changerecord' in self.detail_types:
                    self.result_li[keyword]['changerecord_0page'] = False
                    task = self.loop.create_task(self.get_changerecord(keyword, item, self._changerecord_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'mortreginfo' in self.detail_types:
                    self.result_li[keyword]['mortreginfo_0page'] = False
                    task = self.loop.create_task(self.get_mortreginfo(keyword, item, self._mortreginfo_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'equitypledge' in self.detail_types:
                    self.result_li[keyword]['equitypledge_0page'] = False
                    task = self.loop.create_task(self.get_equitypledge(keyword, item, self._equitypledge_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'trademark' in self.detail_types:
                    self.result_li[keyword]['trademark_0page'] = False
                    task = self.loop.create_task(self.get_trademark(keyword, item, self._trademark_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'checkinformation' in self.detail_types:
                    self.result_li[keyword]['checkinformation_0page'] = False
                    task = self.loop.create_task(self.get_checkinformation(keyword, item, self._checkinformation_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'drraninsres' in self.detail_types:
                    self.result_li[keyword]['drraninsres_0page'] = False
                    task = self.loop.create_task(self.get_drraninsres(keyword, item, self._drraninsres_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'administrativelicensing' in self.detail_types:
                    self.result_li[keyword]['administrativelicensing_0page'] = False
                    task = self.loop.create_task(
                        self.get_administrativelicensing(keyword, item, self._administrativelicensing_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'administrativepenalty' in self.detail_types:
                    self.result_li[keyword]['administrativepenalty_0page'] = False
                    task = self.loop.create_task(
                        self.get_administrativepenalty(keyword, item, self._administrativepenalty_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'exceptioninformation' in self.detail_types:
                    self.result_li[keyword]['exceptioninformation_0page'] = False
                    task = self.loop.create_task(
                        self.get_exceptioninformation(keyword, item, self._exceptioninformation_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'judicialinformation' in self.detail_types:
                    self.result_li[keyword]['judicialinformation_0page'] = False
                    task = self.loop.create_task(
                        self.get_judicialinformation(keyword, item, self._judicialinformation_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'illegalinformation' in self.detail_types:
                    self.result_li[keyword]['illegalinformation_0page'] = False
                    task = self.loop.create_task(
                        self.get_illegalinformation(keyword, item, self._illegalinformation_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_administrativelicensing' in self.detail_types:
                    self.result_li[keyword]['self_administrativelicensing_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_administrativelicensing(keyword, item, self._self_administrativelicensing_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_shareholder' in self.detail_types:
                    self.result_li[keyword]['self_shareholder_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_shareholder(keyword, item, self._self_shareholder_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_insalterstockinfo' in self.detail_types:
                    self.result_li[keyword]['self_insalterstockinfo_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_insalterstockinfo(keyword, item, self._self_insalterstockinfo_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_propledgereginfo' in self.detail_types:
                    self.result_li[keyword]['self_propledgereginfo_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_propledgereginfo(keyword, item, self._self_propledgereginfo_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_appsimplecancelobjection' in self.detail_types:
                    self.result_li[keyword]['self_appsimplecancelobjection_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_appsimplecancelobjection(keyword, item, self._self_appsimplecancelobjection_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_epubgroupmenberinfo' in self.detail_types:
                    self.result_li[keyword]['self_epubgroupmenberinfo_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_epubgroupmenberinfo(keyword, item, self._self_epubgroupmenberinfo_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))

                if 'self_abolishmentlicenseinfo' in self.detail_types:
                    self.result_li[keyword]['self_abolishmentlicenseinfo_0page'] = False
                    task = self.loop.create_task(
                        self.get_self_abolishmentlicenseinfo(keyword, item, self._self_abolishmentlicenseinfo_page))
                    task.add_done_callback(partial(self.callback_result, keyword=keyword))