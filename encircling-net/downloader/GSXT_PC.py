#coding:utf-8
import re
import sys
import os
import time
import random
import asyncio


import requests
import execjs
import ujson
from bs4 import BeautifulSoup
import xmlrpc.client


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'downloader', ''))
sys.path.append(basedir)


from util.sbcdbc import SBC2DBC
from util.fake_ua import UserAgents
from util.get_keyword import getKeyword
from util.load_config import load_config


config = load_config(basedir.replace(os.sep + 'downloader', '') + os.sep +
                     os.sep.join(['configs', 'GSXT_PC_config.ini']))
RPCHOST = config['XMLRPC']['host']
RPCPORT = config['XMLRPC']['port']
RPCAPI = RPCHOST + ':' + RPCPORT

class JS_Launcher(object):

    def imageGif(self, resp):
        resp = """function imageGif(){
        var json=%s;
        return json.map(function(item){
            return String.fromCharCode(item);}).join('');}
        var result=imageGif();""" % (resp)
        ctx = execjs.compile(resp)
        return ctx.eval("result")


class Downloader(object):
    Cookies = {}
    PROXY_API = config['API']['PROXY_API']
    abu_proxies = {'all': config['DOWNLOADER']['ABU_PROXIES']}
    time_record = dict()
    url_key_json = {
    'tradeMarkDetailUrl': '',
    'punishmentInfoUrl': '',
    'eSelfAltInfoTotalUrl': '',
    'objectionWriteUrl': '',
    'toSimpleCancelInfoAndObjectionUrl': '',
    'updateSubModulesInfoUrl': '',
    'getEnliqdetailUrl': '',
    'eNliUpdateUrl': '',
    'insLicenceInfoNull': '',
    'insImKlpInfoNull': '',
    'keyPersonAllUrl': '',
    'punishmentDetailInfoUrl': '',
    'nCaseUrl': '',
    'nLicUrl': '',
    'entBusExcepUrl': '',
    'indBusExcepUrl': '',
    'argBusExcepUrl': '',
    'argBranchBusExcepUrl': '',
    'IllInfoUrl': '',
    'spotCheckInfoUrl': '',
    'judiciaryStockfreezePersonUrl': '',
    'judiciaryStockfreezeDetailUrl': '',
    'judiciaryAltershareholderUrl': '',
    'insInvinfoUrl': '',
    'insInvAlterStockinfoUrl': '',
    'insESelfAltInfoUrl': '',
    'insAlterstockinfoUrl': '',
    'insLicenceinfoUrl': '',
    'insProPledgeRegInfoUrl': '',
    'proPledgeRegInfoUrl': '',
    'trademarkInfoUrl': '',
    'insPunishmentinfoUrl': '',
    'shareholderUrl': '',
    'shareholderDetailUrl': '',
    'alterInfoUrl': '',
    'gtAlertInfoUrl': '',
    'keyPersonUrl': '',
    'gtKeyPersonUrl': '',
    'branchUrl': '',
    'branchUrlAll': '分支机构网页，非json',
    'liquidationUrl': '',
    'allTrademarkUrl': '商标注册信息，非json',
    'mortRegInfoUrl': '',
    'mortRegDetailInfoUrl': '',
    'stakQualitInfoUrl': '',
    'stakQualitDetailInfoUrl': '',
    'otherLicenceInfoUrl': '',
    'otherLicenceDetailInfoUrl': '',
    'assistUrl': '',
    'InsStockAlterModelUrl': '股东及出资信息修改记录网页版， 非jaon',
    'eSelfinfoaltUrl': '执行标准自我声明修改记录网页版, 非json',
    'shareHolderAll': '股东及出资信息网页版, 非json',
    'alterAllUrl': '变更信息网页版, 非json',
    'licenceAllUrl': '行政许可信息网页版， 非json',
    'branchAllUrl': '分支机构信息网页版, 非json',
    'punishmentAllUrl': '行政处罚信息网页版, 非json',
    'StakqualitAllUrl': '股权出质登记信息网页版, 非json',
    'MortregAllUrl': '动产抵押登记信息网页版, 非json',
    'simpleCancelUrl': '',
    'isLoginUrl': '',
    'checkUserInfoUrl': '',
    'addNewHistoryScanUrl': '',
    'showSubcribeUrl': '',
    'insertUserAttentionUrl': "a",
    'getAttentionEffectiveUrl': "a",
    'deleteAttentionByPripidUrl': '',
    'getDrRaninsResUrl': '',
    'getNeRecItemPubUrl': '',
    'getElicenseNullfyUrl': '',
    'getEPubGroupMenberInfoUrl': '',
    'getSimplecCancelInfoUrl': [],
    'eNliqUrl': '',
    'eproquacheckUrl': '',
    'eSelfinfoUrl': '',
    'getSusnateUrl': '',
    }
    annual_new_keys = ['insLicenceInfoNull', 'insImKlpInfoNull', 'tradeMarkDetailUrl', 'keyPersonAllUrl',
                       'punishmentInfoUrl', 'punishmentDetailInfoUrl', 'nCaseUrl', 'nLicUrl', 'entBusExcepUrl',
                       'indBusExcepUrl', 'argBusExcepUrl', 'argBranchBusExcepUrl', 'IllInfoUrl', 'spotCheckInfoUrl',
                       'judiciaryStockfreezePersonUrl', 'judiciaryStockfreezeDetailUrl', 'judiciaryAltershareholderUrl',
                       'insInvinfoUrl', 'insInvAlterStockinfoUrl', 'insESelfAltInfoUrl', 'eSelfAltInfoTotalUrl',
                       'insAlterstockinfoUrl', 'insLicenceinfoUrl', 'insProPledgeRegInfoUrl', 'proPledgeRegInfoUrl',
                       'trademarkInfoUrl', 'insPunishmentinfoUrl', 'shareholderUrl', 'shareholderDetailUrl',
                       'alterInfoUrl', 'gtAlertInfoUrl', 'keyPersonUrl', 'gtKeyPersonUrl', 'branchUrl', 'branchUrlAll',
                       'liquidationUrl', 'allTrademarkUrl', 'mortRegInfoUrl', 'mortRegDetailInfoUrl',
                       'stakQualitInfoUrl', 'stakQualitDetailInfoUrl', 'otherLicenceInfoUrl',
                       'otherLicenceDetailInfoUrl', 'assistUrl', 'InsStockAlterModelUrl', 'eSelfinfoaltUrl',
                       'shareHolderAll', 'alterAllUrl', 'licenceAllUrl', 'branchAllUrl', 'punishmentAllUrl',
                       'StakqualitAllUrl', 'MortregAllUrl', 'toSimpleCancelInfoAndObjectionUrl',  'simpleCancelUrl',
                       'objectionWriteUrl', 'addNewHistoryScanUrl', 'showSubcribeUrl', 'updateSubModulesInfoUrl',
                       'insertUserAttentionUrl', 'getAttentionEffectiveUrl', 'deleteAttentionByPripidUrl',
                       'getDrRaninsResUrl', 'getNeRecItemPubUrl', 'getElicenseNullfyUrl', 'getEPubGroupMenberInfoUrl',
                       'getSimplecCancelInfoUrl', 'getEnliqdetailUrl', 'eNliqUrl', 'eNliUpdateUrl', 'eproquacheckUrl',
                       'eSelfinfoUrl', 'getSusnateUrl']

    def __init__(self):
        self.Headers = {
            'Host': 'www.gsxt.gov.cn',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'
        }
        self.loop = asyncio.get_event_loop()
        config = load_config(basedir.replace(os.sep + 'downloader', '') + os.sep + os.sep.join(['configs', 'GSXT_PC_config.ini']))
        self.need = dict()
        for key in config['NEED']:
            self.need[key] = config.getboolean('NEED', key)
        self.JSl = JS_Launcher()
        self.token = None
        self.Just_Annual = config.getboolean('DOWNLOADER', 'Just_Annual')

    def change_ip(self, proxies):
        resp = requests.get('http://proxy.abuyun.com/switch-ip', proxies=proxies)

    def get_image_gif(self, cookies, proxies):
        localTime = time.localtime(time.time())
        url = "http://www.gsxt.gov.cn/corp-query-custom-geetest-image.gif?v=" + str(localTime.tm_min + localTime.tm_sec)
        headers = self.Headers.copy()
        headers['Referer'] = 'http://www.gsxt.gov.cn/index.html'
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        aaa = self.JSl.imageGif(resp.text)
        matchObj = re.search('location_info = (\d+);', aaa)
        if matchObj:
            return matchObj.group(1)
        else:
            Exception("Not found location_info")

    def set_validate_input(self, cookies, proxies, location_info):
        url = 'http://www.gsxt.gov.cn/corp-query-geetest-validate-input.html?token=' + location_info
        headers = self.Headers.copy()
        headers['Referer'] = 'http://www.gsxt.gov.cn/index.html'
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        aaa = self.JSl.imageGif(resp.text)
        matchObj=re.search('value: (\d+)}', aaa)
        if matchObj:
            location_info = matchObj.group(1)
            token = int(location_info) ^ 536870911
            self.token = str(token)
            return True
        else:
            Exception("Not found location_info")

    def loopable_download(self, year, key, url, proxies, page, retry=20):
        while retry:
            try:
                headers = self.Headers.copy()
                headers['User-Agent'] = random.choice(UserAgents)
                cookies = self.build_cookie_from_js(url, headers, proxies)
                headers['Referer'] = url
                resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
                resp_json = resp.json()
                resp_json_data = resp_json['data']
                if resp_json_data == None:
                    resp_json_data = []
                if not page:
                    self.result_json[year][key] = resp_json_data
                else:
                    self.result_json[year][key] += resp_json_data
                return resp_json
            except Exception as err:
                retry -= 1
        if not page:
            self.result_json[year][key] = 'FAIL'
        else:
            self.result_json[year][key] += ['Miss Page' + str(page)]

    def judge_success(self):
        tag = True
        for year in self.result_json:
            if year != 'keyword':
                for key in self.result_json[year]:
                    if self.result_json[year][key] == 'FAIL' or 'Miss' in self.result_json[year][key]:
                        tag = False
                        return tag
                    for each in self.result_json[year][key]:
                        if 'Miss' in each:
                            tag = False
                            return tag
        return tag

    def build_cookie_from_js(self, url, headers, proxies):
        cookies = {}
        resp_index = requests.get(url, headers=headers, proxies=proxies)
        text = resp_index.text
        js = re.search(r'<script>(.*?)</script>', text).group(1)
        js = re.compile(r'(while\(z\+\+\).*?catch\(_\){})').sub('', js)
        js = js + r'''show = function(){var code = '';while (z++) try''' + \
        r''' {code = y.replace(/\b\w+\b/g, function(y) {return x[f(y, z) - 1] || ("_" + y)});''' + \
        r'''if (/^var.*?=function/.test(code)){code = code.replace(/\\/g,"\\\\");break}} catch(_) {}return code};'''
        ctx = execjs.compile(js)
        result = ctx.call('show')
        result = re.compile(r'(setTimeout.*?\);)').sub('', result)
        result = re.compile(r'(if\(\(function\(\).*?$)').sub('', result).strip()
        s = 'var a' + result.split('document.cookie')[1].split("Path=/;'")[0] + "Path=/;';return a;"
        s = re.sub(r'document.create.*?firstChild.href', '"{}"'.format('http://www.gsxt.gov.cn/index.html'), s)
        resHtml = "function getnewClearance(){" + s + "};"
        resHtml = resHtml.replace('\\\\', '\\')
        resHtml = resHtml.replace('window.headless', 'undefined')
        resHtml = re.sub(r'window\[.*?\]', 'undefined', resHtml)
        ctx = execjs.compile(resHtml)
        jsl_clearance = ctx.call('getnewClearance')
        jsl_clearance = jsl_clearance.split(';')[0]
        jsl_clearance = jsl_clearance.split('=')[1]
        __jsluid_h = resp_index.cookies['__jsluid_h']
        cookies['__jsluid_h'] = __jsluid_h
        cookies['__jsl_clearance'] = jsl_clearance
        return cookies

    def get_cookie(self, headers, proxies):
        url = 'http://www.gsxt.gov.cn'
        cookies = self.build_cookie_from_js(url, headers, proxies)
        headers = self.Headers.copy()
        headers['Referer'] = 'http://www.gsxt.gov.cn/'
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        SECTOKEN = re.search(r'SECTOKEN=(.*?);', resp.headers['Set-Cookie']).group(1)
        JSESSIONID = re.search(r'JSESSIONID=(.*?);', resp.headers['Set-Cookie']).group(1)
        tlb_cookie = re.search(r'tlb_cookie=(.*?);', resp.headers['Set-Cookie']).group(1)
        cookies['SECTOKEN'] = SECTOKEN
        cookies['JSESSIONID'] = JSESSIONID
        cookies['tlb_cookie'] = tlb_cookie
        return cookies, headers

    def get_gt(self, headers, cookies, proxies):
        url = 'http://www.gsxt.gov.cn/SearchItemCaptcha?t=' + str(int(time.time() * 1000))
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        JSESSIONID = re.search(r'JSESSIONID=(.*?);', resp.headers['Set-Cookie']).group(1)
        cookies['JSESSIONID'] = JSESSIONID
        return cookies, resp.text

    def require_yanzhengma(self, gt, challenge):
        client = xmlrpc.client.ServerProxy(RPCAPI)
        resp_text = client.Geetest(gt, challenge)
        if '识别失败' == resp_text['msg']:
            raise Exception
        validate = resp_text['data']['validate']
        challenge = resp_text['data']['challenge']
        return validate, challenge

    def post_req(self, challenge, validate, headers, cookies, keyword, proxies):
        data = {
            'tab': 'ent_tab',
            'province': '',
            'geetest_challenge': challenge,
            'geetest_validate': validate,
            'geetest_seccode': validate + '|jordan',
            'token': self.token,
            'searchword': keyword
        }
        url = 'http://www.gsxt.gov.cn/corp-query-search-1.html'
        resp = requests.post(url, data=data, headers=headers, cookies=cookies, proxies=proxies)
        return resp.text

    def update_cookie(self, headers, cookies, proxies):
        url = 'http://www.gsxt.gov.cn/SearchItemCaptcha?t=' + str(int(time.time() * 1000))
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        if 'Set-Cookie' in resp.headers.keys():
            JSESSIONID = re.search(r'JSESSIONID=(.*?);', resp.headers['Set-Cookie']).group(1)
            cookies['JSESSIONID'] = JSESSIONID
        return cookies

    def get_url(self, url, headers, cookies, proxies):
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        url = 'http://www.gsxt.gov.cn' + re.search(r'var anCheYearInfo = "(.*?)";', resp.text).group(1)
        detail_url = 'http://www.gsxt.gov.cn' + re.search(r'var annRepDetailUrl = "(.*?)";', resp.text).group(1)
        gsxtBrowseHistory1 = re.search(r'gsxtBrowseHistory1=(.*?);', resp.headers['Set-Cookie']).group(1)
        cookies['gsxtBrowseHistory1'] = gsxtBrowseHistory1
        new_urls = {}
        for key in self.url_key_json:
            new_url = 'http://www.gsxt.gov.cn' + re.search(r'var {}\s*=\s*"(.*?)";*'.format(key), resp.text).group(1)
            new_urls[key] = new_url
        return cookies, url, detail_url, new_urls

    def special_get_url(self, url, headers, cookies, proxies):
        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        url = 'http://www.gsxt.gov.cn' + re.search(r'var anCheYearInfo = "(.*?)";', resp.text).group(1)
        detail_url = 'http://www.gsxt.gov.cn' + re.search(r'var annRepDetailUrl = "(.*?)";', resp.text).group(1)
        gsxtBrowseHistory1 = re.search(r'gsxtBrowseHistory1=(.*?);', resp.headers['Set-Cookie']).group(1)
        cookies['gsxtBrowseHistory1'] = gsxtBrowseHistory1
        return cookies, url, detail_url

    def anCheIds(self, url, detail_url, headers, cookies, proxies, retry=20):
        while retry:
            try:
                resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
                if re.match(r'<script>var', resp.text):
                    raise Exception
                if resp.text == '[]':
                    return []
                elif resp.json():
                    s = detail_url + '?anCheId={}&entType=10&anCheYear={}'
                    urls = []
                    for i in resp.json():
                        url = s.format(i['anCheId'], i['anCheYear'])
                        urls.append((url, i['anCheId'], i['anCheYear']))
                    return urls
                else:
                    retry -= 1
            except:
                retry -= 1
        return []

    def get_nesting_url(self, url, proxies):
        nested_dict = dict()
        headers = self.Headers.copy()
        headers['User-Agent'] = random.choice(UserAgents)
        cookies = self.build_cookie_from_js(url, headers, proxies)
        headers['Referer'] = url
        outside_resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        keys = ['allAlterInfoUrl', 'allShareHolderDetailInfoUrl', 'allPunishmentInfoUrl', 'allOtherLicenceInfoUrl',
                'allMortRegInfoUrl', 'allStakQualitInfoUrl', 'allGtAlterInfoUrl', 'branchUrl', 'branchUrlData',
                'tradeMarkUrlData', 'tradeMarkDetailUrl', 'keyPersonUrlData', 'gtKeyPersonUrlData',
                'shareholderDetailUrl', 'otherLicenceDetailInfoUrl', 'punishmentDetailInfoUrl',
                'stakQualitDetailInfoUrl', 'insInvAlterStockinfoUrl', 'insESelfAltInfoUrl', 'nCaseAltUrl', 'nLicAltUrl']
        explain = ['全部变更信息', '全部 股东及出资信息', '全部行政处罚信息', '全部行政许可信息', '全部动产抵押信息',
                   '全部 股权出质登记信息', '全部 个体工商户    变更信息', '分支机构信息', '分支机构信息获取数据',
                   '商标信息获取数据', '商标信息详情', '主要人员信息获取数据', '个体主要人员信息获取数据',
                   '基本信息中股东详情信息', '行政许可信息详情', '行政处罚信息详情', '股权出质登记信息详情',
                   '股东及出资信息 中的股权变更信息', '执行标准自我声明修改信息', '2019行政处罚变更信息',
                   '2019行政许可变更信息']

        for key in keys:
            retry = 20
            while retry:
                try:
                    new_url = 'http://www.gsxt.gov.cn' + \
                              re.search(r'var {}\s*=\s*"(.*?)";*'.format(key), outside_resp.text).group(1)
                    headers = self.Headers.copy()
                    headers['User-Agent'] = random.choice(UserAgents)
                    cookies = self.build_cookie_from_js(url, headers, proxies)
                    headers['Referer'] = url
                    inner_resp = requests.get(new_url, headers=headers, cookies=cookies, proxies=proxies)
                    if re.match(r'<script>var', inner_resp.text):
                        raise Exception
                    nested_dict[str((key, new_url))] = inner_resp.text
                    break
                except:
                    retry -= 1
        return nested_dict

    def get_new_urls(self, new_urls, headers, cookies, proxies):
        new_urls_result = dict()
        nest_tag = True
        for key, url in new_urls.items():
            retry = 20
            if key not in ['MortregAllUrl', 'StakqualitAllUrl', 'punishmentAllUrl', 'branchAllUrl', 'licenceAllUrl',
                           'alterAllUrl', 'shareHolderAll', 'eSelfinfoaltUrl', 'InsStockAlterModelUrl',
                           'allTrademarkUrl', 'branchUrlAll', 'keyPersonAllUrl']:
                while retry:
                    try:
                        headers = self.Headers.copy()
                        headers['User-Agent'] = random.choice(UserAgents)
                        cookies = self.build_cookie_from_js(url, headers, proxies)
                        headers['Referer'] = url
                        resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
                        if re.match(r'<script>var', resp.text):
                            raise Exception
                        elif resp.status_code == 404:
                            new_urls_result[str((key, url))] = 404
                        elif '您访问的页面不存在' in resp.text:
                            new_urls_result[str((key, url))] = '您访问的页面不存在'
                        elif resp.status_code == 503:
                            new_urls_result[str((key, url))] = 503
                        else:
                            new_urls_result[str((key, url))] = resp.text
                        break
                    except:
                        retry -= 1
            elif nest_tag:
                nested_dict = self.get_nesting_url(url, proxies=proxies)
                nest_tag = False
                new_urls_result['nesting'] = nested_dict
        return new_urls_result

    async def request_key(self, tu, proxies, result, key):
        url = 'http://www.gsxt.gov.cn' + result[key]
        resp_json = await self.loop.run_in_executor(None, self.loopable_download, str(tu[2])\
                                                    + ' ' + str(tu[1]), key, url, proxies, 0)
        if resp_json:
            totalpage = resp_json['totalPage']
            draw = 1
            if totalpage > 1:
                for i in range(2, totalpage + 1):
                    draw += 1
                    start = 5 * (i - 1)
                    url = 'http://www.gsxt.gov.cn' + result[key] + '?entType=10&draw={}&start={}&length=5&_={}'\
                        .format(draw, start, int(time.time() * 1000))
                    resp_json = await self.loop.run_in_executor(None, self.loopable_download, str(tu[2])\
                                                                + ' ' + str(tu[1]), key, url, proxies, i)
                    if not resp_json:
                        continue

    def every_annual(self, tu, proxies):
        self.result_json[str(tu[2]) + ' ' + str(tu[1])] = {}
        result = {
            "allWebInfoModelUrl": "/corp-query-entprise-allInfo-webAllInfo-{}.html",
            "ann_socsecinfo_url": "/corp-query-entprise-info-AnnSocsecinfo-{}.html",
            "allForinvestmentInfoUrl": "/corp-query-entprise-info-allForinvestmentInfo-{}.html",
            "allWebInfoUrl": "/corp-query-entprise-info-allWebInfo-{}.html",
            "allForgModelUrl": "/corp-query-entprise-allInfo-forgruAllInfo-{}.html",
            "forGuaranteeinfoOrNoUrl": "/corp-query-entprise-info-forGuaranteeinfoOrNo-{}.html",
            "for_guaranteeinfo_url": "/corp-query-entprise-info-forGuaranteeinfo-{}.html",
            "alter_url": "/corp-query-entprise-info-annualAlter-{}.html",
            "web_site_info_url": "/corp-query-entprise-info-webSiteInfo-{}.html",
            "vAnnualReportBranchProductionUrl": "/corp-query-entprise-info-vAnnualReportBranchProduction-{}.html",
            "for_investment_url": "/corp-query-entprise-info-forInvestment-{}.html",
            "alterTotalPageUrl": "/corp-query-entprise-info-AlterTotalPage-{}.html",
            "sponsor_url": "/corp-query-entprise-info-sponsor-{}.html",
            "baseinfo_url": "/corp-query-entprise-info-baseinfo-{}.html",
            "v_annual_report_alterstockinfo_url": "/corp-query-entprise-info-vAnnualReportAlterstockinfo-{}.html"}
        for key in result:
            result[key] = result[key].format(tu[1])
        tasks = []
        for key in result:
            if key in ['baseinfo_url', 'ann_socsecinfo_url', 'v_annual_report_alterstockinfo_url', 'sponsor_url',
                       'for_guaranteeinfo_url', 'alter_url', 'web_site_info_url', 'for_investment_url'] and \
                    self.need[key]:
                task = self.loop.create_task(self.request_key(tu, proxies, result, key))
                tasks.append(task)
        self.loop.run_until_complete(asyncio.wait(tasks))
        return True

    def get_new_urls_in_annuals(self, tuples, proxies, retry=20):
        new_annual_url_dict = dict()
        url, id, year = tuples[0]
        while retry:
            try:
                headers = self.Headers.copy()
                headers['User-Agent'] = random.choice(UserAgents)
                cookies = self.build_cookie_from_js(url, headers, proxies)
                headers['Referer'] = url
                resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
                if re.match(r'<script>var', resp.text):
                    raise Exception
                for key in self.annual_new_keys:
                    new_annual_url = 'http://www.gsxt.gov.cn' + re.search(r'var {}\s*=\s*"(.*?)";*'.format(key),
                                                                          resp.text).group(1)
                    new_annual_url_dict[key] = new_annual_url
                return new_annual_url_dict
            except:
                retry -= 1
        return new_annual_url_dict

    def get_data_from_new_annual_urls(self, url_dict, proxies):
        nested_dict = dict()
        for key in url_dict:
            url = url_dict[key]
            retry = 20
            while retry:
                try:
                    headers = self.Headers.copy()
                    headers['User-Agent'] = random.choice(UserAgents)
                    cookies = self.build_cookie_from_js(url, headers, proxies)
                    headers['Referer'] = url
                    inner_resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
                    if re.match(r'<script>var', inner_resp.text):
                        raise Exception
                    elif inner_resp.status_code == 404:
                        nested_dict[str((key, url))] = 404
                    elif '您访问的页面不存在' in inner_resp.text:
                        nested_dict[str((key, url))] = '您访问的页面不存在'
                    elif '请输入企业名称、统一社会信用代码或注册号' in inner_resp.text:
                        nested_dict[str((key, url))] = '请输入企业名称、统一社会信用代码或注册号'
                    elif inner_resp.status_code == 503:
                        nested_dict[str((key, url))] = 503
                    elif inner_resp.status_code == 521:
                        nested_dict[str((key, url))] = 521
                    else:
                        nested_dict[str((key, url))] = inner_resp.text
                    break
                except:
                    retry -= 1
        return nested_dict

    def inner_run(self, keyword,  retry_times=3):
        self.keyword = keyword
        self.result_json = {'keyword': self.keyword}
        tag = False
        while not tag and retry_times:
            requests.get('http://proxy.abuyun.com/switch-ip', proxies=self.abu_proxies)
            retry_times -= 1
            keyword = self.keyword
            headers = self.Headers.copy()
            proxies = self.abu_proxies
            try:
                cookies, headers = self.get_cookie(headers, proxies)
                old_cookies = cookies.copy()
                cookies, text = self.get_gt(headers, cookies, proxies)
                location_info = self.get_image_gif(old_cookies, proxies)
                challenge = ujson.loads(text)['challenge']
                gt = ujson.loads(text)['gt']
                validate, new_challenge = self.require_yanzhengma(gt, challenge)
                if new_challenge:
                    challenge = new_challenge
                self.set_validate_input(cookies, proxies, location_info)
                headers = self.Headers.copy()
                headers['Referer'] = 'http://www.gsxt.gov.cn/index.html'
                headers['Origin'] = 'http://www.gsxt.gov.cn'
                text = self.post_req(challenge, validate, headers, cookies, keyword, proxies)
                soup = BeautifulSoup(text, 'lxml')
                div = soup.find('div', attrs={'class': 'search_result'})
                number = int(div.span.text)
                if number > 0:
                    a_li = soup.find_all('a', attrs={'class': 'search_list_item'})
                    find_flag = False
                    for item in a_li:
                        uniscid = ''
                        entname = item.h1.text.strip()
                        cop = re.compile("[^\u4e00-\u9fa5^a-z^A-Z^0-9]")
                        # entname = cop.sub('_', entname)
                        try:
                            temp_uniscid = item.find('div', attrs={'class': 'div-map2'})
                            uniscid = temp_uniscid.text.split('：')[1].strip()
                        except:
                            pass
                        for match in [SBC2DBC(uniscid), SBC2DBC(entname)]:
                            if not match:
                                continue
                            match_flag = False
                            for m in match.split('；'):
                                if SBC2DBC(keyword).strip() == m.strip():
                                    match_flag = True
                                    find_flag = True
                                    break
                            if match_flag:
                                break
                        else:
                            continue
                        headers = self.Headers.copy()
                        headers['Referer'] = 'http://www.gsxt.gov.cn/corp-query-search-1.html'
                        cookies = self.update_cookie(headers, cookies, proxies)
                        href = 'http://www.gsxt.gov.cn' + item['href']
                        headers = self.Headers.copy()
                        cookies, url, detail_url, new_urls = self.get_url(href, headers, cookies, proxies)
                        headers = self.Headers.copy()
                        headers['Referer'] = href
                        cookies = self.update_cookie(headers, cookies, proxies)
                        if not self.Just_Annual:
                            new_urls_result = self.get_new_urls(new_urls, headers, cookies, proxies)
                            self.result_json['new_urls_result'] = new_urls_result
                        tuples = self.anCheIds(url, detail_url, headers, cookies, proxies)
                        if not tuples:
                            tag = True
                            return None
                        elif tuples:
                            if not self.Just_Annual:
                                new_annual_url_dict = self.get_new_urls_in_annuals(tuples, proxies)
                                self.result_json['new_annual_urls_result'] = self.get_data_from_new_annual_urls(
                                    new_annual_url_dict, proxies)
                            for tu in tuples:
                                inner_retry_times = 3
                                while inner_retry_times:
                                    inner_retry_times -= 1
                                    self.result_json[str(tu[2]) + ' ' + str(tu[1])] = ''
                                    try:
                                        inner_tag = self.every_annual(tu, proxies)
                                        if inner_tag:
                                            break
                                    except Exception as e:
                                        pass
                        tag = True
                        break
                    if not find_flag:
                        return None
                else:
                    return None
            except Exception as e:
                if not retry_times:
                    return list()
        success = self.judge_success()
        if success:
            return self.result_json
        else:
            return list()

    def run(self, firm):
        if firm in Downloader.time_record:
            last_time = Downloader.time_record[firm]
            duration = time.time() - last_time
            if duration < 60*60*3:
                retry_times = 3
            elif duration > 60*60*24:
                Downloader.time_record[firm] = time.time()
                retry_times = 3
            elif duration < 60*60*6:
                retry_times = 2
            else:
                retry_times = 1
        else:
            Downloader.time_record[firm] = time.time()
            retry_times = 3
        self.tu = getKeyword(firm)
        result = None
        for keyword in self.tu:
            if not keyword:
                continue
            result = self.inner_run(keyword, retry_times)
            if not isinstance(result, list):
                return result
        return result





