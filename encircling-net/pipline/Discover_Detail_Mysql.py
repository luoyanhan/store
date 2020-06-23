# coding=utf-8
import sys
import os
import time
import queue
import threading
import multiprocessing
import multiprocessing.pool

import redis
import ujson as json
import pymongo
import logging
import sqlalchemy.orm.session


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'pipline', ''))
sys.path.append(basedir.replace(os.sep + 'pipline', os.sep + 'parser'))
sys.path.append(basedir)


from util.load_config import load_config
from detail_parser import GSXTParser


from manage import app_shell
app_shell('dev')

from app.main.blueprint.production.model.production import Baseinfo, Reginfo, Shareholder, ShareholderDetail, Branch, \
    Liquidation, LiquidationGroup, Changerecord, AdministrativeLicensing, AdministrativePenalty, ExceptionInformation, \
    IllegalInformation, CheckInformation, JudicialInformation, ChattelMortgage, EquityPledge, EmployeesGSXT, Usednames
from app.main.blueprint.production.service import keynomapping as KeynoMappingService
from app.main.blueprint.production.service import annual_che_year as service_annual_che_year
from app.main.blueprint.production.service import resource as ResourceService


Baseinfo.__mark_resource__ = False
Baseinfo.__mark_difference__ = False
Reginfo.__mark_resource__ = False
Reginfo.__mark_difference__ = False
Shareholder.__mark_resource__ = False
Shareholder.__mark_difference__ = False
ShareholderDetail.__mark_resource__ = False
ShareholderDetail.__mark_difference__ = False
Branch.__mark_resource__ = False
Branch.__mark_difference__ = False
Liquidation.__mark_resource__ = False
Liquidation.__mark_difference__ = False
LiquidationGroup.__mark_resource__ = False
LiquidationGroup.__mark_difference__ = False
Changerecord.__mark_resource__ = False
Changerecord.__mark_difference__ = False
AdministrativeLicensing.__mark_resource__ = False
AdministrativeLicensing.__mark_difference__ = False
AdministrativePenalty.__mark_resource__ = False
AdministrativePenalty.__mark_difference__ = False
ExceptionInformation.__mark_resource__ = False
ExceptionInformation.__mark_difference__ = False
IllegalInformation.__mark_resource__ = False
IllegalInformation.__mark_difference__ = False
CheckInformation.__mark_resource__ = False
CheckInformation.__mark_difference__ = False
JudicialInformation.__mark_resource__ = False
JudicialInformation.__mark_difference__ = False
ChattelMortgage.__mark_resource__ = False
ChattelMortgage.__mark_difference__ = False
EquityPledge.__mark_resource__ = False
EquityPledge.__mark_difference__ = False
EmployeesGSXT.__mark_resource__ = False
EmployeesGSXT.__mark_difference__ = False
Usednames.__mark_resource__ = False
Usednames.__mark_difference__ = False
service_annual_che_year.AnnualCheYear.__mark_resource__ = False
service_annual_che_year.AnnualCheYear.__mark_difference__ = False
service_annual_che_year.AnnualCheYearResult.__mark_resource__ = False
service_annual_che_year.AnnualCheYearResult.__mark_difference__ = False
service_annual_che_year.AnnualReportDataAlterStock.__mark_resource__ = False
service_annual_che_year.AnnualReportDataAlterStock.__mark_difference__ = False
service_annual_che_year.AnnualReportDataInvestment.__mark_resource__ = False
service_annual_che_year.AnnualReportDataInvestment.__mark_difference__ = False
service_annual_che_year.AnnualReportDataWebsite.__mark_resource__ = False
service_annual_che_year.AnnualReportDataWebsite.__mark_difference__ = False
service_annual_che_year.AnnualReportDataSocsecInfo.__mark_resource__ = False
service_annual_che_year.AnnualReportDataSocsecInfo.__mark_difference__ = False
service_annual_che_year.AnnualReportDataAlt.__mark_resource__ = False
service_annual_che_year.AnnualReportDataAlt.__mark_difference__ = False
service_annual_che_year.AnnualReportDataSponsor.__mark_resource__ = False
service_annual_che_year.AnnualReportDataSponsor.__mark_difference__ = False
service_annual_che_year.AnnualReportDataGuaranteeinfo.__mark_resource__ = False
service_annual_che_year.AnnualReportDataGuaranteeinfo.__mark_difference__ = False
service_annual_che_year.AnnualReportData.__mark_resource__ = False
service_annual_che_year.AnnualReportData.__mark_difference__ = False


class UpsertAPIPipline(object):
    def __init__(self):
        self.session = sqlalchemy.orm.session.Session(bind=Baseinfo.__engine__[Baseinfo._node])

    def handler(self, input_dict, keyno, only_insert):
        if input_dict['Baseinfo']:
            if input_dict.get('Shareholder'):
                shareholders = input_dict['Shareholder']['shareholders']
                if shareholders:
                    shareholderbase = [i['capital_id_item'] for i in shareholders if i['capital_id_item']]
                    shareholderdetail = []
                    for i in shareholders:
                        if i.get('additions') and i['additions']:
                            shareholderdetail.append(i['additions'])
                        else:
                            shareholderdetail.append(None)
                    shareholder_results = Shareholder.upsert_list_by_keyno(keyno, datas=shareholderbase,
                                                                           session=self.session,
                                                                           only_insert=only_insert)
                    shareholderdetail_datas = list()
                    for j in range(len(shareholder_results)):
                        capital_id = shareholder_results[j].capital_id
                        if shareholderdetail[j]:
                            for detail_data in shareholderdetail[j]:
                                detail_data['capital_id'] = capital_id
                                shareholderdetail_datas.append(detail_data)
                    shareholderdetail_datas_foreign = shareholderdetail_datas[0]['capital_id']\
                    if shareholderdetail_datas else None
                    ShareholderDetail.upsert_list_by_datas(shareholderdetail_datas, session=self.session,
                                                           only_insert=only_insert,
                                                           foreign=shareholderdetail_datas_foreign)
            branch_information = input_dict['BranchInformation']
            branch_firm = []
            for each in branch_information:
                additions = each['additions']
                li = [one['name'] for one in additions]
                branch_firm.extend(li)
            if branch_firm:
                baseinfo_results = []
                branch_upsert_list = []
                exists_firms = KeynoMappingService.exists_dict_by_firms_keynos(branch_firm)
                branch_upsert_list.extend([{'branchkeyno': v}
                    for v in exists_firms.values()])
                for each in branch_firm:
                    if each not in exists_firms:
                        data = {
                            "firm": each,
                            "creditcode": "",
                            "reg_no_type": "",
                            "reg_no": "",
                            "regdate": "",
                            "collect_time": ""
                        }
                        baseinfo_results.append(data)
                baseinfo_results = Baseinfo.batch_insert(baseinfo_results, session=self.session)
                branch_upsert_list.extend([{'branchkeyno': keyno} for keyno in baseinfo_results])
                Branch.upsert_list_by_keyno(keyno, branch_upsert_list, session=self.session, only_insert=only_insert)
            if input_dict['Liquidation']:
                liquidation = input_dict['Liquidation']['liquidations']
                if liquidation:
                    liquidation_dict = liquidation[0]
                    memebers = liquidation_dict['additions']
                    result = Liquidation.upsert_list_by_keyno(keyno, datas=[liquidation_dict['base']],
                                                              session=self.session, only_insert=only_insert)
                    liquidation_id = result[0].liquidation_id
                    LiquidationGroup.upsert_list_by_liquidation_id(liquidation_id, memebers, session=self.session,
                                                                   only_insert=only_insert)
            if input_dict['Changerecord']:
                changerecords = input_dict['Changerecord']['changerecords']
                if changerecords:
                    Changerecord.upsert_list_by_keyno(keyno, changerecords, session=self.session,
                                                      only_insert=only_insert)
            administrativeLicensing = input_dict['AdministrativeLicensing']
            if administrativeLicensing:
                AdministrativeLicensing.upsert_list_by_keyno(keyno, administrativeLicensing, session=self.session,
                                                             only_insert=only_insert)
            administrativePenalty = input_dict['AdministrativePenalty']
            if administrativePenalty:
                AdministrativePenalty.upsert_list_by_keyno(keyno, administrativePenalty, session=self.session,
                                                           only_insert=only_insert)
            exceptionInformation = input_dict['ExceptionInformation']
            if exceptionInformation:
                ExceptionInformation.upsert_list_by_keyno(keyno, exceptionInformation, session=self.session,
                                                          only_insert=only_insert)
            illegalInformation = input_dict['IllegalInformation']
            if illegalInformation:
                IllegalInformation.upsert_list_by_keyno(keyno, illegalInformation, session=self.session,
                                                        only_insert=only_insert)
            checkInformation = input_dict['CheckInformation']
            if checkInformation:
                CheckInformation.upsert_list_by_keyno(keyno, checkInformation, session=self.session,
                                                      only_insert=only_insert)
            judicialInformation = input_dict['JudicialInformation']
            if judicialInformation:
                JudicialInformation.upsert_list_by_keyno(keyno, judicialInformation, session=self.session,
                                                         only_insert=only_insert)
            chattelMortgage = input_dict['ChattelMortgage']
            if chattelMortgage:
                ChattelMortgage.upsert_list_by_keyno(keyno, chattelMortgage, session=self.session,
                                                     only_insert=only_insert)
            equityPledge = input_dict['EquityPledge']
            if equityPledge:
                EquityPledge.upsert_list_by_keyno(keyno, equityPledge, session=self.session, only_insert=only_insert)
            if input_dict['MainPersonnel']:
                employees = input_dict['MainPersonnel'][0]['employees']
                if employees:
                    result = EmployeesGSXT.upsert_list_by_keyno(keyno, employees, session=self.session,
                                                                only_insert=only_insert)
            if input_dict['SearchInfo']:
                usednames = input_dict['SearchInfo'][0]['usednames']
                if usednames:
                    Usednames.upsert_list_by_keyno(keyno, usednames, session=self.session, only_insert=only_insert)
            Annual = input_dict['Annual']
            if Annual:
                for each in Annual:
                    annual_che_year = each.get('annual_che_year')
                    annual_che_year_result = each.get('annual_che_year_result')
                    annual_report_data = each.get('annual_report_data')
                    annual_report_data_alter_stocks = each.get('annual_report_data_alter_stocks', list())
                    annual_report_data_investments = each.get('annual_report_data_investments', list())
                    annual_report_data_socsec_infos = each.get('annual_report_data_socsec_infos', list())
                    annual_report_data_alts = each.get('annual_report_data_alts', list())
                    annual_report_data_sponsors = each.get('annual_report_data_sponsors', list())
                    annual_report_data_guarantee_infos = each.get('annual_report_data_guarantee_infos', list())
                    annual_report_data_websites = each.get('annual_report_data_websites', list())
                    service_annual_che_year.fill_total(keyno, annual_che_year, annual_che_year_result,
                                                       annual_report_data, annual_report_data_alter_stocks,
                                                       annual_report_data_investments, annual_report_data_websites,
                                                       annual_report_data_socsec_infos, annual_report_data_alts,
                                                       annual_report_data_sponsors, annual_report_data_guarantee_infos,
                                                       only_insert=only_insert)
        else:
            raise Exception

    def throw_duplicate_company(self, companies):
        qualified_companies = [each for each in companies if each['Baseinfo']]
        firms = [each['Baseinfo'][0]['firm'] for each in qualified_companies if each['Baseinfo'][0]['firm']]
        result = KeynoMappingService.exists_list_by_firms(firms, session=self.session)
        new_companies = list()
        replenish_old_companies = list()
        replenish_old_companies_names = list()
        update_old_companies = list()
        update_old_companies_names = list()
        old_companies_names = set()
        for each in qualified_companies:
            name = each['Baseinfo'][0]['firm']
            if name not in result:
                new_companies.append(each)
            else:
                old_companies_names.add(name)
        infos = KeynoMappingService.get_keynos_by_firms(old_companies_names, session=self.session)
        old_companies_keyno_dict = dict(infos)
        result = ResourceService.exists_list_by_keynos(list(old_companies_keyno_dict.keys()), session=self.session)
        oldname_keyno_map = dict()
        for each in old_companies_keyno_dict:
            oldname_keyno_map[old_companies_keyno_dict[each]] = each
            if each in result:
                update_old_companies_names.append(old_companies_keyno_dict[each])
            else:
                replenish_old_companies_names.append(old_companies_keyno_dict[each])
        for each in qualified_companies:
            name = each['Baseinfo'][0]['firm']
            if name in update_old_companies_names:
                each['Baseinfo'][0]['keyno'] = oldname_keyno_map[name]
                update_old_companies.append(each)
            elif name in replenish_old_companies_names:
                each['Baseinfo'][0]['keyno'] = oldname_keyno_map[name]
                replenish_old_companies.append(each)
        return new_companies, replenish_old_companies, update_old_companies


    def main(self, companies):
        new_companies, replenish_old_companies, update_old_companies = self.throw_duplicate_company(companies)
        action_companies_num = 0
        if new_companies:
            keynos = Baseinfo.batch_insert([nc['Baseinfo'][0] for nc in new_companies], session=self.session)
            reginfos = list()
            for nc, keyno in zip(new_companies, keynos):
                reginfo = nc['Reginfo'][0] if nc['Reginfo'] else dict()
                reginfo['keyno'] = keyno
                reginfos.append(reginfo)
            reginfos = Reginfo.upsert_list_by_datas(reginfos, session=self.session, only_insert=True)
            for company, keyno in zip(new_companies, keynos):
                self.handler(company, keyno, True)
            action_companies_num += len(keynos)
        else:
            if replenish_old_companies:
                Baseinfo.__mark_resource__ = True
                Baseinfo.__mark_difference__ = False
                Reginfo.__mark_resource__ = False
                Reginfo.__mark_difference__ = False
                keynos = Baseinfo.batch_update_by_datas([nc['Baseinfo'][0] for nc in replenish_old_companies],
                                                        session=self.session)
                reginfos = list()
                keynos = [each.keyno for each in keynos]
                for nc, keyno in zip(replenish_old_companies, keynos):
                    reginfo = nc['Reginfo'][0] if nc['Reginfo'] else dict()
                    reginfo['keyno'] = keyno
                    reginfos.append(reginfo)
                reginfos = Reginfo.upsert_list_by_datas(reginfos, session=self.session, only_insert=False)
                for company, keyno in zip(replenish_old_companies, keynos):
                    self.handler(company, keyno, True)
                action_companies_num += len(keynos)
            if update_old_companies:
                Baseinfo.__mark_resource__ = True
                Baseinfo.__mark_difference__ = True
                Reginfo.__mark_resource__ = False
                Reginfo.__mark_difference__ = True
                keynos = Baseinfo.batch_update_by_datas([nc['Baseinfo'][0] for nc in update_old_companies],
                                                        session=self.session)
                reginfos = list()
                for nc, keyno in zip(update_old_companies, keynos):
                    reginfo = nc['Reginfo'][0] if nc['Reginfo'] else dict()
                    reginfo['keyno'] = keyno
                    reginfos.append(reginfo)
                reginfos = Reginfo.upsert_list_by_datas(reginfos, session=self.session, only_insert=False)
                for company, keyno in zip(update_old_companies, keynos):
                    self.handler(company, keyno, False)
                action_companies_num += len(keynos)
        self.session.commit()
        self.session.close()
        return action_companies_num


def performance_log_main(queue):
    process_name = multiprocessing.current_process().name
    logger = logging.getLogger('pipline_performance')
    logger.setLevel(logging.INFO)
    pipline_performance_handler = logging.FileHandler(filename=basedir + '/performance.log')
    pipline_performance_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    pipline_performance_handler.setFormatter(formatter)
    logger.addHandler(pipline_performance_handler)
    total_nums = 0
    total_timeing = time.time()
    while True:
        input_nums, affect_nums, timing = queue.get(True)
        total_nums += affect_nums
        logger.info('cost time: ' + str(timing) +
            ' input nums: ' + str(input_nums) +' affect nums: ' + str(affect_nums) +
            ' avg a/s: ' + str(float(affect_nums)/timing) +
            ' avg ta/s ' + str(float(total_nums)/(time.time() - total_timeing)))


def process_main(queue, performance_log_queue):
    Baseinfo.__engine__[Baseinfo._node].dispose()
    while True:
        li = queue.get(True)
        time_begin = time.time()
        pipline = UpsertAPIPipline()
        affect_company_num = pipline.main(li)
        time_end = time.time()
        performance_log_queue.put((len(li), affect_company_num, time_end - time_begin))


def detail_build_li(number, skip, collection):
    tables = collection.find({}, {'_id': 1, 'keyword': 1, 'info_resp': 1}).sort([('_id', 1)]).limit(number).skip(skip)
    li = []
    for table in tables:
        info_resp = table['info_resp']
        creditcode = table['keyword']
        try:
            result = GSXTParser().main(info_resp)
            li.append(result)
        except:
            pass
    return li


def detail_run(config_name):
    def detail_run_build_li(parser_result_li, lua_cmd):
        collections_length = len(MONGODB_COLLECTIONS)
        collection_offset = 0
        while True:
            each = MONGODB_COLLECTIONS[collection_offset]
            collection_offset += 1
            if collection_offset >= collections_length:
                collection_offset = 0
            HOST = each['host']
            PORT = each['port']
            DB = each['db']
            COLLECTION = each['collection']
            USER = each['user']
            PASS = each['password']
            statue_redis_tag = '/'.join([HOST, str(PORT), DB, COLLECTION, NAME])
            statue_redis = redis_conn.get(statue_redis_tag)
            client = pymongo.MongoClient(host=HOST, port=PORT, username=USER, password=PASS)
            db = client[DB]
            collection = db[COLLECTION]
            if not statue_redis:
                redis_conn.set(statue_redis_tag, 0)
            while True:
                if not client:
                    client = pymongo.MongoClient(host=HOST, port=PORT, username=USER, password=PASS)
                    db = client[DB]
                    collection = db[COLLECTION]
                total = collection.find({}).count()
                pre = int(lua_cmd(keys=[statue_redis_tag], args=[times, everytime]).decode())
                if pre < (total // (times * everytime)) * times * everytime:
                    li = detail_build_li(times * everytime, pre, collection)
                    if li:
                        parser_result_li.put(li, block=True, timeout=None)
                else:
                    redis_conn.incrby(statue_redis_tag, -times * everytime)
                    break

    config = load_config(basedir.replace(os.sep + 'pipline', '') + os.sep +
                         os.sep.join(['configs', config_name]))
    MONGODB_COLLECTIONS = json.loads(config['MONGO']['MONGODB_COLLECTIONS'])
    log_dir = config['LOG']['MYSQL_PIPLINE_LOG_DIR']
    logger = logging.getLogger('detail_pipline')
    logger.setLevel(logging.INFO)
    detail_pipline_handler = logging.FileHandler(filename=basedir + log_dir)
    detail_pipline_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    detail_pipline_handler.setFormatter(formatter)
    logger.addHandler(detail_pipline_handler)
    REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
    REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
    REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
    everytime = int(config['MYSQL_PIPLINE']['EVERYTIME'])
    detail_everytime = int(config['MYSQL_PIPLINE']['DETAIL_EVERYTIME'])
    times = int(config['MYSQL_PIPLINE']['TIMES'])
    queue_num = int(config['MYSQL_PIPLINE']['QUEUE_NUMBER'])
    processing_num = int(config['MYSQL_PIPLINE']['PROCESSING_NUMBER'])
    NAME = config['MYSQL_PIPLINE']['NAME']
    redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
    lua = '''
           local key = KEYS[1]
           local times = ARGV[1]
           local everytime = ARGV[2]
           local number = redis.call('get', key)
           redis.call('incrby', key, times*everytime)
           return number
       '''
    lua_cmd = redis_conn.register_script(lua)
    parser_result_li = queue.Queue(queue_num)
    threading.Thread(target=detail_run_build_li, args=(parser_result_li, lua_cmd)).start()
    performance_log_queue = multiprocessing.Queue()
    if processing_num:
        processing_queue = multiprocessing.Queue(maxsize=processing_num)
        processing_pool = multiprocessing.Pool(processing_num, process_main, (processing_queue, performance_log_queue,))
    else:
        processing_queue = queue.Queue(maxsize=1)
        processing_pool = multiprocessing.pool.ThreadPool(1, process_main, (processing_queue, performance_log_queue,))
    while True:
        company_total = 0
        li = parser_result_li.get()
        def chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i+n]
        start = time.time()
        loop_company_total = 0
        for li in chunks(li, detail_everytime):
            if li:
                processing_queue.put(li)
                loop_company_num = len(li)
                loop_company_total += loop_company_num
                company_total += loop_company_num
        if loop_company_total > 0:
            logger.info('cost time: ' + str(time.time() - start) +
                        ' loop company total: ' + str(loop_company_total) + ' company_total: ' + str(company_total) +
                        ' avg c/s: ' + str(float(loop_company_total) / (time.time() - start)))


def search_build_li(number, skip, collection):
    tables = collection.find({}, {'_id': 1, 'keyword': 1, 'info_resp': 1}).sort([('_id', 1)]).limit(number).skip(skip)
    li = []
    for table in tables:
        info_resp = table['info_resp']
        datas = list()
        for key in info_resp:
            if key.startswith('search_'):
                try:
                    data = json.loads(info_resp[key])['data']['result']['data']
                    datas.extend([{'search': json.dumps({'data': {'result': {'data': [each]}}})} for each in data])
                except:
                    pass
        try:
            for each in datas:
                result = GSXTParser().main(each)
                li.append(result)
        except:
            pass
    return li

def search_run(config_name):
    def build_li(parser_result_li, lua_cmd, redis_conn):
        discover_collections_length = len(DISCOVER_MONGODB_COLLECTIONS)
        discover_collection_offset = 0
        while True:
            each = DISCOVER_MONGODB_COLLECTIONS[discover_collection_offset]
            discover_collection_offset += 1
            if discover_collection_offset >= discover_collections_length:
                discover_collection_offset = 0
            HOST = each['host']
            PORT = each['port']
            DB = each['db']
            COLLECTION = each['collection']
            USER = each['user']
            PASS = each['password']
            statue_redis_tag = '/'.join([HOST, str(PORT), DB, COLLECTION, NAME])
            statue_redis = redis_conn.get(statue_redis_tag)
            client = pymongo.MongoClient(host=HOST, port=PORT, username=USER, password=PASS)
            db = client[DB]
            collection = db[COLLECTION]
            if not statue_redis:
                redis_conn.set(statue_redis_tag, 0)
            while True:
                if not client:
                    client = pymongo.MongoClient(host=HOST, port=PORT, username=USER, password=PASS)
                    db = client[DB]
                    collection = db[COLLECTION]
                search_pre = int(lua_cmd(keys=[statue_redis_tag], args=[times, everytime]).decode())
                total = collection.find({}).count()
                if search_pre < (total // (times * everytime)) * times * everytime:
                    li = search_build_li(times * everytime, search_pre, collection)
                    li = [json.dumps(each) for each in li]
                    li = list(set(li))
                    li = [json.loads(each) for each in li]
                    firms = list()
                    usednames = list()
                    for each in range(len(li)):
                        data = li[each]
                        if data['Baseinfo']:
                            firm = data['Baseinfo'][0]['firm']
                            firms.append([firm, each])
                            if data['SearchInfo']:
                                inner_li = data['SearchInfo'][0].get('usednames')
                                usednames.extend([d['usedname'] for d in inner_li])
                    temp = li[:]
                    for each in firms:
                        li_index = each[1]
                        firm = each[0]
                        if not firm or firm in usednames:
                            li.remove(temp[li_index])
                    if li:
                        parser_result_li.put(li, block=True, timeout=None)
                else:
                    redis_conn.incrby(statue_redis_tag, -times*everytime)
                    break
    discover_config = load_config(basedir.replace(os.sep + 'pipline', '') + os.sep +
                                  os.sep.join(['configs', config_name]))
    DISCOVER_MONGODB_COLLECTIONS = json.loads(discover_config['MONGO']['MONGODB_COLLECTIONS'])
    search_log_dir = discover_config['LOG']['MYSQL_SEARCH_PIPLINE_LOG_DIR']
    discover_everytime = int(discover_config['MYSQL_PIPLINE']['DISCOVER_EVERYTIME'])
    queue_num = int(discover_config['MYSQL_PIPLINE']['QUEUE_NUMBER'])
    processing_num = int(discover_config['MYSQL_PIPLINE']['PROCESSING_NUMBER'])
    REDIS_TAG_HOST = discover_config['REDIS_TAG']['REDIS_TAG_HOST']
    REDIS_TAG_PORT = int(discover_config['REDIS_TAG']['REDIS_TAG_PORT'])
    REDIS_TAG_DB = int(discover_config['REDIS_TAG']['REDIS_TAG_DB'])
    logger = logging.getLogger('search_pipline')
    logger.setLevel(logging.INFO)
    search_pipline_handler = logging.FileHandler(filename=basedir + search_log_dir)
    search_pipline_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    search_pipline_handler.setFormatter(formatter)
    logger.addHandler(search_pipline_handler)
    everytime = int(discover_config['MYSQL_PIPLINE']['EVERYTIME'])
    times = int(discover_config['MYSQL_PIPLINE']['TIMES'])
    NAME = discover_config['MYSQL_PIPLINE']['NAME']
    redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
    lua = '''
        local key = KEYS[1]
        local times = ARGV[1]
        local everytime = ARGV[2]
        local number = redis.call('get', key)
        redis.call('incrby', key, times*everytime)
        return number
    '''
    lua_cmd = redis_conn.register_script(lua)
    parser_result_li = queue.Queue(queue_num)
    performance_log_queue = multiprocessing.Queue()
    threading.Thread(target=build_li, args=(parser_result_li, lua_cmd, redis_conn)).start()
    threading.Thread(target=performance_log_main, args=(performance_log_queue,)).start()
    if processing_num:
        processing_queue = multiprocessing.Queue(maxsize=processing_num)
        processing_pool = multiprocessing.Pool(processing_num, process_main, (processing_queue, performance_log_queue,))
    else:
        processing_queue = queue.Queue(maxsize=1)
        processing_pool = multiprocessing.pool.ThreadPool(1, process_main, (processing_queue, performance_log_queue,))
    while True:
        li = parser_result_li.get()
        def chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]
        start = time.time()
        loop_company_total = 0
        for li in chunks(li, discover_everytime):
            if li:
                processing_queue.put(li)
                loop_company_num = len(li)
                loop_company_total += loop_company_num
        if loop_company_total > 0:
            logger.info('cost time: ' + str(time.time() - start) + ' loop company total: ' + str(loop_company_total) +
                        ' avg c/s: ' + str(float(loop_company_total)/(time.time() - start)))
