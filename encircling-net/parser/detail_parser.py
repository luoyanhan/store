# coding=utf-8
import re
import os
import sys
import ujson as json
import datetime
from urllib.parse import urlparse

import animal_case

basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'parser', ''))
sys.path.append(basedir)


class GSXTParser(object):
    remove_label_re = re.compile('<[^>]+>')

    def __init__(self):
        self.Baseinfo_baseinfo = {
            "entName": "firm",  # 企业名称
            "traName": "firm",  # 企业名称
            "uniscId": "creditcode",  # 统一社会信用代码
            "regNo": "reg_no",  # 当这个不为空的时候，"reg_no_type"入’工商注册号‘
            "reg_no_type": "reg_no_type",  # 当"regNo"不为空的时候，"reg_no_type"入’工商注册号‘
            "estDate": "regdate",  # 成立日期
        }

        self.Baseinfo_reginfo = {
            "regOrg_CN": "belongorg",  # 登记机关
            "name": "person_in_charge",  # 法定代表人/负责人/执行事务合伙人
            "opFrom": "startdate",  # 营业/合伙期限自
            "opTo": "enddate",  # 营业/合伙期限至
            "regState_CN": "status",  # 登记状态
            "regCaption": "registered_capital",  # 注册资本,应该是"regCaption"+‘万’+"regCapCurCN"拼接
            "entType_CN": "firmtype",  # 类型
            "dom": "address",  # 主要经营场所/住所
            "opLoc": "address",  # 主要经营场所/住所
            "opScope": "scope",  # 经营范围
            "orgcode": "orgcode",  # 不入库
            "siccode": "siccode",  # 不入库
        }

        self.SearchInfo_baseinfo = {
            "entName": "firm",  # 企业名称
            "traName": "firm",  # 企业名称
            "uniscId": "creditcode",  # 统一社会信用代码
            "regNo": "reg_no",  # 当这个不为空的时候，"reg_no_type"入’工商注册号‘
            "reg_no_type": "reg_no_type",  # 当"regNo"不为空的时候，"reg_no_type"入’工商注册号‘
            "estDate": "regdate",  # 成立日期
        }

        self.SearchInfo_reginfo = {
            "regOrg": "belongorg",  # 登记机关
            "legelRep": "person_in_charge",  # 法定代表人/负责人/执行事务合伙人
            "opFrom": "startdate",  # 营业/合伙期限自
            "opTo": "enddate",  # 营业/合伙期限至
            "corpStatusString": "status",  # 登记状态
            "regCaption": "registered_capital",  # 注册资本,应该是"regCaption"+‘万’+"regCapCurCN"拼接
            "entTypeCn": "firmtype",  # 类型
            "dom": "address",  # 主要经营场所/住所
            "opLoc": "address",  # 主要经营场所/住所
            "opScope": "scope",  # 经营范围
            "orgcode": "orgcode",  # 不入库
            "siccode": "siccode",  # 不入库
        }

        self.ShareholderScontributionDesc = {
            "inv": "name",  # 股东名称
            "invType_CN": "type",  # 股东类型
            "cerNo": "certificate",  # 证件号码
            "cerType_CN": "certificate_type",  # 当"cerNo"不为空的时候，这个才入库
            "liSubConAm": "promise_capital",
            "liAcConAm": "actual_capital",
            "url": "url",  # 确定唯一性
        }

        self.unit_list = ["acConAm", "subConAm", "liSubConAm", "liAcConAm", "acConAmStr", "subConAmStr", "aubSum",
                          "subSum"]

        self.ShareholderScontribution_detail_subDetailsDesc = {
            "conForm_CN": "promise_capital_type",  # 认缴出资方式
            "subConAm": "promise_capital",  # 认缴出资金额
            "conDate": "promise_capital_date",  # 认缴出资日期
        }

        # 认缴详情默认值
        self.ShareholderScontribution_detail_subDetails_default = {
            "promise_capital_type": "",  # 认缴出资方式
            "promise_capital": "",  # 认缴出资金额
            "promise_capital_date": "",  # 认缴出资日期
        }

        self.ShareholderScontribution_detail_aubDetailsDesc = {
            "conForm_CN": "actual_capital_type",  # 实缴出资方式
            "acConAm": "actual_capital",  # 实缴出资金额
            "conDate": "actual_capital_date",  # 实缴出资日期
        }

        # 实缴详情默认值
        self.ShareholderScontribution_detail_aubDetails_default = {
            "actual_capital_type": "",  # 实缴出资方式
            "actual_capital": "",  # 实缴出资金额
            "actual_capital_date": "",  # 实缴出资日期
        }

        self.BranchInformationDesc = {
            "brName": "name",  # 分支机构名称,特殊处理
        }

        self.LiquidationDesc = {
            "liqMem": "principal",
            "ligpriSign": "role"
        }

        self.ChangerecordDesc = {
            "altItem_CN": "changetype",  # 变更类型
            "altBe": "before",  # 变更前
            "altAf": "after",  # 变更后
            "altDate": "changedate",  # 变更时间
        }

        self.AdministrativeLicensingDesc = {
            "licNo": "license_no",  # 许可文件编号
            "licName_CN": "license_name",  # 许可文件名称
            "valFrom": "valid_from",  # 有效期自
            "valTo": "valid_until",  # 有效期至
            "licAnth": "organ",  # 许可机关
            "licItem": "content",  # 许可内容
        }

        self.AdministrativePenaltyDesc = {
            "penDecNo": "document_number",  # 行政处罚决定书文号
            "illegActType": "type",  # 违法行为类型
            "penContent": "content",  # 行政处罚内容
            "penAuth_CN": "organ",  # 作出行政处罚决定机关名称
            "penDecIssDate": "decision_date",  # 作出行政处罚决定日期
            "publicDate": "public_date",  # 公示日期
        }

        self.ExceptionInformationDesc = {
            "speCause_CN": "entry_reason",  # 列入经营异常名录原因
            "abntime": "entry_date",  # 列入日期
            "decOrg_CN": "entry_organ",  # 作出决定机关(列入)
            "remExcpRes_CN": "removal_reason",  # 移出经营异常名录原因
            "remDate": "removal_date",  # 移出日期
            "reDecOrg_CN": "removal_organ",  # 作出决定机关(移出)
        }

        self.IllegalInformationDesc = {
            "serILLRea_CN": "entry_reason",  # 列入严重违法企业名单原因
            "abntime": "entry_date",  # 列入日期
            "decOrg_CN": "entry_organ",  # 作出决定机关(列入)
            "remExcpRes_CN": "removal_reason",  # 移出严重违法企业名单原因
            "remDate": "removal_date",  # 移出日期
            "reDecOrg_CN": "removal_organ",  # 作出决定机关(移出)
        }

        self.CheckInformationDesc = {
            "insAuth_CN": "organ",  # 检查实施机关
            "insType_CN": "type",  # 类型
            "insDate": "date",  # 日期
            "insRes_CN": "result",  # 结果
        }

        self.JudicialInformationDesc = {
            "inv": "executed_legal_person",  # 被执行人
            "froAm": "amount_of_equity",  # 股权数额
            "froAuth": "court",  # 执行法院
            "executeNo": "document_number",  # 执行通知书文号
            "frozState_CN": "status"
        }

        self.ChattelMortgage = {
            "morRegCNo": "registration_number",  # 登记编号
            "regiDate": "registration_date",  # 登记日期
            "regOrg_CN": "registration_organ",  # 登记机关
            "priClaSecAm": "amount_of_secured_claims",  # 被担保债权数额
            "type": "status",  # ’1‘为有效，’2‘为无效
            "publicDate": "public_date",  # 公示日期
        }

        self.EquityPledgeDesc = {
            "equityNo": "registration_number",  # 登记编号
            "pledgor": "pledgor",  # 出质人
            "pledgor_id": "pledgor_id",  # 不入库
            "impAm": "amount_of_equity",  # 出质股权数额
            "impOrg": "pledgee",  # 质权人
            "equPleDate": "registration_date",  # 股权出质设立登记日期
            "type": "status",  # ’1‘为有效，’2‘为无效
            "publicDate": "public_date",  # 公示日期
        }

        self.MainPersonnelDesc = {
            "name": "name",  # 姓名
            "position_CN": "title",  # 职位
            "certificate": "certificate",  # 不入库
            "certificate_type": "certificate_type",  # 不入库
        }

        self.ShareholderScontribution_SELFDesc = {
            "inv": "shareholder",  # 股东
            "aubSum": "actual_capital",  # 实缴额
            "subSum": "promise_capital",  # 认缴额
            "aubDetails": "real_payment_detail",  # 实缴详情
            "subDetails": "confis_payment_detail",  # 认缴详情
        }

        self.ShareholderScontribution_SELF_aubDetailsDesc = {
            "acConFormName": "actual_capital_type",  # 实缴出资方式
            "publicDate": "actual_capital_publicdate",  # 实缴出资公示日期
            "acConAmStr": "actual_capital",  # 实缴出资金额
            "conDate": "actual_capital_date",  # 实缴出资日期
        }

        self.ShareholderScontribution_SELF_subDetailsDesc = {
            "subConForm_CN": "promise_capital_type",  # 认缴出资方式
            "publicDate": "promise_capital_publicdate",  # 认缴出资公示日期
            "subConAmStr": "promise_capital",  # 认缴出资金额
            "currency": "promise_capital_date",  # 认缴出资日期
        }

        self.SearchInfoHistoryName = {
            "historyName": "usedname",  # 曾用名
            "uniscId": "creditcode",  # 统一社会信用代码
        }

    def baseinfo_resp(self, company_dict):
        try:
            baseinfo_convert_list = []
            reginfo_convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict["base_info"])]
            if not raw_info:
                return None, None
            for data in raw_info:
                result = data.get("result")
                if not result:
                    continue
                result["regCapCurCN"] = data.get("regCapCurCN")
                global unit
                unit = "万" + data.get("regCapCurCN", "")
                result["regCaption"] = data.get("regCaption")
                result["regCap"] = data.get("regCap")
                data_list.append(result)
            for result in data_list:
                baseinfo_convert_dict = {}
                reginfo_convert_dict = {}
                for key in result:
                    if key in self.Baseinfo_baseinfo:
                        baseinfo_convert_dict[self.Baseinfo_baseinfo[key]] = str(result.get(key)) \
                            if result.get(key) else None
                    elif key in self.Baseinfo_reginfo:
                        reginfo_convert_dict[self.Baseinfo_reginfo[key]] = str(result.get(key)) \
                            if result.get(key) else None
                if result.get("regCaption"):
                    registered_capital = result.get("regCaption") + unit
                    reginfo_convert_dict["registered_capital"] = registered_capital
                else:
                    reginfo_convert_dict["registered_capital"] = None
                baseinfo_convert_list.append(baseinfo_convert_dict)
                reginfo_convert_list.append(reginfo_convert_dict)
            return baseinfo_convert_list, reginfo_convert_list
        except:
            return None, None

    def baseinfo_resp_sqare(self, company_dict):
        try:
            baseinfo_convert_list = []
            reginfo_convert_list = []
            raw_info = json.loads(company_dict["search"])['data']['result']['data']
            if not raw_info:
                return None, None
            for result in raw_info:
                global unit
                unit = "万" + result.get("regCapCurCN", "")
                baseinfo_convert_dict = {}
                reginfo_convert_dict = {}
                for key in result:
                    if key in self.SearchInfo_baseinfo:
                        baseinfo_convert_dict[self.SearchInfo_baseinfo[key]] = self.remove_label(str(result.get(key))) \
                            if result.get(key) else None
                    elif key in self.SearchInfo_reginfo:
                        reginfo_convert_dict[self.SearchInfo_reginfo[key]] = self.remove_label(str(result.get(key))) \
                            if result.get(key) else None
                if result.get("regCap"):
                    registered_capital = str(result.get("regCap")) + unit
                    reginfo_convert_dict["registered_capital"] = registered_capital
                else:
                    reginfo_convert_dict["registered_capital"] = None
                baseinfo_convert_list.append(baseinfo_convert_dict)
                reginfo_convert_list.append(reginfo_convert_dict)
            return baseinfo_convert_list, reginfo_convert_list
        except:
            return None, None

    def shareholder_resp(self, company_dict, shareholder_keys):
        try:
            if not shareholder_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in shareholder_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.ShareholderScontributionDesc.get(key):
                        continue
                    if key in self.unit_list:
                        convert_dict[self.ShareholderScontributionDesc.get(key)] = str(item.get(key)) + unit \
                            if item.get(key) else None
                    else:
                        convert_dict[self.ShareholderScontributionDesc.get(key)] = str(item.get(key)) if item.get(
                            key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def shareholder_resp_detail(self, company_dict, shareholder_detail_keys):
        try:
            if not shareholder_detail_keys:
                return None
            raw_info = [json.loads(company_dict[key]) for key in shareholder_detail_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            data_list = []
            for num in range(len(raw_info)):
                data = raw_info[num]
                i = data.get("data")
                inner_dict = {}
                inner_dict["real_payment_detail"] = self.shareholder_resp_detail_aubdetail(i[0]) if i else None
                inner_dict["confis_payment_detail"] = self.shareholder_resp_detail_subdetail(i[1]) if i else None
                inner_dict["name"] = shareholder_detail_keys[num].split('page')[1]
                data_list.append(inner_dict)
            convert_list = []
            for item in data_list:
                convert_list.append(item)
            return convert_list
        except:
            pass

    def shareholder_resp_detail_subdetail(self, subdetail):
        convert_list = []
        for item in subdetail:
            convert_dict = {}
            for key in item.keys():
                if not self.ShareholderScontribution_detail_subDetailsDesc.get(key):
                    continue
                if key in self.unit_list:
                    convert_dict[self.ShareholderScontribution_detail_subDetailsDesc.get(key)] = str(
                        item.get(key)) + unit if item.get(key) else None
                else:
                    convert_dict[self.ShareholderScontribution_detail_subDetailsDesc.get(key)] = str(
                        item.get(key)) if item.get(key) else None
            convert_list.append(convert_dict)
        if not convert_list:
            convert_list.append(self.ShareholderScontribution_detail_subDetails_default)
        return convert_list

    def shareholder_resp_detail_aubdetail(self, aubdetail):
        convert_list = []
        for item in aubdetail:
            convert_dict = {}
            for key in item.keys():
                if not self.ShareholderScontribution_detail_aubDetailsDesc.get(key):
                    continue
                if key in self.unit_list:
                    convert_dict[self.ShareholderScontribution_detail_aubDetailsDesc.get(key)] = str(
                        item.get(key)) + unit if item.get(key) else None
                else:
                    convert_dict[self.ShareholderScontribution_detail_aubDetailsDesc.get(key)] = str(
                        item.get(key)) if item.get(key) else None
            convert_list.append(convert_dict)
        if not convert_list:
            convert_list.append(self.ShareholderScontribution_detail_aubDetails_default)
        return convert_list

    def branch_resp(self, company_dict, branch_keys):
        try:
            if not branch_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in branch_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.BranchInformationDesc.get(key):
                        continue
                    convert_dict[self.BranchInformationDesc.get(key)] = str(item.get(key)) if item.get(key) and len(
                        item.get(key)) >= 4 else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def liquidation_resp(self, company_dict, liquidation_keys):
        try:
            if not liquidation_keys:
                return None
            new_list = list()
            convert_list = list()
            data_list = list()
            raw_info = [json.loads(company_dict[key]) for key in liquidation_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                data_list.append(data.get("data"))
            for item in data_list:
                for j in item:
                    convert_dict = dict()
                    for key in j.keys():
                        if not self.LiquidationDesc.get(key):
                            continue
                        convert_dict[self.LiquidationDesc.get(key)] = str(j.get(key)) if j.get(key) else None
                    convert_list.append(convert_dict)
                new_list.append(self.handle_liquidation(convert_list))
            return new_list
        except:
            return None

    def handle_liquidation(self, convert_list):
        try:
            name_list = []
            convert_dict = {
                "principal": "",
                "name": name_list,
            }
            for i in convert_list:
                if i.get("principal") and i.get("role") == "1":
                    convert_dict["principal"] = i.get("principal").strip()
                elif i.get("principal") and i.get("role") != "1":
                    principal_str = i.get("principal")
                    if "," in principal_str:
                        name_list = name_list + principal_str.split(',')
                    elif '、' in principal_str:
                        name_list = name_list + principal_str.split('、')
                    elif "，" in principal_str:
                        name_list = name_list + principal_str.split("，")
                    else:
                        name_list = name_list + [principal_str]
                    convert_dict["name"] = name_list
            return convert_dict
        except:
            return None

    def changerecord_resp(self, company_dict, changerecord_keys):
        try:
            if not changerecord_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in changerecord_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.ChangerecordDesc.get(key):
                        continue
                    convert_dict[self.ChangerecordDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def administrativelicensing_resp(self, company_dict, administrativelicensing_keys):
        try:
            if not administrativelicensing_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in administrativelicensing_keys
                        if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.AdministrativeLicensingDesc.get(key):
                        continue
                    convert_dict[self.AdministrativeLicensingDesc.get(key)] = str(item.get(key)) if item.get(
                        key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def administrativepenalty_resp(self, company_dict, administrativepenalty_keys):
        try:
            if not administrativepenalty_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in administrativepenalty_keys
                        if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.AdministrativePenaltyDesc.get(key):
                        continue
                    convert_dict[self.AdministrativePenaltyDesc.get(key)] = str(item.get(key)) if item.get(
                        key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def exceptioninformation_resp(self, company_dict, exceptioninformation_keys):
        try:
            if not exceptioninformation_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in exceptioninformation_keys
                        if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.ExceptionInformationDesc.get(key):
                        continue
                    convert_dict[self.ExceptionInformationDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def illegalinformation_resp(self, company_dict, illegalinformation_keys):
        try:
            if not illegalinformation_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in illegalinformation_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.IllegalInformationDesc.get(key):
                        continue
                    convert_dict[self.IllegalInformationDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def checkinformation_resp(self, company_dict, checkinformation_keys):
        try:
            if not checkinformation_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in checkinformation_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.CheckInformationDesc.get(key):
                        continue
                    convert_dict[self.CheckInformationDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def judicialinformation_resp(self, company_dict, judicialinformation_keys):
        try:
            if not judicialinformation_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in judicialinformation_keys
                        if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.JudicialInformationDesc.get(key):
                        continue
                    convert_dict[self.JudicialInformationDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def mortreginfo_resp(self, company_dict, mortreginfo_keys):
        try:
            if not mortreginfo_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in mortreginfo_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.ChattelMortgage.get(key):
                        continue
                    convert_dict[self.ChattelMortgage.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def equitypledge_resp(self, company_dict, equitypledge_keys):
        try:
            if not equitypledge_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in equitypledge_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.EquityPledgeDesc.get(key):
                        continue
                    convert_dict[self.EquityPledgeDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def employe_resp(self, company_dict, employe_keys):
        try:
            if not employe_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in employe_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.MainPersonnelDesc.get(key):
                        continue
                    convert_dict[self.MainPersonnelDesc.get(key)] = str(item.get(key)) if item.get(key) else None
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def self_shareholder_resp(self, company_dict, self_shareholder_keys):
        try:
            if not self_shareholder_keys:
                return None
            convert_list = []
            data_list = []
            raw_info = [json.loads(company_dict[key]) for key in self_shareholder_keys if company_dict[key] != 'FAIL']
            if not raw_info:
                return None
            for data in raw_info:
                for i in data.get("data"):
                    data_list.append(i)
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.ShareholderScontribution_SELFDesc.get(key):
                        continue
                    if key in self.unit_list:
                        convert_dict[self.ShareholderScontribution_SELFDesc.get(key)] = str(item.get(key)) + unit
                    else:
                        convert_dict[self.ShareholderScontribution_SELFDesc.get(key)] = str(item.get(key))
                convert_dict["real_payment_detail"] = self.sharehoder_aubdetail(item.get("aubDetails"))
                convert_dict["confis_payment_detail"] = self.sharehoder_subdetail(item.get("subDetails"))
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def sharehoder_aubdetail(self, aubdetail):
        convert_list = []
        for item in aubdetail:
            convert_dict = {}
            for key in item.keys():
                if not self.ShareholderScontribution_SELF_aubDetailsDesc.get(key):
                    continue
                if key in self.unit_list:
                    convert_dict[self.ShareholderScontribution_SELF_aubDetailsDesc.get(key)] = str(
                        item.get(key)) + unit if item.get(key) else None
                else:
                    convert_dict[self.ShareholderScontribution_SELF_aubDetailsDesc.get(key)] = str(
                        item.get(key)) if item.get(key) else None
            convert_list.append(convert_dict)
        if not convert_list:
            convert_list.append(self.ShareholderScontribution_detail_aubDetails_default)
        return convert_list

    def sharehoder_subdetail(self, subdetail):
        convert_list = []
        for item in subdetail:
            convert_dict = {}
            for key in item.keys():
                if not self.ShareholderScontribution_SELF_subDetailsDesc.get(key):
                    continue
                if key in self.unit_list:
                    convert_dict[self.ShareholderScontribution_SELF_subDetailsDesc.get(key)] = str(
                        item.get(key)) + unit if item.get(key) else None
                else:
                    convert_dict[self.ShareholderScontribution_SELF_subDetailsDesc.get(key)] = str(
                        item.get(key)) if item.get(key) else None
            convert_list.append(convert_dict)
        if not convert_list:
            convert_list.append(self.ShareholderScontribution_detail_subDetails_default)
        return convert_list

    def searchinfo_resp_history_name(self, company_dict):
        try:
            convert_list = []
            data_list = json.loads(company_dict["search"])['data']['result']['data']
            if not data_list:
                return data_list
            for item in data_list:
                convert_dict = {}
                for key in item.keys():
                    if not self.SearchInfoHistoryName.get(key):
                        continue
                    if key != "historyName":
                        convert_dict[self.SearchInfoHistoryName.get(key)] = str(self.remove_label(item.get(key))) \
                            if item.get(key) else None
                    else:
                        usedname_list = item.get(key).split("；") if item.get(key) else list()
                        convert_dict[self.SearchInfoHistoryName.get(key)] = [self.remove_label(i) for i in
                                                                             usedname_list]
                convert_list.append(convert_dict)
            return convert_list
        except:
            return None

    def annual(self, company_dict, annual_keys):
        convert_list = []
        for key in annual_keys:
            if company_dict[key] != "FAIL":
                each = json.loads(company_dict[key])
                annual_che_year_result = each['result']
                annual_report_data = each['annRep']
                result = {
                    'annual_che_year': {
                        'an_che_id': annual_report_data['annRepData']['anCheId'],
                        'ancheyear': each['anCheYear'],
                        'ent_type_forquery': each['entTypeForQuery'],
                        'is_invest': annual_report_data['isInvest'],
                        'is_web': annual_report_data['isWeb'],
                        'is_morg': annual_report_data['isMorg'],
                        'is_stock_alter': annual_report_data['isStockAlter']
                    },
                    'annual_che_year_result': annual_che_year_result,
                    'annual_report_data': annual_report_data['annRepData'],
                    'annual_report_data_alter_stocks': annual_report_data['annRepDataAlterstock'],
                    'annual_report_data_investments': annual_report_data['annRepDataInvestment'],
                    'annual_report_data_socsec_infos': annual_report_data['annRepDataSocsecinfo'],
                    'annual_report_data_alts': annual_report_data['annRepDataAlt'],
                    'annual_report_data_sponsors': annual_report_data['annRepDataSponsor'],
                    'annual_report_data_guarantee_infos': annual_report_data['annRepDataGuaranteeinfo'],
                    'annual_report_data_websites': annual_report_data['annRepDataWebsite']
                }
                convert_list.append(animal_case.parse_keys(result))
        return convert_list

    def remove_label(self, value):
        new_value = self.remove_label_re.sub("", value)
        return new_value

    def get_searchinfo_creditcode(self, output):
        creditcode = ""
        for item in output.get("SearchInfo"):
            if item.get("creditcode"):
                creditcode = item.get("creditcode")
                break
        return creditcode

    def check_items_value(self, item):
        values = dict(item).values()
        check_list = [value if isinstance(value, str) else None for value in values]
        for i in check_list:
            if i:
                return True
        return False

    def main(self, content):
        company_dict = content
        output = dict()
        shareholder_keys = list()
        shareholder_detail_keys = list()
        branch_keys = list()
        liquidation_keys = list()
        changerecord_keys = list()
        administrativelicensing_keys = list()
        administrativepenalty_keys = list()
        exceptioninformation_keys = list()
        illegalinformation_keys = list()
        checkinformation_keys = list()
        judicialinformation_keys = list()
        mortreginfo_keys = list()
        equitypledge_keys = list()
        employe_keys = list()
        self_shareholder_keys = list()
        annual_keys = list()
        for key in company_dict:
            if key.startswith('sharehold_'):
                shareholder_keys.append(key)
                shareholder_detail_keys.append(key)
            elif key.startswith('branch_'):
                branch_keys.append(key)
            elif key.startswith('liquidation_'):
                liquidation_keys.append(key)
            elif key.startswith('changerecord_'):
                changerecord_keys.append(key)
            elif key.startswith('administrativelicensing_'):
                administrativelicensing_keys.append(key)
            elif key.startswith('administrativepenalty_'):
                administrativepenalty_keys.append(key)
            elif key.startswith('exceptioninformation_'):
                exceptioninformation_keys.append(key)
            elif key.startswith('illegalinformation_'):
                illegalinformation_keys.append(key)
            elif key.startswith('checkinformation_'):
                checkinformation_keys.append(key)
            elif key.startswith('judicialinformation_'):
                judicialinformation_keys.append(key)
            elif key.startswith('mortreginfo_'):
                mortreginfo_keys.append(key)
            elif key.startswith('equitypledge_'):
                equitypledge_keys.append(key)
            elif key.startswith('keyPerson_'):
                employe_keys.append(key)
            elif key.startswith('self_shareholder_'):
                self_shareholder_keys.append(key)
            elif key.isdigit():
                annual_keys.append(key)
        baseinfo, reginfo = self.baseinfo_resp(company_dict)
        output["Baseinfo"] = baseinfo
        output["Reginfo"] = reginfo
        if not output["Baseinfo"]:
            baseinfo, reginfo = self.baseinfo_resp_sqare(company_dict)
            if baseinfo:
                output["Baseinfo"] = baseinfo
            if reginfo and not output["Reginfo"]:
                output["Reginfo"] = reginfo
        shareholder = self.shareholder_resp(company_dict, shareholder_keys)
        if shareholder:
            output["ShareholderScontribution"] = shareholder
        shareholderdetail = self.shareholder_resp_detail(company_dict, shareholder_detail_keys)
        if shareholderdetail:
            output["ShareholderScontribution_detail"] = shareholderdetail
        branch = self.branch_resp(company_dict, branch_keys)
        if branch:
            output["BranchInformation"] = branch
        liquidation = self.liquidation_resp(company_dict, liquidation_keys)
        if liquidation:
            output["Liquidation"] = liquidation
        changerecord = self.changerecord_resp(company_dict, changerecord_keys)
        if changerecord:
            output["Changerecord"] = changerecord
        administrativelicensing = self.administrativelicensing_resp(company_dict, administrativelicensing_keys)
        if administrativelicensing:
            output["AdministrativeLicensing"] = administrativelicensing
        administrativepenalty = self.administrativepenalty_resp(company_dict, administrativepenalty_keys)
        if administrativepenalty:
            output["AdministrativePenalty"] = administrativepenalty
        exceptioninformation = self.exceptioninformation_resp(company_dict, exceptioninformation_keys)
        if exceptioninformation:
            output["ExceptionInformation"] = exceptioninformation
        illegalinformation = self.illegalinformation_resp(company_dict, illegalinformation_keys)
        if illegalinformation:
            output["IllegalInformation"] = illegalinformation
        checkinformation = self.checkinformation_resp(company_dict, checkinformation_keys)
        if checkinformation:
            output["CheckInformation"] = checkinformation
        judicialinformation = self.judicialinformation_resp(company_dict, judicialinformation_keys)
        if judicialinformation:
            output["JudicialInformation"] = judicialinformation
        mortreginfo = self.mortreginfo_resp(company_dict, mortreginfo_keys)
        if mortreginfo:
            output["ChattelMortgage"] = mortreginfo
        equitypledge = self.equitypledge_resp(company_dict, equitypledge_keys)
        if equitypledge:
            output["EquityPledge"] = equitypledge
        employe = self.employe_resp(company_dict, employe_keys)
        if employe:
            output["MainPersonnel"] = employe
        self_shareholder = self.self_shareholder_resp(company_dict, self_shareholder_keys)
        if self_shareholder:
            output["ShareholderScontribution_SELF"] = self_shareholder
        searchinfo_resp = self.searchinfo_resp_history_name(company_dict)
        if searchinfo_resp:
            output["SearchInfo"] = searchinfo_resp
        annual = self.annual(company_dict, annual_keys)
        if annual:
            output["Annual"] = annual

        result = dict()
        for key in ['Baseinfo', 'Reginfo', 'SearchInfo', 'BranchInformation', 'Liquidation', 'Changerecord',
                    'AdministrativeLicensing', 'AdministrativePenalty', 'ExceptionInformation', 'IllegalInformation',
                    'CheckInformation', 'JudicialInformation', 'ChattelMortgage', 'EquityPledge', 'MainPersonnel',
                    'Annual']:
            result[key] = list()

        if output.get("Baseinfo") is not None:
            data_list = output.get("Baseinfo")
            for item_base in data_list:
                if not item_base.get('creditcode'):
                    item_base['creditcode'] = self.get_searchinfo_creditcode(output=output)
                item_base['collect_time'] = str(datetime.datetime.now())
                result['Baseinfo'].append(item_base)

        if output.get("Reginfo") is not None:
            data_list = output.get("Reginfo")
            for item_reg in data_list:
                item_reg['tel'] = None
                item_reg['email'] = None
                item_reg['website'] = None
                result['Reginfo'].append(item_reg)

        if output.get("SearchInfo") is not None:
            data_list = output.get("SearchInfo")
            item = dict()
            usednames = list()
            for i in data_list:
                if i.get("usedname"):
                    for j in i.get("usedname"):
                        usednames.append({"usedname": j})
            if usednames:
                item["usednames"] = usednames
                result['SearchInfo'].append(item)

        if output.get("ShareholderScontribution") is not None:
            shareholders = output.get("ShareholderScontribution")
            shareholders_item = dict()
            shareholders_item_list = list()

            shareholder_details = output.get("ShareholderScontribution_SELF")
            shareholder_resp_details = output.get("ShareholderScontribution_detail")

            if shareholders and not shareholder_details:
                if shareholders and not shareholder_resp_details:
                    for shareholder in shareholders:
                        item = dict()
                        shareholder_item = dict()
                        shareholderscontribution = {**shareholder}
                        shareholder_item["name"] = shareholderscontribution.get("name") \
                            if shareholderscontribution.get("name") else None
                        shareholder_item["type"] = shareholderscontribution.get("type") \
                            if shareholderscontribution.get("type") else None
                        shareholder_item["certificate"] = shareholderscontribution.get("certificate") \
                            if shareholderscontribution.get("certificate") else None
                        shareholder_item["certificate_type"] = shareholderscontribution.get("certificate_type") \
                            if shareholderscontribution.get("certificate_type") else None
                        shareholder_item["promise_capital"] = shareholderscontribution.get("promise_capital") \
                            if shareholderscontribution.get("promise_capital") else None
                        shareholder_item["actual_capital"] = shareholderscontribution.get("actual_capital") \
                            if shareholderscontribution.get("actual_capital") else None
                        if not self.check_items_value(shareholder_item):
                            continue
                        item["capital_id_item"] = shareholder_item
                        shareholders_item_list.append(item)
                elif shareholders and shareholder_resp_details:
                    count_url = list()
                    for shareholder in shareholders:
                        for shareholder_resp_detail in shareholder_resp_details:
                            shareholder_url = urlparse(shareholder.get("url"))
                            shareholder_resp_detail_url = urlparse(shareholder.get("url"))
                            if shareholder_resp_detail_url.path + shareholder_resp_detail_url.query == \
                                    shareholder_url.path + shareholder_url.query and shareholder_resp_detail_url.path + \
                                    shareholder_resp_detail_url.query not in count_url:
                                count_url.append(
                                    shareholder_resp_detail_url.path + shareholder_resp_detail_url.query)
                                shareholderscontribution = {**shareholder}
                                real_payment_detail = shareholder_resp_detail.get("real_payment_detail")
                                confis_payment_detail = shareholder_resp_detail.get("confis_payment_detail")
                                range_len = len(real_payment_detail) if len(real_payment_detail) >= len(
                                    confis_payment_detail) else len(confis_payment_detail)
                                new_list = list()
                                for index in range(range_len):
                                    if index < len(real_payment_detail) and index < len(confis_payment_detail):
                                        new_list.append(
                                            {**real_payment_detail[index], **confis_payment_detail[index]})
                                    elif index < len(real_payment_detail) and index >= len(confis_payment_detail):
                                        new_list.append({**real_payment_detail[index]})
                                    elif index >= len(real_payment_detail) and index < len(confis_payment_detail):
                                        new_list.append({**confis_payment_detail[index]})
                                for j in new_list:
                                    item = dict()
                                    item['additions'] = list()
                                    shareholder_item = dict()
                                    shareholder_item["name"] = shareholderscontribution.get("name") \
                                        if shareholderscontribution.get("name") else None
                                    shareholder_item["type"] = shareholderscontribution.get("type") \
                                        if shareholderscontribution.get("type") else None
                                    shareholder_item["certificate"] = shareholderscontribution.get(
                                        "certificate") \
                                        if shareholderscontribution.get("certificate") else None
                                    shareholder_item["certificate_type"] = shareholderscontribution.get(
                                        "certificate_type") \
                                        if shareholderscontribution.get("certificate_type") else None
                                    shareholder_item["promise_capital"] = shareholderscontribution.get(
                                        "promise_capital") \
                                        if shareholderscontribution.get("promise_capital") else None
                                    shareholder_item["actual_capital"] = shareholderscontribution.get(
                                        "actual_capital") \
                                        if shareholderscontribution.get("actual_capital") else None
                                    if not self.check_items_value(shareholder_item):
                                        continue
                                    item["capital_id_item"] = shareholder_item
                                    i = dict()
                                    i["capital_type"] = j.get("promise_capital_type") \
                                        if j.get("promise_capital_type") else None
                                    i["capital_publicdate"] = j.get("promise_capital_publicdate") \
                                        if j.get("promise_capital_publicdate") else None
                                    i["capital"] = j.get("promise_capital") \
                                        if j.get("promise_capital") else None
                                    i["capital_date"] = j.get("promise_capital_date") \
                                        if j.get("promise_capital_date") else None
                                    i["capital_enum"] = "promise"
                                    item['additions'].append(i)
                                    i1 = dict()
                                    i1["capital_type"] = j.get("actual_capital_type") \
                                        if j.get("actual_capital_type") else None
                                    i1["capital_publicdate"] = j.get("actual_capital_publicdate") \
                                        if j.get("actual_capital_publicdate") else None
                                    i1["capital"] = j.get("actual_capital") \
                                        if j.get("actual_capital") else None
                                    i1["capital_date"] = j.get("actual_capital_date") \
                                        if j.get("actual_capital_date") else None
                                    i1["capital_enum"] = "actual"
                                    item['additions'].append(i1)
                                    shareholders_item_list.append(item)
            elif not shareholders and shareholder_details:
                for shareholder_detail in shareholder_details:
                    shareholderscontribution = {**shareholder_detail}
                    real_payment_detail = shareholderscontribution.get("real_payment_detail")
                    confis_payment_detail = shareholderscontribution.get("confis_payment_detail")
                    range_len = len(real_payment_detail) if len(real_payment_detail) >= len(
                        confis_payment_detail) else len(confis_payment_detail)
                    new_list = []
                    for index in range(range_len):
                        if index < len(real_payment_detail) and index < len(confis_payment_detail):
                            new_list.append(
                                {**real_payment_detail[index], **confis_payment_detail[index]})
                        elif index < len(real_payment_detail) and index >= len(confis_payment_detail):
                            new_list.append({**real_payment_detail[index]})
                        elif index >= len(real_payment_detail) and index < len(confis_payment_detail):
                            new_list.append({**confis_payment_detail[index]})
                    for j in new_list:
                        item = dict()
                        item['additions'] = list()
                        shareholder_item = dict()
                        shareholder_item["name"] = shareholderscontribution.get("shareholder") \
                            if shareholderscontribution.get("shareholder") else None
                        shareholder_item["type"] = shareholderscontribution.get("type") \
                            if shareholderscontribution.get("type") else None
                        shareholder_item["certificate"] = shareholderscontribution.get("certificate") \
                            if shareholderscontribution.get("certificate") else None
                        shareholder_item["certificate_type"] = shareholderscontribution.get(
                            "certificate_type") \
                            if shareholderscontribution.get("certificate_type") else None
                        shareholder_item["promise_capital"] = shareholderscontribution.get(
                            "promise_capital") \
                            if shareholderscontribution.get("promise_capital") else None
                        shareholder_item["actual_capital"] = shareholderscontribution.get(
                            "actual_capital") \
                            if shareholderscontribution.get("actual_capital") else None
                        if not self.check_items_value(shareholder_item):
                            continue
                        item["capital_id_item"] = shareholder_item
                        i1 = dict()
                        i1["capital_type"] = j.get("promise_capital_type") \
                            if j.get("promise_capital_type") else None
                        i1["capital_publicdate"] = j.get("promise_capital_publicdate") \
                            if j.get("promise_capital_publicdate") else None
                        i1["capital"] = j.get("promise_capital") \
                            if j.get("promise_capital") else None
                        i1["capital_date"] = j.get("promise_capital_date") \
                            if j.get("promise_capital_date") else None
                        i1["capital_enum"] = "promise"
                        item['additions'].append(i1)
                        i = dict()
                        i["capital_type"] = j.get("actual_capital_type") \
                            if j.get("actual_capital_type") else None
                        i["capital_publicdate"] = j.get("actual_capital_publicdate") \
                            if j.get("actual_capital_publicdate") else None
                        i["capital"] = j.get("actual_capital") \
                            if j.get("actual_capital") else None
                        i["capital_date"] = j.get("actual_capital_date") \
                            if j.get("actual_capital_date") else None
                        i["capital_enum"] = "actual"
                        item['additions'].append(i)
                        shareholders_item_list.append(item)

            elif shareholders and shareholder_details:
                check_list = [item.get("shareholder").strip() for item in shareholder_details]
                if len(check_list) == len(list(set(check_list))):
                    shareholder_trash_list = list()
                    for shareholder in shareholders:
                        if shareholder.get("name") in shareholder_trash_list or shareholder.get(
                                "name").strip() == "":
                            continue
                        else:
                            shareholder_trash_list.append(shareholder.get("name"))
                            shareholder_details_trash_list = list()
                            for shareholder_detail in shareholder_details:
                                if shareholder_detail.get("shareholder") in shareholder_details_trash_list or \
                                        shareholder_detail.get("shareholder") == "":
                                    continue
                                else:
                                    shareholder_details_trash_list.append(shareholder_detail.get("shareholder"))
                                    if (shareholder.get("name") != shareholder_detail.get("shareholder")) and (
                                            shareholder.get(
                                                "name") not in [shareholder_detail.get("shareholder") for
                                                                shareholder_detail in
                                                                shareholder_details]):
                                        item = dict()
                                        item['additions'] = list()
                                        shareholderscontribution = {**shareholder}
                                        shareholder_item = dict()
                                        shareholder_item["name"] = shareholderscontribution.get("name") \
                                            if shareholderscontribution.get("name") else None
                                        shareholder_item["type"] = shareholderscontribution.get("type") \
                                            if shareholderscontribution.get("type") else None
                                        shareholder_item["certificate"] = shareholderscontribution.get(
                                            "certificate") \
                                            if shareholderscontribution.get("certificate") else None
                                        shareholder_item["certificate_type"] = shareholderscontribution.get(
                                            "certificate_type") \
                                            if shareholderscontribution.get("certificate_type") else None
                                        shareholder_item["promise_capital"] = shareholderscontribution.get(
                                            "promise_capital") \
                                            if shareholderscontribution.get("promise_capital") else None
                                        shareholder_item["actual_capital"] = shareholderscontribution.get(
                                            "actual_capital") \
                                            if shareholderscontribution.get("actual_capital") else None
                                        if not self.check_items_value(shareholder_item):
                                            continue
                                        item["capital_id_item"] = shareholder_item
                                        shareholders_item_list.append(item)
                                    else:
                                        shareholderscontribution = {**shareholder_detail}
                                        for item in shareholder.items():
                                            if item[1]:
                                                shareholderscontribution[item[0]] = item[1]
                                        real_payment_detail = shareholderscontribution.get(
                                            "real_payment_detail")
                                        confis_payment_detail = shareholderscontribution.get(
                                            "confis_payment_detail")
                                        range_len = len(real_payment_detail) if len(real_payment_detail) >= len(
                                            confis_payment_detail) else len(confis_payment_detail)
                                        new_list = []
                                        for index in range(range_len):
                                            if index < len(real_payment_detail) and index < len(
                                                    confis_payment_detail):
                                                new_list.append(
                                                    {**real_payment_detail[index],
                                                     **confis_payment_detail[index]})
                                            elif index < len(real_payment_detail) and \
                                                    index >= len(confis_payment_detail):
                                                new_list.append({**real_payment_detail[index]})
                                            elif index >= len(real_payment_detail) and index < \
                                                    len(confis_payment_detail):
                                                new_list.append({**confis_payment_detail[index]})
                                        for j in new_list:
                                            item = dict()
                                            item['additions'] = list()
                                            shareholder_item = dict()
                                            shareholder_item["name"] = shareholderscontribution.get(
                                                "shareholder") \
                                                if shareholderscontribution.get(
                                                "shareholder") else None
                                            shareholder_item["type"] = shareholderscontribution.get("type") \
                                                if shareholderscontribution.get("type") else None
                                            shareholder_item["certificate"] = shareholderscontribution.get(
                                                "certificate") \
                                                if shareholderscontribution.get("certificate") else None
                                            shareholder_item["certificate_type"] = shareholderscontribution.get(
                                                "certificate_type") \
                                                if shareholderscontribution.get("certificate_type") else None
                                            shareholder_item["promise_capital"] = shareholderscontribution.get(
                                                "promise_capital") \
                                                if shareholderscontribution.get("promise_capital") else None
                                            shareholder_item["actual_capital"] = shareholderscontribution.get(
                                                "actual_capital") \
                                                if shareholderscontribution.get("actual_capital") else None
                                            if not self.check_items_value(shareholder_item):
                                                continue
                                            item["capital_id_item"] = shareholder_item
                                            i1 = dict()
                                            i1["capital_type"] = j.get("promise_capital_type") \
                                                if j.get("promise_capital_type") else None
                                            i1["capital_publicdate"] = j.get("promise_capital_publicdate") \
                                                if j.get("promise_capital_publicdate") else None
                                            i1["capital"] = j.get("promise_capital") \
                                                if j.get("promise_capital") else None
                                            i1["capital_date"] = j.get("promise_capital_date") \
                                                if j.get("promise_capital_date") else None
                                            i1["capital_enum"] = "promise"
                                            item['additions'].append(i1)
                                            i = dict()
                                            i["capital_type"] = j.get("actual_capital_type") \
                                                if j.get("actual_capital_type") else None
                                            i["capital_publicdate"] = j.get("actual_capital_publicdate") \
                                                if j.get("actual_capital_publicdate") else None
                                            i["capital"] = j.get("actual_capital") \
                                                if j.get("actual_capital") else None
                                            i["capital_date"] = j.get("actual_capital_date") \
                                                if j.get("actual_capital_date") else None
                                            i["capital_enum"] = "actual"
                                            item['additions'].append(i)
                                            shareholders_item_list.append(item)
                else:
                    if shareholders and not shareholder_resp_details:
                        for shareholder in shareholders:
                            item = dict()
                            shareholder_item = dict()
                            shareholderscontribution = {**shareholder}
                            shareholder_item["name"] = shareholderscontribution.get("name") \
                                if shareholderscontribution.get("name") else None
                            shareholder_item["type"] = shareholderscontribution.get("type") \
                                if shareholderscontribution.get("type") else None
                            shareholder_item["certificate"] = shareholderscontribution.get("certificate") \
                                if shareholderscontribution.get("certificate") else None
                            shareholder_item["certificate_type"] = shareholderscontribution.get(
                                "certificate_type") \
                                if shareholderscontribution.get("certificate_type") else None
                            shareholder_item["promise_capital"] = shareholderscontribution.get(
                                "promise_capital") \
                                if shareholderscontribution.get("promise_capital") else None
                            shareholder_item["actual_capital"] = shareholderscontribution.get("actual_capital") \
                                if shareholderscontribution.get("actual_capital") else None
                            if not self.check_items_value(shareholder_item):
                                continue
                            item["capital_id_item"] = shareholder_item
                            shareholders_item_list.append(item)
                    elif shareholders and shareholder_resp_details:
                        count_url = list()
                        for shareholder in shareholders:
                            for shareholder_resp_detail in shareholder_resp_details:
                                shareholder_url = urlparse(shareholder.get("url"))
                                shareholder_resp_detail_url = urlparse(shareholder.get("url"))
                                if shareholder_resp_detail_url.path + shareholder_resp_detail_url.query == \
                                        shareholder_url.path + shareholder_url.query and \
                                        shareholder_resp_detail_url.path + \
                                        shareholder_resp_detail_url.query not in count_url:
                                    count_url.append(
                                        shareholder_resp_detail_url.path + shareholder_resp_detail_url.query)
                                    shareholderscontribution = {**shareholder}
                                    real_payment_detail = shareholder_resp_detail.get("real_payment_detail")
                                    confis_payment_detail = shareholder_resp_detail.get("confis_payment_detail")
                                    range_len = len(real_payment_detail) if len(real_payment_detail) >= len(
                                        confis_payment_detail) else len(confis_payment_detail)
                                    new_list = list()
                                    for index in range(range_len):
                                        if index < len(real_payment_detail) and index < len(
                                                confis_payment_detail):
                                            new_list.append(
                                                {**real_payment_detail[index], **confis_payment_detail[index]})
                                        elif index < len(real_payment_detail) and index >= len(confis_payment_detail):
                                            new_list.append({**real_payment_detail[index]})
                                        elif index >= len(real_payment_detail) and index < len(confis_payment_detail):
                                            new_list.append({**confis_payment_detail[index]})
                                    for j in new_list:
                                        item = dict()
                                        item['additions'] = list()
                                        shareholder_item = dict()
                                        shareholder_item["name"] = shareholderscontribution.get("name") \
                                            if shareholderscontribution.get("name") else None
                                        shareholder_item["type"] = shareholderscontribution.get("type") \
                                            if shareholderscontribution.get("type") else None
                                        shareholder_item["certificate"] = shareholderscontribution.get(
                                            "certificate") \
                                            if shareholderscontribution.get("certificate") else None
                                        shareholder_item["certificate_type"] = shareholderscontribution.get(
                                            "certificate_type") \
                                            if shareholderscontribution.get("certificate_type") else None
                                        shareholder_item["promise_capital"] = shareholderscontribution.get(
                                            "promise_capital") \
                                            if shareholderscontribution.get("promise_capital") else None
                                        shareholder_item["actual_capital"] = shareholderscontribution.get(
                                            "actual_capital") \
                                            if shareholderscontribution.get("actual_capital") else None
                                        if not self.check_items_value(shareholder_item):
                                            continue
                                        item["capital_id_item"] = shareholder_item
                                        i = dict()
                                        i["capital_type"] = j.get("promise_capital_type") \
                                            if j.get("promise_capital_type") else None
                                        i["capital_publicdate"] = j.get("promise_capital_publicdate") \
                                            if j.get("promise_capital_publicdate") else None
                                        i["capital"] = j.get("promise_capital") \
                                            if j.get("promise_capital") else None
                                        i["capital_date"] = j.get("promise_capital_date") \
                                            if j.get("promise_capital_date") else None
                                        i["capital_enum"] = "promise"
                                        item['additions'].append(i)
                                        i1 = dict()
                                        i1["capital_type"] = j.get("actual_capital_type") \
                                            if j.get("actual_capital_type") else None
                                        i1["capital_publicdate"] = j.get("actual_capital_publicdate") \
                                            if j.get("actual_capital_publicdate") else None
                                        i1["capital"] = j.get("actual_capital") \
                                            if j.get("actual_capital") else None
                                        i1["capital_date"] = j.get("actual_capital_date") \
                                            if j.get("actual_capital_date") else None
                                        i1["capital_enum"] = "actual"
                                        item['additions'].append(i1)
                                        shareholders_item_list.append(item)
            shareholders_item["shareholders"] = shareholders_item_list
            result['Shareholder'] = shareholders_item
        if output.get("BranchInformation") is not None:
            item = dict()
            item['additions'] = list()
            for i in output.get("BranchInformation"):
                if i.get("name"):
                    branch_item = dict()
                    branch_item["name"] = i.get("name")
                    item['additions'].append(branch_item)
            result['BranchInformation'].append(item)
        if output.get("Liquidation") is not None:
            liquidations_item = dict()
            liquidations = list()
            liquidations_list = output.get("Liquidation")
            for liquidation in liquidations_list:
                if not (liquidation.get("principal") and liquidation.get("name")):
                    continue
                principal_item = dict()
                principal_item["principal"] = liquidation.get("principal")
                item = dict()
                item['additions'] = list()
                for name in liquidation.get("name"):
                    i = dict()
                    i["name"] = name
                    item['additions'].append(i)
                item["base"] = principal_item
                i_item_dict = {}
                i_item_dict["base"] = dict(item.get("base"))
                i_item_dict["additions"] = [dict(i) for i in item.get("additions")] if item.get(
                    "additions") else [{"name": ""}]
                principal_item["hash_info"] = i_item_dict
                item["base"] = principal_item
                liquidations.append(item)
            if liquidations:
                liquidations_item["liquidations"] = liquidations
            else:
                liquidations_item["liquidations"] = list()
            result['Liquidation'] = liquidations_item
        if output.get("Changerecord") is not None:
            list_item = dict()
            new_list = list()
            for i in output.get("Changerecord"):
                item = dict()
                item["changetype"] = i.get("changetype") if i.get("changetype") else None
                item["before"] = i.get("before") if i.get("before") else None
                item["after"] = i.get("after") if i.get("after") else None
                item["changedate"] = i.get("changedate") if i.get("changedate") else None
                if not self.check_items_value(item):
                    continue
                new_list.append(item)
            list_item["changerecords"] = new_list
            result['Changerecord'] = list_item
        if output.get("AdministrativeLicensing") is not None:
            for i in output.get("AdministrativeLicensing"):
                item = dict()
                item["license_no"] = i.get("license_no") if i.get("license_no") else None
                item["license_name"] = i.get("license_name") if i.get("license_name") else None
                item["valid_from"] = i.get("valid_from") if i.get("valid_from") else None
                item["valid_until"] = i.get("valid_until") if i.get("valid_until") else None
                item["organ"] = i.get("organ") if i.get("organ") else None
                item["content"] = i.get("content") if i.get("content") else None
                result['AdministrativeLicensing'].append(item)
        if output.get("AdministrativePenalty") is not None:
            for i in output.get("AdministrativePenalty"):
                item = dict()
                item["document_number"] = i.get("document_number") if i.get("document_number") else None
                item["type"] = i.get("type") if i.get("type") else None
                item["content"] = i.get("content") if i.get("content") else None
                item["organ"] = i.get("organ") if i.get("organ") else None
                item["decision_date"] = i.get("decision_date") if i.get("decision_date") else None
                item["public_date"] = i.get("public_date") if i.get("public_date") else None
                result['AdministrativePenalty'].append(item)
        if output.get("ExceptionInformation") is not None:
            for i in output.get("ExceptionInformation"):
                item = dict()
                item["entry_reason"] = i.get("entry_reason") if i.get("entry_reason") else None
                item["entry_date"] = i.get("entry_date") if i.get("entry_date") else None
                item["entry_organ"] = i.get("entry_organ") if i.get("entry_organ") else None
                item["removal_reason"] = i.get("removal_reason") if i.get("removal_reason") else None
                item["removal_date"] = i.get("removal_date") if i.get("removal_date") else None
                item["removal_organ"] = i.get("removal_organ") if i.get("removal_organ") else None
                result['ExceptionInformation'].append(item)
        if output.get("IllegalInformation") is not None:
            for i in output.get("IllegalInformation"):
                item = dict()
                item["entry_reason"] = i.get("entry_reason") if i.get("entry_reason") else None
                item["entry_date"] = i.get("entry_date") if i.get("entry_date") else None
                item["entry_organ"] = i.get("entry_organ") if i.get("entry_organ") else None
                item["removal_reason"] = i.get("removal_reason") if i.get("removal_reason") else None
                item["removal_date"] = i.get("removal_date") if i.get("removal_date") else None
                item["removal_organ"] = i.get("removal_organ") if i.get("removal_organ") else None
                result['IllegalInformation'].append(item)
        if output.get("CheckInformation") is not None:
            for i in output.get("CheckInformation"):
                item = dict()
                item["organ"] = i.get("organ") if i.get("organ") else None
                item["type"] = i.get("type") if i.get("type") else None
                item["date"] = i.get("date") if i.get("date") else None
                item["result"] = i.get("result") if i.get("result") else None
                result['CheckInformation'].append(item)
        if output.get("JudicialInformation") is not None:
            for i in output.get("JudicialInformation"):
                item = dict()
                item["executed_legal_person"] = i.get("executed_legal_person") if i.get(
                    "executed_legal_person") else None
                item["amount_of_equity"] = i.get("amount_of_equity") if i.get("amount_of_equity") else None
                item["court"] = i.get("court") if i.get("court") else None
                item["document_number"] = i.get("document_number") if i.get("document_number") else None
                item["status"] = i.get("status") if i.get("status") else None
                result['JudicialInformation'].append(item)
        if output.get("ChattelMortgage") is not None:
            for i in output.get("ChattelMortgage"):
                item = dict()
                item["registration_number"] = i.get("registration_number") if i.get(
                    "registration_number") else None
                item["registration_date"] = i.get("registration_date") if i.get("registration_date") else None
                item["registration_organ"] = i.get("registration_organ") if i.get("registration_organ") else None
                item["amount_of_secured_claims"] = i.get("amount_of_secured_claims") if i.get(
                    "amount_of_secured_claims") else None
                item["status"] = i.get("status") if i.get("status") else None
                item["public_date"] = i.get("public_date") if i.get("public_date") else None
                result['ChattelMortgage'].append(item)
        if output.get("EquityPledge") is not None:
            for i in output.get("EquityPledge"):
                item = dict()
                item["registration_number"] = i.get("registration_number") if i.get(
                    "registration_number") else None
                item["pledgor"] = i.get("pledgor") if i.get("pledgor") else None
                item["pledgor_id"] = i.get("pledgor_id") if i.get("pledgor_id") else None
                item["amount_of_equity"] = i.get("amount_of_equity") if i.get("amount_of_equity") else None
                item["pledgee"] = i.get("pledgee") if i.get("pledgee") else None
                item["registration_date"] = i.get("registration_date") if i.get("registration_date") else None
                status = ""
                if i.get("status") == "1":
                    status = "有效"
                elif i.get("status") == "2":
                    status = "无效"
                item["status"] = status
                item["public_date"] = i.get("public_date") if i.get("public_date") else None
                result['EquityPledge'].append(item)
        if output.get("MainPersonnel") is not None:
            employees_item = dict()
            employees_item_list = list()
            for i in output.get("MainPersonnel"):
                item = dict()
                item["name"] = i.get("name") if i.get("name") else None
                item["title"] = i.get("title") if i.get("title") else None
                item["certificate"] = i.get("certificate") if i.get("certificate") else None
                item["certificate_type"] = i.get("certificate_type") if i.get("certificate_type") else None
                if not self.check_items_value(item):
                    continue
                employees_item_list.append(item)
            employees_item["employees"] = employees_item_list
            result['MainPersonnel'].append(employees_item)
        if output.get('Annual') is not None:
            result['Annual'] = output['Annual']
        return result












