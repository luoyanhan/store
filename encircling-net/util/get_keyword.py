import os
import sys


import requests


basedir = os.path.abspath(os.path.dirname(__file__))


def load_config(path):
    import configparser
    config = configparser.ConfigParser()
    config.read(path)
    return config


config = load_config(basedir.replace(r'\util', '') + '\configs\GSXT_PC_config.ini')
API = config['GETKeyword']['API']


def getKeyword(firm):
    keynoMapping_url = API + '/keynomappings/list/firm/{}?page=1&per_page=10'.format(firm)
    resp = requests.get(keynoMapping_url)
    keyno = resp.json()['data'][0]['keyno']
    baseinfo_url = API + '/baseinfos/keyno/{}'.format(keyno)
    resp = requests.get(baseinfo_url)
    creditcode = resp.json()['creditcode']
    return (creditcode, firm)





