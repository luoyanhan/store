import os
import sys


import redis
import ujson


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))

from util.load_config import load_config


if __name__ == "__main__":
    if len(sys.argv[1:]) % 2 != 0:
        print('输入参数数目错误')
        raise Exception
    for each in range(1, len(sys.argv), 2):
        mode = sys.argv[each]
        config_name = sys.argv[each+1]
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep +
                             os.sep.join(['configs', config_name]))
        if mode == 'DISCOVER':
            REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
            REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
            REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
            REDIS_DAWG_LIST_NAME = config['REDIS_TAG']['REDIS_DAWG_LIST_NAME']
            DISCOVER_MONGODB_COLLECTIONS = ujson.loads(config['MONGO']['MONGODB_COLLECTIONS'])
            NAME = config['MYSQL_PIPLINE']['NAME']
            dawg_redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
            if dawg_redis_conn.exists(REDIS_DAWG_LIST_NAME):
                dawg_redis_conn.delete(REDIS_DAWG_LIST_NAME)
            for each in DISCOVER_MONGODB_COLLECTIONS:
                HOST = each['host']
                PORT = each['port']
                DB = each['db']
                COLLECTION = each['collection']
                USER = each['user']
                PASS = each['password']
                statue_redis_tag = '/'.join([HOST, str(PORT), DB, COLLECTION, NAME])
                if dawg_redis_conn.exists(statue_redis_tag):
                    dawg_redis_conn.delete(statue_redis_tag)
        elif mode == 'UPDATE_DETAIL':
            REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
            REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
            REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
            redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
            UPDATE_SRART_TIME_TAG = config['UPDATE_GENERATOR']['UPDATE_SRART_TIME_TAG']
            UPDATE_ROUND_START_TIME_TAG = config['UPDATE_GENERATOR']['UPDATE_ROUND_START_TIME_TAG']
            if redis_conn.exists(UPDATE_SRART_TIME_TAG):
                redis_conn.delete(UPDATE_SRART_TIME_TAG)
            if redis_conn.exists(UPDATE_ROUND_START_TIME_TAG):
                redis_conn.delete(UPDATE_ROUND_START_TIME_TAG)
            MONGODB_COLLECTIONS = ujson.loads(config['MONGO']['MONGODB_COLLECTIONS'])
            NAME = config['MYSQL_PIPLINE']['NAME']
            for each in MONGODB_COLLECTIONS:
                HOST = each['host']
                PORT = each['port']
                DB = each['db']
                COLLECTION = each['collection']
                USER = each['user']
                PASS = each['password']
                statue_redis_tag = '/'.join([HOST, str(PORT), DB, COLLECTION, NAME])
                if redis_conn.exists(statue_redis_tag):
                    redis_conn.delete(statue_redis_tag)
        elif mode == 'UPDATE_ANNUAL':
            REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
            REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
            REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
            redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
            ANNUAL_SRART_TIME_TAG = config['UPDATE_GENERATOR']['ANNUAL_SRART_TIME_TAG']
            ANNUAL_ROUND_START_TYPE_TAG = config['UPDATE_GENERATOR']['ANNUAL_ROUND_START_TYPE_TAG']
            if redis_conn.exists(ANNUAL_SRART_TIME_TAG):
                redis_conn.delete(ANNUAL_SRART_TIME_TAG)
            if redis_conn.exists(ANNUAL_ROUND_START_TYPE_TAG):
                redis_conn.delete(ANNUAL_ROUND_START_TYPE_TAG)
            MONGODB_COLLECTIONS = ujson.loads(config['MONGO']['MONGODB_COLLECTIONS'])
            NAME = config['MYSQL_PIPLINE']['NAME']
            for each in MONGODB_COLLECTIONS:
                HOST = each['host']
                PORT = each['port']
                DB = each['db']
                COLLECTION = each['collection']
                USER = each['user']
                PASS = each['password']
                statue_redis_tag = '/'.join([HOST, str(PORT), DB, COLLECTION, NAME])
                if redis_conn.exists(statue_redis_tag):
                    redis_conn.delete(statue_redis_tag)





