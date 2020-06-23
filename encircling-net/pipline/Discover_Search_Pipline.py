import os
import sys

import ujson
import asyncio
import redis
from kafka import KafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'pipline', ''))
sys.path.append(basedir)


from util.load_config import load_config


def add_to_mongo(config_name):
    config = load_config(basedir.replace(os.sep + 'pipline', '') + os.sep +
                         os.sep.join(['configs', config_name]))
    MONGODB_SERVER = config['MONGO']['MONGODB_SERVER']
    MONGODB_PORT = int(config['MONGO']['MONGODB_PORT'])
    MONGODB_DB = config['MONGO']['MONGODB_DB']
    MONGODB_COLLECTION = config['MONGO']['MONGODB_COLLECTION']
    MONGODB_USER = config['MONGO']['MONGODB_USER']
    MONGODB_PASSWORD = config['MONGO']['MONGODB_PASSWORD']
    KAFKA_BOOTSTRAP_SERVERS = config['KAFKA']['KAFKA_BOOTSTRAP_SERVERS']
    KAFKA_TOPIC = config['KAFKA']['KAFKA_TOPIC']
    KAFKA_GROUP = config['KAFKA']['KAFKA_GROUP']
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                             group_id=KAFKA_GROUP)
    client = AsyncIOMotorClient('mongodb://' + MONGODB_USER + ':' + MONGODB_PASSWORD + '@' + MONGODB_SERVER + ':'
                                + str(MONGODB_PORT))
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    results = consumer
    for result in results:
        msg = result.value.decode()
        item = ujson.loads(msg)
        key = list(item.keys())[0]
        value = item[key]
        target = {
            'keyword': str(key).strip(),
            'info_resp': value
        }
        async def do_update():
            result = await collection.update_many(
                {'keyword': str(key).strip()},
                {'$set': target}, upsert=True)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(do_update())
