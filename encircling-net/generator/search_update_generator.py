import os
import sys


import redis
import sqlalchemy.orm.session
from sqlalchemy import and_


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'generator', ''))
sys.path.append(basedir)


from util.load_config import load_config


from manage import app_shell
app_shell('dev')
from app.main.blueprint.production.model.production import Baseinfo


def creditcode_producer(creditcode_queue, config_name):
    config = load_config(basedir.replace(os.sep + 'generator', '') + os.sep + os.sep.join(['configs', config_name]))
    REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
    REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
    REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
    UPDATE_SRART_ID_TAG = config['UPDATE_GENERATOR']['UPDATE_SRART_ID_TAG']
    ADD_TO_QUEUE_EVERYTIME = int(config['UPDATE_GENERATOR']['ADD_TO_QUEUE_EVERYTIME'])
    redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
    session = sqlalchemy.orm.session.Session(bind=Baseinfo.__engine__[Baseinfo._node])
    while True:
        start_id = redis_conn.get(UPDATE_SRART_ID_TAG)
        if start_id:
            start_id = int(start_id.decode())
            creditcodes = session.query(Baseinfo.id, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm).\
                filter(and_(Baseinfo.id > start_id, Baseinfo.creditcode.like('91%'))).\
                order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
        else:
            creditcodes = session.query(Baseinfo.id, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm).\
                filter(Baseinfo.creditcode.like('91%')).order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
        if not len(creditcodes):
            redis_conn.delete(UPDATE_SRART_ID_TAG)
            continue
        else:
            last_collect_id = creditcodes[-1][0]
            redis_conn.set(UPDATE_SRART_ID_TAG, last_collect_id)
        for creditcode in creditcodes:
            if creditcode[2]:
                creditcode_queue.put(creditcode[2], block=True, timeout=None)
            else:
                creditcode_queue.put(creditcode[3], block=True, timeout=None)




