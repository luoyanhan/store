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
from app.main.blueprint.production.model.annual_che_year import AnnualCheYear
from app.main.blueprint.production.model.resource import ResourceAttribute
from app.main.blueprint.production.service import resource as ResourceService


def creditcode_producer(creditcode_queue, config_name):
    config = load_config(basedir.replace(os.sep + 'generator', '') + os.sep +
                         os.sep.join(['configs', config_name]))
    REDIS_TAG_HOST = config['REDIS_TAG']['REDIS_TAG_HOST']
    REDIS_TAG_PORT = int(config['REDIS_TAG']['REDIS_TAG_PORT'])
    REDIS_TAG_DB = int(config['REDIS_TAG']['REDIS_TAG_DB'])
    ANNUAL_SRART_ID_TAG = config['UPDATE_GENERATOR']['ANNUAL_SRART_ID_TAG']
    ANNUAL_ROUND_START_TYPE_TAG = config['UPDATE_GENERATOR']['ANNUAL_ROUND_START_TYPE_TAG']
    ADD_TO_QUEUE_EVERYTIME = int(config['UPDATE_GENERATOR']['ADD_TO_QUEUE_EVERYTIME'])
    redis_conn = redis.StrictRedis(REDIS_TAG_HOST, REDIS_TAG_PORT, REDIS_TAG_DB)
    session = sqlalchemy.orm.session.Session(bind=Baseinfo.__engine__[Baseinfo._node])
    while True:
        start_id = redis_conn.get(ANNUAL_SRART_ID_TAG)
        round_start_type = redis_conn.get(ANNUAL_ROUND_START_TYPE_TAG)
        if not round_start_type:
            round_start_type = 'replenish_annual'
            redis_conn.set(ANNUAL_ROUND_START_TYPE_TAG, round_start_type)
        else:
            round_start_type = round_start_type.decode()
        annual_rescid = ResourceAttribute.get_resc_id_by_model(AnnualCheYear)
        keynos_with_record = ResourceService.get_keynos_by_rescid(annual_rescid)
        if round_start_type == 'replenish_annual':
            if start_id:
                start_id = int(start_id.decode())
                creditcodes = session.query(Baseinfo.id, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm). \
                            filter(and_(Baseinfo.keyno.notin_(keynos_with_record),
                                        Baseinfo.id > start_id,
                                        Baseinfo.creditcode.like('91%')
                                        )).order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
            else:
                creditcodes = session.query(Baseinfo.id, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm).\
                    filter(and_(Baseinfo.keyno.notin_(keynos_with_record),
                                Baseinfo.creditcode.like('91%')
                                )).order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
        elif round_start_type == 'update_annual':
            if start_id:
                start_id = int(start_id.decode())
                creditcodes = session.query(Baseinfo.collect_time, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm).\
                    filter(and_(Baseinfo.keyno.in_(keynos_with_record),
                                Baseinfo.id > start_id,
                                Baseinfo.creditcode.like('91%')
                                )).order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
            else:
                creditcodes = session.query(Baseinfo.id, Baseinfo.keyno, Baseinfo.creditcode, Baseinfo.firm). \
                    filter(and_(Baseinfo.keyno.in_(keynos_with_record),
                                Baseinfo.creditcode.like('91%')
                                )).order_by(Baseinfo.id.asc()).limit(ADD_TO_QUEUE_EVERYTIME).all()
        if len(creditcodes) > 0:
            last_collect_id = creditcodes[-1][0]
            redis_conn.set(ANNUAL_SRART_ID_TAG, last_collect_id)
        else:
            redis_conn.delete(ANNUAL_SRART_ID_TAG)
            if round_start_type == 'replenish_annual':
                redis_conn.set(ANNUAL_ROUND_START_TYPE_TAG, 'update_annual')
            else:
                redis_conn.set(ANNUAL_ROUND_START_TYPE_TAG, 'replenish_annual')
        for creditcode in creditcodes:
            if creditcode[2]:
                creditcode_queue.put(creditcode[2], block=True, timeout=None)
            else:
                creditcode_queue.put(creditcode[3], block=True, timeout=None)


