import os
import sys
import queue
import threading

from flask import Flask
from flask_restplus import Resource, Api
from gevent.pywsgi import WSGIServer


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))


from util.load_config import load_config


app = Flask(__name__)
api = Api(app)
__all__ = ['app']


if __name__ == "__main__":
    mode = sys.argv[1]
    config_name = sys.argv[2]
    config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep +
                         os.sep.join(['configs', config_name]))
    QUEUE_SIZE = int(config['UPDATE_GENERATOR']['QUEUE_SIZE'])
    COHERSOUP_DIR = config['COHERSOUP']['DIR']
    sys.path.append(COHERSOUP_DIR)
    creditcode_queue = queue.Queue(QUEUE_SIZE)
    if mode == 'DISCOVER':
        pass
        # from generator.detail_generator import http_server
        # http_server.serve_forever()
    elif mode == 'UPDATE':
        from generator.search_update_generator import creditcode_producer
        # GSXT_MOBILE_DETAIL_config.ini
    elif mode == 'UPDATE_ANNUAL':
        # GSXT_MOBILE_ANNUAL_config.ini
        from generator.search_update_annual_generator import creditcode_producer
    elif mode == 'THREE_SECOND':
        # GSXT_MOBILE_ANNUAL_THREE_SECOND_config.ini
        from generator.three_second_update_annual import creditcode_producer

    threading.Thread(target=creditcode_producer, args=(creditcode_queue, config_name)).start()

    @api.route('/detail/get')
    class Get(Resource):
        def get(self):
            return creditcode_queue.get()


    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()