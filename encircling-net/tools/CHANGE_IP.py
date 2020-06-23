import requests
import time
import os
import sys
import json

basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))

from util.load_config import load_config

if __name__ == "__main__":
    config_name = sys.argv[1]
    config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep +
                         os.sep.join(['configs', config_name]))
    sleep_time = float(config['IP']['SLEEP'])
    with open(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', 'source.json']), 'r') \
            as f:
        proxies_list = json.load(f)
    while True:
        for proxy in proxies_list:
            resp = requests.get('http://proxy.abuyun.com/switch-ip', proxies=proxy)
            print(resp.text)
            print('#########################')
            time.sleep(sleep_time)