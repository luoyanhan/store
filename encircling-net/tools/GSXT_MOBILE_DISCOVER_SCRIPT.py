import os
import sys


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))


from downloader.GSXT_MOBILE import DownloadItem as Mobile_Item
from downloader.GSXT_DISCOVER import DownloadItem as Discover_Item
from util.load_config import load_config


if __name__ == "__main__":
    mode = sys.argv[1]
    config_name = sys.argv[2]
    if mode == 'DISCOVER':
        # GSXT_MOBILE_DISCOVER_config.ini
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', config_name]))
        downloader = Discover_Item(config)
        downloader.do_batch()
    elif mode == 'DETAIL':
        # GSXT_MOBILE_DETAIL_config.ini
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', config_name]))
        DETAIL_NEED = config['DETAIL_NEED']
        detail_types = list()
        for each in DETAIL_NEED:
            if config.getboolean('DETAIL_NEED', each):
                detail_types.append(each)
        downloader = Mobile_Item(config, detail_types)
        downloader.do_batch()
    elif mode == 'ANNUAL':
        # GSXT_MOBILE_ANNUAL_config.ini
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', config_name]))
        ANNUAL_NEED = config['ANNUAL_NEED']
        detail_types = list()
        for each in ANNUAL_NEED:
            if config.getboolean('ANNUAL_NEED', each):
                detail_types.append(each)
        downloader = Mobile_Item(config, detail_types)
        downloader.do_batch()