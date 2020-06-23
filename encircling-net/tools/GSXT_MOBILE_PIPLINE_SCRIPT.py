import os
import sys


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))


from util.load_config import load_config


if __name__ == "__main__":
    mode = sys.argv[1]
    config_name = sys.argv[2]
    if mode == 'DETAIL_MYSQL':
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', config_name]))
        COHERSOUP_DIR = config['COHERSOUP']['DIR']
        sys.path.append(COHERSOUP_DIR)
        from pipline.Discover_Detail_Mysql import detail_run
        detail_run(config_name)       #GSXT_MOBILE_DETAIL_config.ini or GSXT_MOBILE_ANNUAL_config.ini
    elif mode == 'SEARCH_MYSQL':
        config = load_config(basedir.replace(os.sep + 'tools', '') + os.sep + os.sep.join(['configs', config_name]))
        COHERSOUP_DIR = config['COHERSOUP']['DIR']
        sys.path.append(COHERSOUP_DIR)
        from pipline.Discover_Detail_Mysql import search_run
        search_run(config_name)      #GSXT_MOBILE_DISCOVER_config.ini
    elif mode == 'DETAIL':
        from pipline.Discover_Detail_Pipline import add_to_mongo
        add_to_mongo(config_name)    # GSXT_MOBILE_DETAIL_config.ini or GSXT_MOBILE_ANNUAL_config.ini
    elif mode == 'SEARCH':
        from pipline.Discover_Search_Pipline import add_to_mongo
        add_to_mongo(config_name)    # GSXT_MOBILE_DISCOVER_config.ini