import configparser


def load_config(path):
    config = configparser.ConfigParser()
    config.read(path, encoding='UTF-8')
    return config
