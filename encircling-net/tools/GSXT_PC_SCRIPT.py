import sys
import os


basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(basedir.replace(os.sep + 'tools', ''))


from downloader.GSXT_PC import Downloader


if __name__ == "__main__":
    firm = sys.argv[1]
    downloader = Downloader()
    result_json = downloader.run(firm)
    print(result_json)