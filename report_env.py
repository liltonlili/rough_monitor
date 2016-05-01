import ConfigParser
import io

__author__ = 'root'

report_env = 'dev'
config = None

def init_config(file_path):
    global config
    config = ConfigParser.ConfigParser()
    config.readfp(io.open(file_path, encoding="utf8"))


def get_prop(prop_name):
    global config
    return config.get(report_env, prop_name)


def get_config():
    global config
    return config