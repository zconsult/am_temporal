import sys
import yaml

from .import data as data_module
from importlib.resources import files


CONFIG_FILE_NAME = "config.yaml"


def get_config_part(key: str):
    return __conf[key]


def __read_config() -> dict:
    s = __reader(CONFIG_FILE_NAME)
    my_dict = yaml.safe_load(s)
    return my_dict


def __reader(resource_file_name: str) -> str:
    """
    Internal data aka "resource" usage.
    This function simply reads our *package internal* (yaml) file and returns the entire content as a string

    :param resource_file_name: internal yaml file name
    :return: file content as string
    """
    return files(data_module).joinpath(resource_file_name).read_text()


__conf = __read_config()
