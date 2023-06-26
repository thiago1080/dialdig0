import configparser
from ruamel.yaml import YAML
import yaml
import hashlib
import os

CRED_FILE = 'credentials_anon'

def set_aws_credentials(credentials):
    os.environ['AWS_ACCESS_KEY_ID'] = credentials['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = credentials['aws_secret_access_key']
    os.environ['AWS_SESSION_TOKEN'] = credentials['aws_session_token']

def txt2dict(path):
    """
    Reads a .txt file containing credentials and converts it to a dictionary.

    :param path: str
        Path to the .txt file containing the credentials.
    :return: dict
        Dictionary containing the credentials.
    """
    # Read the credentials.txt file
    config = configparser.ConfigParser()
    config.read(path)

    profile_name = config.sections()[0]
    keys = config.options(profile_name)
    data = {}
    for key in keys:
        data[key] = config.get(profile_name, key)
    return data


def dump_yml(data, path=None):
    """
    Dumps a dictionary to a .yml file.

    :param data: dict
        Dictionary containing the data to be dumped.
    :param path: str, optional
        Path to the .yml file where the data will be dumped. If not provided,
        the data will be dumped to './dumped_yml'.
    """
    yml = YAML()
    yml.indent(mapping=2)
    if path is None:
        path = './dumped_yml'
    with open(path, 'w') as outfile:
        yml.dump(data, outfile)


def read_yml(path):
    """
    Reads a .yml file and returns its contents as a dictionary.

    :param path: str
        Path to the .yml file to be read.
    :return: dict
        Dictionary containing the data from the .yml file.
    """
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    return data


def anom_vals(dic):
    """
    Anonymizes the values in a dictionary by replacing them with their SHA-256 hash.

    :param dic: dict
        Dictionary containing the values to be anonymized.
    :return: dict
        Dictionary with the same keys as the input dictionary and with the values replaced by their SHA-256 hash.
    """
    return {k: hashlib.sha256(str(v).encode()).hexdigest() for k, v in dic.items()}