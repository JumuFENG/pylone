import os
import sys
import logging
import json
import base64
import random
from functools import lru_cache


class Config:
    app_name = 'pyswee' # app name for logging
    @classmethod
    @lru_cache(maxsize=1)
    def _cfg_path(cls):
        cpth = os.path.join(os.path.dirname(__file__), '../config/config.json')
        if not os.path.isdir(os.path.dirname(cpth)):
            os.mkdir(os.path.dirname(cpth))
        return cpth

    @classmethod
    def save(cls, cfg):
        cfg_path = cls._cfg_path()
        with open(cfg_path, 'w') as f:
            json.dump(cfg, f, indent=4)

    @classmethod
    def simple_encrypt(cls, txt):
        r = random.randint(1, 5)
        x = base64.b64encode(txt.encode('utf-8'))
        for i in range(r):
            x = base64.b64encode(x)
        return '*'*r + x.decode('utf-8')

    @classmethod
    def simple_decrypt(cls, etxt):
        r = etxt.rfind('*')
        if r == -1:
            return etxt
        etxt = etxt[r:]
        x = base64.b64decode(etxt.encode('utf-8'))
        for i in range(r+1):
            x = base64.b64decode(x)
        return x.decode('utf-8')

    @classmethod
    @lru_cache(maxsize=1)
    def _lg_path(cls):
        lgpath = os.path.join(os.path.dirname(__file__), f'../logs/{cls.app_name}.log')
        if not os.path.isdir(os.path.dirname(lgpath)):
            os.mkdir(os.path.dirname(lgpath))
        return lgpath

    @classmethod
    def log_level(cls):
        lvl = cls.all_configs().get("log", {}).get("log_level", "INFO").upper()
        return logging._nameToLevel[lvl]

    @classmethod
    def log_handler(cls):
        handlers = cls.all_configs().get("log", {}).get('log_handler', ['file', 'stdout'])
        lhandlers = []
        if 'file' in handlers:
            lhandlers.append(logging.FileHandler(cls._lg_path()))
        if any(x in handlers for x in ['stdout', 'console']):
            lhandlers.append(logging.StreamHandler(sys.stdout))
        return lhandlers

    @classmethod
    def _check_encrypted(cls, cfg):
        encrypted = False
        for k, v in cfg.items():
            if isinstance(v, dict):
                encrypted |= cls._check_encrypted(v)
            elif k == 'password' and not v.startswith('*'):
                cfg[k] = cls.simple_encrypt(v)
                encrypted = True

        return encrypted

    @classmethod
    @lru_cache(maxsize=None)
    def all_configs(cls):
        cfg_path = cls._cfg_path()
        allconfigs = None
        if not os.path.isfile(cfg_path):
            allconfigs = {}
            allconfigs['log'] = {'log_level':'DEBUG', 'log_handler': ['stdout']}
            allconfigs['client'] = {'app_name': cls.app_name}
            cls.save(allconfigs)
            return allconfigs

        with open(cfg_path, 'r') as f:
            allconfigs = json.load(f)

        if cls._check_encrypted(allconfigs):
            cls.save(allconfigs)

        return allconfigs

    @classmethod
    def client_config(cls):
        return cls.all_configs().get('client', {})


logging.basicConfig(
    level=Config.log_level(),
    format='%(levelname)s | %(asctime)s-%(filename)s@%(lineno)d<%(name)s> %(message)s',
    handlers=Config.log_handler(),
    force=True
)

logger : logging.Logger = logging.getLogger(Config.app_name)
