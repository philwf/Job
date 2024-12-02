from ApiAuth import ApiAuth
from .. import MyConfig, job_cfg, log

api_auth = ApiAuth(MyConfig(job_cfg.auth_cfg_filename).config)
