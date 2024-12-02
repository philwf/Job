from fastapi import FastAPI
from myConfig import MyConfig
from myLogger import MyLogger
from .services.jobCfg import JobCfg

# ---------------------------------------------------------------------------
# 加载job的配置文件
# ---------------------------------------------------------------------------
#
try:
    default_config = MyConfig().config
    if 'job' in default_config:
        default_filename = default_config['job']
    else:
        default_filename = 'job.json'
except ValueError:
    default_filename = 'job.json'

job_cfg = JobCfg(default_filename)

# ---------------------------------------------------------------------------
# 初始化job的全局日志
# ---------------------------------------------------------------------------
#
log = MyLogger()
log.initLogger(job_cfg.log_filename)

# ---------------------------------------------------------------------------
# 初始化app
# ---------------------------------------------------------------------------
#
log.debug('------------Initiate app...------------')
app = FastAPI()

# ---------------------------------------------------------------------------
# 加载job相关api
# ---------------------------------------------------------------------------
#
from .apis.jobAPI import JobAPI
job_api = JobAPI()
job_api.setup_routes(app)