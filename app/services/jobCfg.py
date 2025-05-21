from myConfig import MyConfig


# ---------------------------------------------------------------------------
# 定时任务处理器触发类型设置
# ---------------------------------------------------------------------------
#
class JobTriggerType(object):
    """
    triggerType: 触发器类型。Blocking--阻塞型；Background--后台型；
    """
    def __init__(self):
        #: Blocking--阻塞型
        self.BLOCKING = "Blocking"
        #: Background--后台型
        self.BACKGROUND = "Background"


# ---------------------------------------------------------------------------
# 定时任务处理器操作类型设置
# ---------------------------------------------------------------------------
#
class SchedulerOptType(object):
    """
    scheduler operate type:
    """
    def __init__(self):
        #:
        self.START = "start"
        self.STOP = "stop"
        self.STOP_NOW = "stop_now"
        self.RESUME = "resume"
        self.PAUSE = "pause"


# ---------------------------------------------------------------------------
# 定时任务操作类型设置
# ---------------------------------------------------------------------------
#
class JobOptType(object):
    """
    Job operate type:
    """
    def __init__(self):
        #:
        self.REMOVE = "remove"
        self.RESUME = "resume"
        self.PAUSE = "pause"
        self.START_NOW = "start-now"


# ---------------------------------------------------------------------------
# Job配置文件读取
# ---------------------------------------------------------------------------
#
class JobCfg(object):
    """

    """
    def __init__(self, cfg_filename):
        """

        :param cfg_filename:
        """
        self.cfg = MyConfig(cfg_filename).config
        self.log_filename = self.cfg["log-filename"]
        self.auth_cfg_filename = self.cfg["auth-cfg-filename"]
        self.requests_cfg_filename = self.cfg["requests-cfg-filename"]
        self.servers_cfg_filename = self.cfg["servers-cfg-filename"]
        self.apis_cfg_filename = self.cfg["apis-cfg-filename"]
        self.scheduler_cfg_filename = self.cfg["scheduler-cfg-filename"]


# ---------------------------------------------------------------------------
# 定时任务配置文件读取
# ---------------------------------------------------------------------------
#
class SchedulerCfg(object):
    """
    定时任务配置文件读取
    """
    def __init__(self, cfg_filename):
        """
        初始化配置文件信息:
        :param cfg_filename: 定时任务配置文件名称，默认为scheduler.json；
        :return:
        """
        self.cfg = MyConfig(cfg_filename).config
        self.cfg_file = MyConfig(cfg_filename).cfgFile
        self.log_file = self.cfg['log-filename']
        self.redis_name = self.cfg['redis-name']
        self.trigger_type = self.cfg['trigger-type']
        self.timezone = self.cfg['timezone']
