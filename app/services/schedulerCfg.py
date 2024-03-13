import myConfig as mc
import schedulerCron as sc
import myFileUtil as fu


# ---------------------------------------------------------------------------
# 定时任务配置文件读取
# ---------------------------------------------------------------------------
#
class SchedulerCfg(object):
    """
    定时任务配置文件读取
    """
    def __init__(self, configFile=None):
        """
        初始化配置文件信息:
        :param configFile: 定时任务配置文件名称，默认为scheduler.json；
        :return:
        """
        if not configFile:
            configFile = 'scheduler.json'
        self.cfgIns = mc.MyConfig(configFile)
        self.sCfg = self.cfgIns.config
        self.jobsCfg = JobsCfg(self.sCfg)
        self.logFile = self.sCfg['log-file']
        self.processFilePath = self.sCfg['process-file-path']
        # 开关，0-关闭，1-开启，2-暂停
        self.switch = checkSwitch(self.sCfg['switch'])
        self.redisName = self.sCfg['redis-name']


# ---------------------------------------------------------------------------
# 定时任务配置文件中Jobs内容读取
# ---------------------------------------------------------------------------
#
class JobsCfg(object):
    """
    定时任务配置文件Job内容读取
    """
    def __init__(self, cfg):
        """
        初始化配置文件中job信息:
        :param cfg: 配置文件内容
        :return:
        """
        self.jobs = cfg['jobs']
        self._getJobField()

    def _getJobField(self):
        """
        读取job节点下数据字典中的值。并按name作为关键值定义多个字典
            - jobNames: 保存job节点下所有name的列表；
            - jobCron: cron参数的字典。key为name，value为对应的cron参数字典；
            - jobCronStr: cron参数的字符串。key为name，value为对应的cron参数字符串；
            - jobFunc: job回调函数字符串的字典。key为name，value为对应的回调函数字符串；
            - jobSwitch: 开关参数的字典。key为name，value为对应的开关；
        :return:
        """
        self.jobNames = []
        self.jobDescNames = {}
        self.jobCron = {}
        self.jobCronStr = {}
        self.jobFunc = {}
        self.jobSwitch = {}
        self.jobApiCall = {}
        if self.jobs:
            for job in self.jobs:
                name = job['name']
                self.jobNames.append(job['name'])
                self.jobDescNames[name] = job['descName']
                self.jobCronStr[name] = job['cron']
                self.jobCron[name] = sc.CronFiled(job['cron']).cron
                self.jobFunc[name] = job['func']
                # 开关，0-关闭，1-开启，2-暂停
                self.jobSwitch[name] = checkSwitch(job['switch'])
                if 'ApiCall' in job:
                    self.jobApiCall = job['ApiCall']


# ---------------------------------------------------------------------------
# 定时任务配置文件修改
# ---------------------------------------------------------------------------
#
class SchedulerCfgMdf(object):
    """
    定时任务配置文件修改
    """
    def __init__(self):
        """
        初始化基本信息
        :return:
        """
        self.cfgFile = SchedulerCfg().cfgIns.cfgFile

    def changeTotalSwitch(self, switch):
        """
        更新总开关。
        :param switch: 总开关值
        :return:
        """
        switch = checkSwitch(switch)
        fu.updateJsonFile(self.cfgFile, 'switch', switch)

    def changeJobSwitch(self, jobName, switch):
        """
        更新job开关。
        :param jobName: job名称；
        :param switch: job开关值；
        :return:
        """
        switch = checkSwitch(switch)
        fu.updateJsonFile(self.cfgFile, f'jobs.[name={jobName}]switch', switch)

    def changeJobCron(self, jobName, cron):
        """
        更新job的cron字符串。
        :param jobName: job名称；
        :param cron: job的cron字符串；
        :return:
        """
        if len(cron.split(' ')) not in (6, 7):
            raise ValueError(f'input cron must be contains 6 or 7 parameters, input cron is: {cron}')
        fu.updateJsonFile(self.cfgFile, f'jobs.[name={jobName}]cron', cron)

    def changeJobFunc(self, jobName, func):
        """
        更新job开关。
        :param jobName: job名称；
        :param func: job回调方法；
        :return:
        """
        fu.updateJsonFile(self.cfgFile, f'jobs.[name={jobName}]func', func)

    def changeJobDesc(self, jobName, descName):
        """
        更新job描述。
        :param jobName: job名称；
        :param descName: job的描述；
        :return:
        """
        fu.updateJsonFile(self.cfgFile, f'jobs.[name={jobName}]descName', descName)


# ---------------------------------------------------------------------------
# 校验开关合法性！0-关闭，1-开启，2-暂停；合法，则返回int格式的数字
# ---------------------------------------------------------------------------
#
def checkSwitch(switch):
    """
    校验开关合法性！0-关闭，1-开启，2-暂停；合法，则返回int格式的数字
    """
    if isinstance(switch, str):
        if switch not in ('-1', '0', '1', '2'):
            raise ValueError(f'Switch must be -1, 0, 1, 2! Input is: {switch}')
        else:
            return int(switch)
    if isinstance(switch, int):
        if switch not in (-1, 0, 1, 2):
            raise ValueError(f'Switch must be -1, 0, 1, 2! Input is: {switch}')
        else:
            return switch
