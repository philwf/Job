import traceback

from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.jobstores.redis import RedisJobStore

from .schedulerCfg import SchedulerCfg
from .schedulerCron import CronFiled
from .. import MyLogger
import myComUtil as cu
import myRedisUtil as ru
import myFileUtil as fu


# ---------------------------------------------------------------------------
# 定时任务管理
# ---------------------------------------------------------------------------
#
class SchedulerMgt(object):
    """
    定时任务管理。
    """
    #: constant indicating a scheduler's closed state
    STATE_CLOSED = -1
    #: constant indicating a scheduler's stopped state
    STATE_STOPPED = 0
    #: constant indicating a scheduler's running state (started and processing jobs)
    STATE_RUNNING = 1
    #: constant indicating a scheduler's paused state (started but not processing jobs)
    STATE_PAUSED = 2

    def __init__(self, triggerType=None, configFile=None):
        """
        初始化定时任务管理基本信息。
        :return:
        """
        # 初始化定时任务配置
        self.configFile = configFile
        self.sCfg = SchedulerCfg(self.configFile)

        # 初始化日志
        self.log = MyLogger()
        self.log.initLogger(fileName=self.sCfg.logFile)

        # 初始化定时任务触发器
        self.triggerType = triggerType

        # 设置当前定时任务运行相关参数
        self.jobCrons = {}
        self.jobSwitchs = {}
        self.currentSwitch = None

        # 初始化任务和触发器
        self._initScheduler(triggerType=self.triggerType)

    def _initScheduler(self, triggerType=None):
        """
        初始化触发器实例。
        :param triggerType: 触发器类型。Blocking--阻塞型；Background--后台型；
        :return:
        """
        # 设置定时任务默认类型
        if not triggerType:
            triggerType = 'Background'

        # 设置存储
        jobstores = None
        if self.sCfg.redisName:
            redisCfg = ru.Redis(self.sCfg.redisName).redisCfg
            self.log.debug(f'redisCfg: {redisCfg}')
            jobstores = {
                'redis': RedisJobStore(**redisCfg)
            }

        # 根据定时任务类型，初始化对应定时任务
        if triggerType == 'Background':
            self.scheduler = BackgroundScheduler(jobstores=jobstores)
        elif triggerType == 'Blocking':
            self.scheduler = BlockingScheduler(jobstores=jobstores)
        else:
            raise ValueError(f'type must be Background or Blocking! input: {self.scheduler}')

        # 初始化任务
        self._initJobs()

        # 启动触发器
        if self.sCfg.switch == self.STATE_RUNNING:
            self.scheduler.start()
            self.currentSwitch = self.STATE_RUNNING

    def _initJobs(self):
        """
        初始化任务。
        :return:
        """
        # 初始化job
        toDoJobNames = self._getToDoJobNames(self.sCfg.jobsCfg.jobNames)
        self.addJobs(toDoJobNames['needAdd'], self.sCfg.jobsCfg.jobFunc, self.sCfg.jobsCfg.jobCronStr, self.sCfg.jobsCfg.jobApiCall)
        self.pauseJobs(toDoJobNames['needPause'])
        self.log.info(f'schedulerMgt._initJobs====>needAdd: {toDoJobNames["needAdd"]}, needPause: {toDoJobNames["needPause"]}')

    def addJobs(self, jobNames: list, jobFuncs: dict, jobCronStrs: dict, jobApiCall: dict):
        """
        添加任务。
        :param jobNames: 任务名称列表；
        :param jobFuncs: 任务方法列表；
        :param jobCronStrs: 任务cron字符串字典；
        :param jobApiCall: 任务调用API
        :return:
        """
        for name in jobNames:
            # 回调函数
            func = jobFuncs[name]
            # 定时任务触发器参数
            cronStr = jobCronStrs[name]
            # 定时任务cron字符串
            cron = CronFiled(cronStr).cron
            # 回调函数参数
            kwargs = {'apiCallStr': jobApiCall[name]}

            # 打印添加定时任务日志
            self.log.info(cu.FontColor(f'添加定时任务, name={name}, cron={cronStr}').green)
            self.scheduler.add_job(id=name, func=func, jobstore='redis', replace_existing=True,
                                   trigger='cron', kwargs=kwargs, **cron)

            # 添加到任务清单中
            self.jobCrons[name] = cronStr
            self.jobSwitchs[name] = self.sCfg.jobsCfg.jobSwitch[name]

    def pauseJobs(self, jobNames):
        """
        暂停定时任务。
        :param jobNames: 任务名称列表；
        :return:
        """
        for name in jobNames:
            self.log.info(cu.FontColor(f'暂停定时任务: {name}').red)
            self.scheduler.pause_job(job_id=name, jobstore='redis')
            self.jobSwitchs[name] = self.STATE_PAUSED

    def resumeJobs(self, jobNames):
        """
        恢复定时任务。
        :param jobNames: 任务名称列表；
        :return:
        """
        for name in jobNames:
            self.log.info(cu.FontColor(f'恢复定时任务: {name}').green)
            self.scheduler.resume_job(job_id=name, jobstore='redis')
            self.jobSwitchs[name] = self.STATE_RUNNING

    def rescheduleJobs(self, jobNames, jobCronStrs):
        """
        更新定时任务触发器。
        :param jobNames: 任务名称列表；
        :param jobCronStrs: 任务cron字符串字典；
        :return:
        """
        for name in jobNames:
            # 定时任务触发器参数
            # cron = self.sCfg.jobsCfg.jobCron[name]
            # 定时任务cron字符串
            cronStr = jobCronStrs[name]

            self.updateJobCron(name, cronStr)

    def updateJobCron(self, jobName, cronStr):
        """
        更新定时任务触发器。
        :param jobName: 任务名称；
        :param cronStr: cron字符串
        :return:
        """
        cron = CronFiled(cronStr).cron
        self.log.info(cu.FontColor(f'更新定时任务: {jobName}, cron={cronStr}').green)
        self.scheduler.reschedule_job(job_id=jobName, jobstore='redis', trigger='cron', **cron)
        self.jobCrons[jobName] = cronStr

    def updateJobFuncs(self, jobNames, jobFuncs, jobApiCalls):
        """
        批量更新定时任务回调方法。
        :param jobNames: 任务名称列表；
        :param jobFuncs: 回调方法字典；
        :param jobApiCalls: 回调方法参数字典；
        :return:
        """
        for name in jobNames:
            jobFunc = jobFuncs[name]
            jobApiCall = jobApiCalls[name]

            self.updateJobFunc(name, jobFunc, jobApiCall)

    def updateJobFunc(self, jobName, jobFun, jobApiCall):
        """
        更新定时回调方法。
        :param jobName: 任务名称；
        :param jobFun: 回调方法；
        :param jobApiCall: 回调方案参数；
        :return:
        """
        self.log.info(cu.FontColor(f'更新定时任务: {jobName}, func={jobFun}').green)
        kwargs = {'apiCallStr': jobApiCall}
        self.scheduler.modify_job(job_id=jobName, jobstore='redis', func=jobFun, kwargs=kwargs)

    def removeJobs(self, jobNames):
        """
        删除任务。
        :param jobNames: 任务名称列表；
        :return:
        """
        for name in jobNames:
            # 打印删除定时任务日志
            self.log.info(cu.FontColor(f'删除定时任务, name={name}').red)

            # 从定时任务中移除
            self.scheduler.remove_job(job_id=name, jobstore='redis')
            # 从任务清单中移除
            self.jobCrons.pop(name)
            self.jobSwitchs.pop(name)

    def startSchedule(self):
        """
        启动定时任务调度器。
        :return:
        """
        # 总开关
        self.log.info(f'schedulerMgt.startSchedule===>before scheduler.state: {self.scheduler.state}')
        if self.scheduler.state is None or self.scheduler.state == self.STATE_STOPPED:
            # 初始化job
            self.log.info(cu.FontColor(f'启动定时任务调度器...').green)
            self.sCfg = SchedulerCfg(self.configFile)
            self.log.info(f'读取最新开关配置: {self.sCfg.switch}')
            self._initScheduler(triggerType='Background')
            self.log.info(cu.FontColor(f'定时任务调度器已启动！').green)
            self.currentSwitch = self.STATE_RUNNING
        else:
            self.log.warning(cu.FontColor(f'定时任务调度器已经在运行中！').red)

        self.log.info(f'schedulerMgt.startSchedule===>after scheduler.state: {self.scheduler.state}')

    def stopSchedule(self, wait=True):
        """
        停止定时任务调度器。
        :return:
        """
        self.log.info(f'schedulerMgt.stopSchedule===>before scheduler.state: {self.scheduler.state}')

        # 判断状态
        if self.scheduler.state != self.STATE_STOPPED:
            # 清空当前定时任务运行相关参数
            self.jobSwitchs = {}
            self.jobCrons = {}
            # 停止定时任务调度器
            self.log.info(cu.FontColor(f'停止定时任务调度器...').red)
            # self.scheduler.remove_all_jobs()
            self.scheduler.shutdown(wait)
            self.log.info(cu.FontColor(f'定时任务调度器已停止！').red)
            # 更新总开关标识
            self.currentSwitch = self.STATE_STOPPED
        else:
            self.log.warning(cu.FontColor(f'定时任务调度器未运行！').red)
            if self.scheduler.state is None:
                self.currentSwitch = self.STATE_STOPPED

        self.log.info(f'schedulerMgt.stopSchedule===>after scheduler.state: {self.scheduler.state}')

    def pauseSchedule(self):
        """
        暂停定时任务调度器。
        :return:
        """
        self.log.info(f'schedulerMgt.pauseSchedule===>before scheduler.state: {self.scheduler.state}')

        if self.scheduler.state == self.STATE_RUNNING:
            self.log.info(cu.FontColor(f'暂停定时任务调度器...').red)
            self.scheduler.pause()
            self.log.info(cu.FontColor(f'定时任务调度器已暂停！').red)
            self.currentSwitch = self.STATE_PAUSED
        else:
            self.log.warning(cu.FontColor(f'定时任务调度器未运行！').red)
            if self.scheduler.state is None:
                self.currentSwitch = self.STATE_PAUSED

        self.log.info(f'schedulerMgt.pauseSchedule===>after scheduler.state: {self.scheduler.state}')

    def resumeSchedule(self):
        """
        暂停定时任务调度器。
        :return:
        """
        if self.scheduler.state == self.STATE_PAUSED:
            self.log.info(cu.FontColor(f'恢复定时任务调度器...').green)
            self.scheduler.resume()
            self.log.info(cu.FontColor(f'定时任务调度器已恢复！').green)
            self.currentSwitch = self.STATE_RUNNING
        else:
            self.log.warning(cu.FontColor(f'定时任务调度器未暂停！').red)

    def displayJob(self, jobName):
        """
        显示当前运行中定时任务。
        :return: result--当前运行中定时任务清单。
        """
        # 读取最新配置
        cfg = SchedulerCfg()
        cfgJobs = cfg.jobsCfg.jobs
        # 读取触发器中任务
        scheduledJobs = self.scheduler.get_jobs(jobstore='redis')
        # 定义返回参数
        result = {}

        for job in cfgJobs:
            if job['name'] == jobName:
                # 下次启动时间
                job['nextRunTime'] = ''
                for sJob in scheduledJobs:
                    if job['name'] == sJob.id:
                        if hasattr(sJob, 'next_run_time'):
                            if sJob.next_run_time:
                                job['nextRunTime'] = sJob.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
                        break

                result['job'] = job

                # 上次运行记录
                jobLog = self.readJobLog(job['name'], limit=10)
                result['logs'] = jobLog['logs']
                try:
                    result['lastRunRecord'] = jobLog['logs'][0]
                except:
                    self.log.error(f'jobName={job["name"]}, jobLog={jobLog}')
                    self.log.error(traceback.format_exc())
                    result['lastRunRecord'] = {'start': '', 'end': '', 'spend': ''}

                return result

        return result

    def getJobNames(self):
        """

        """
        # 读取最新配置
        cfg = SchedulerCfg()
        jobNames = cfg.jobsCfg.jobNames
        return {'jobNames': jobNames}

    def displayJobs(self, pageNo, pageSize):
        """
        显示当前运行中定时任务。
        :return: result--当前运行中定时任务清单。
        """
        # 读取最新配置
        cfg = SchedulerCfg()
        cfgJobs = cfg.jobsCfg.jobs
        # 读取触发器中任务
        scheduledJobs = self.scheduler.get_jobs(jobstore='redis')
        # 定义返回参数
        totalRecords = len(cfgJobs)
        result = {'switch': cfg.switch, 'state': self.scheduler.state, 'totalRecords': totalRecords}

        self.log.debug(cu.FontColor(f'当前运行或暂停定时任务({len(scheduledJobs)}个): ').green)

        for job in cfgJobs:
            # 下次启动时间
            job['nextRunTime'] = ''
            for sJob in scheduledJobs:
                if job['name'] == sJob.id:
                    if hasattr(sJob, 'next_run_time'):
                        if sJob.next_run_time:
                            job['nextRunTime'] = sJob.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
                    break
            # 上次运行记录
            jobLog = self.readJobLog(job['name'], limit=10)
            job['logs'] = jobLog['logs']
            try:
                job['lastRunRecord'] = jobLog['logs'][0]
            except:
                self.log.error(f'jobName={job["name"]}, jobLog={jobLog}')
                self.log.error(traceback.format_exc())
                job['lastRunRecord'] = {'start': '', 'end': '', 'spend': ''}

            self.log.debug(cu.FontColor(f'--{job["name"]}, 下次运行: {job["nextRunTime"]}').green)

        # 设置返回
        startNo = (int(pageNo)-1) * int(pageSize) + 1
        endNo = int(pageNo) * int(pageSize)
        if totalRecords < endNo:
            endNo = totalRecords
        if startNo > totalRecords:
            startNo = totalRecords
            endNo = totalRecords

        result['jobs'] = cfgJobs[startNo-1: endNo]

        return result

    def refreshJobs(self):
        """
        根据配置文件，刷新定时任务。
        :return:
        """
        # 读取最新配置
        cfg = SchedulerCfg()
        cfgJobs = cfg.jobsCfg

        # 添加任务，覆盖现有
        self.addJobs(cfgJobs.jobNames, cfgJobs.jobFunc, cfgJobs.jobCronStr)

    def readSchedulerLog(self):
        """
        读取触发器的日志文件。
        :return: logLists-按行存在的日志列表
        """
        logLists = fu.readLastMatchLinesFromFile(self.sCfg.logFile)[0]
        return logLists

    def readJobLog(self, jobName, limit=11):
        """
        读取任务的日志记录。
        :param jobName: 任务名称；
        :param limit: 展示的行数，默认展示11行；
        :return:
        """
        # jobName转换
        if jobName == '91porn':
            jobName = '91'
        elif jobName == '1024-novelReply':
            jobName = '1024 - reply'
        elif jobName == 'archive':
            jobName = '归档'

        matchStrs = [f'{cu.FontColor(f"启动{jobName}").green}定时任务', f'{cu.FontColor(f"完成{jobName}").red}定时任务']
        filterStrs = ['ERROR', 'WARN', 'WARNNING']
        logLists = fu.readLastMatchLinesFromFile(self.sCfg.logFile, matchStrs=matchStrs, filterStrs=filterStrs)[0]
        if not logLists:
            logLists = fu.readLastMatchLinesFromFile(f'{self.sCfg.logFile}.1',
                                                     matchStrs=matchStrs, filterStrs=filterStrs)[0]
        resultDict = {}
        result = {'logs': []}

        i = 1

        for logLine in logLists:
            if 'ERROR' in logLine:
                continue

            indexStr = f'定时任务：'
            try:
                cursor = logLine.index(indexStr)
            except ValueError:
                self.log.error(f'indexStr: {indexStr}, 在行: {logLine} 中不存在')
                continue
            timeStr = logLine[cursor + len(indexStr): cursor + len(indexStr) + 19]
            if '完成' in logLine:
                if i % 2 == 0:
                    i += 1
                resultDict[i // 2] = {'end': timeStr}

                cursorSpend = logLine.index(f'耗时：')
                resultDict[i // 2]['spend'] = logLine[cursorSpend + 3:]
            elif '启动' in logLine:
                if i % 2 == 1:
                    i += 1
                if i // 2 - 1 in resultDict:
                    resultDict[i // 2 - 1]['start'] = timeStr
                else:
                    resultDict[i // 2 - 1] = {'start': timeStr}

            i += 1

        j = 0
        for key in sorted(resultDict):
            result['logs'].append(resultDict[key])
            j += 1
            if j >= limit:
                break

        return result

    def readJobLogDetail(self, name=None, start=1, end=100):
        """

        """
        result = {'jobNames': self.sCfg.jobsCfg.jobNames}

        logFile = self.sCfg.logFile
        matchStrs = []
        if name:
            matchStrs.append(name)
        logItems, totalRecords = fu.readLastMatchLinesFromFile(logFile, matchStrs=matchStrs, start=start, end=end)
        result['logItems'] = logItems
        result['totalRecords'] = totalRecords

        return result

    def _getToDoJobNames(self, jobNames):
        """
        获取对应任务列表各种处理场景的清单。
        :param jobNames: 任务列表；
        :return: toDoJobNames--待处理任务名称字典表。其中key值有：
                    - needAdd: 待添加任务清单
                    - needRemove: 待删除任务清单
                    - needPause: 待暂停任务清单
                    - needResume: 待恢复任务清单
                    - needReschedule: 待更新触发器任务清单
        """
        toDoJobNames = {'needAdd': [], 'needRemove': [], 'needPause': [], 'needResume': [], 'needReschedule': []}
        for name in jobNames:
            # 定时任务已添加的情况
            if name in self.jobCrons:
                # 如果cron发生了变化
                if self.jobCrons[name] != self.sCfg.jobsCfg.jobCronStr[name]:
                    # 待更新触发器清单
                    toDoJobNames['needReschedule'].append(name)
                # 如果定时任务开关发生了变化
                if self.jobSwitchs[name] != self.sCfg.jobsCfg.jobSwitch[name]:
                    # 待暂停清单
                    if self.sCfg.jobsCfg.jobSwitch[name] == self.STATE_PAUSED:
                        toDoJobNames['needPause'].append(name)
                    # 待恢复清单
                    elif self.sCfg.jobsCfg.jobSwitch[name] == self.STATE_RUNNING:
                        toDoJobNames['needResume'].append(name)
                    # 待删除清单
                    elif self.sCfg.jobsCfg.jobSwitch[name] == self.STATE_STOPPED:
                        toDoJobNames['needRemove'].append(name)
            # 定时任务未添加的情况
            elif self.sCfg.switch != self.STATE_STOPPED:
                # 启动状态的定时任务
                if self.sCfg.jobsCfg.jobSwitch[name] == self.STATE_RUNNING:
                    toDoJobNames['needAdd'].append(name)
                # 暂停状态的定时任务，先添加，再暂停
                elif self.sCfg.jobsCfg.jobSwitch[name] == self.STATE_PAUSED:
                    toDoJobNames['needAdd'].append(name)
                    toDoJobNames['needPause'].append(name)

        # 处理配置文件中已被删除的定时任务
        for name in self.jobCrons:
            if name not in jobNames:
                toDoJobNames['needRemove'].append(name)

        return toDoJobNames
