# -*- coding: utf-8 -*-
from pydantic import BaseModel
from .. import app, traceback, Depends
from . import log, authentication
from ..services.schedulerCfg import SchedulerCfgMdf, SchedulerCfg
from ..services.schedulerMgt import SchedulerMgt


sm = SchedulerMgt(triggerType='Background')
sc = SchedulerCfg()
scm = SchedulerCfgMdf()


class TotalSwitchModel(BaseModel):
    switch: int = None


class JobSwitchModel(BaseModel):
    jobName: str = None
    switch: int = None


class JobCronModel(BaseModel):
    jobName: str = None
    cron: str = None


class JobFuncModel(BaseModel):
    jobName: str = None
    func: str = None


class JobDescModel(BaseModel):
    jobName: str = None
    descName: str = None


@app.post('/jobApi/jobMgt/totalSwitch')
def modifyTotalSwitchAPI(requst_data: TotalSwitchModel, auth: str = Depends(authentication)):
    switch = requst_data.switch
    log.debug(f'schedulerAPI.modifyTotalSwitch===>switch: {switch}')
    try:
        # 更新配置文件
        if switch == 3:
            jobSwitch = 1
        else:
            jobSwitch = switch
        scm.changeTotalSwitch(jobSwitch)

        # 更新触发器状态
        # 启动
        if switch == 1:
            log.info(f'schedulerAPI.modifyTotalSwitch===>startSchedule')
            sm.startSchedule()
        # 暂停
        elif switch == 2:
            log.info(f'schedulerAPI.modifyTotalSwitch===>pauseSchedule')
            sm.pauseSchedule()
        # 停止
        elif switch == 0:
            log.info(f'schedulerAPI.modifyTotalSwitch===>stopSchedule')
            sm.stopSchedule()
        # 恢复
        elif switch == 3:
            log.info(f'schedulerAPI.modifyTotalSwitch===>resumeSchedule')
            sm.resumeSchedule()
        # 停止
        elif switch == -1:
            log.info(f'schedulerAPI.modifyTotalSwitch===>stopSchedule(wait=False)')
            sm.stopSchedule(wait=False)
        else:
            raise ValueError(f'Unknown switch value: {switch}')

        result = {'result': 'ok'}
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.post('/jobApi/jobMgt/jobSwitch')
def modifyJobSwitchAPI(requst_data: JobSwitchModel, auth: str = Depends(authentication)):
    switch = requst_data.switch
    jobName = requst_data.jobName
    log.debug(f'schedulerAPI.modifyJobSwitch===>jobName:{jobName}, switch:{switch}')
    try:
        # 更新配置文件
        if switch == 3:
            jobSwitch = 1
        else:
            jobSwitch = switch

        scm.changeJobSwitch(jobName, jobSwitch)

        # 更新job
        jobNames = [jobName]
        if switch == 1:
            sm.addJobs(jobNames, sc.jobsCfg.jobFunc, sc.jobsCfg.jobCronStr)
        elif switch == 2:
            sm.pauseJobs(jobNames)
        elif switch == 3:
            sm.resumeJobs(jobNames)
        elif switch == 0:
            sm.removeJobs(jobNames)
        else:
            raise ValueError(f'Unknown switch value: {switch}')

        result = {'result': 'ok'}
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.post('/jobApi/jobMgt/jobCron')
def modifyJobCronAPI(requst_data: JobCronModel, auth: str = Depends(authentication)):
    cron = requst_data.cron
    jobName = requst_data.jobName
    log.debug(f'schedulerAPI.modifyJobCron===>jobName:{jobName}, cron:{cron}')
    try:
        # 更新配置文件
        scm.changeJobCron(jobName, cron)

        # 更新job
        sm.updateJobCron(jobName, cron)

        result = {'result': 'ok'}
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.post('/jobApi/jobMgt/jobDesc')
def modifyJobCronAPI(requst_data: JobDescModel, auth: str = Depends(authentication)):
    descName = requst_data.descName
    jobName = requst_data.jobName
    log.debug(f'schedulerAPI.modifyJobCron===>jobName:{jobName}, descName:{descName}')
    try:
        # 更新配置文件
        scm.changeJobDesc(jobName, descName)

        result = {'result': 'ok'}
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.post('/jobApi/jobMgt/jobFunc')
def modifyJobFuncAPI(requst_data: JobFuncModel, auth: str = Depends(authentication)):
    func = requst_data.func
    jobName = requst_data.jobName
    log.debug(f'schedulerAPI.modifyJobFunc===>jobName:{jobName}, func:{func}')
    try:
        # 更新配置文件
        scm.changeJobFunc(jobName, func)

        # 更新job
        sm.updateJobFunc(jobName, func)

        result = {'result': 'ok'}
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.get('/jobApi/jobMgt/jobs')
def displayJobsAPI(jobName: str = None, pageNo=1, pageSize=10, auth: str = Depends(authentication)):
    log.debug(f'schedulerAPI.displayJobs===>jobName: {jobName}, pageNo/Size: {pageNo}/{pageSize}')
    try:
        if jobName:
            result = sm.displayJob(jobName)
        else:
            result = sm.displayJobs(pageNo, pageSize)
        result['result'] = 'ok'
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.get('/jobApi/jobMgt/jobNames')
def getJobNamesAPI(auth: str = Depends(authentication)):
    log.debug(f'schedulerAPI.getJobNamesAPI===>')
    try:
        result = sm.getJobNames()
        result['result'] = 'ok'
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result


@app.get('/jobApi/jobMgt/jobLog')
def getJobLogAPI(jobName: str = None, start: int = 0, end: int = 100, auth: str = Depends(authentication)):
    log.debug(f'schedulerAPI.getJobLogAPI===>')
    try:
        result = sm.readJobLogDetail(name=jobName, start=start, end=end)
        result['result'] = 'ok'
    except:
        result = {'result': 'error', 'msg': traceback.format_exc()}
    return result
