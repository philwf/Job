from myLogger import MyLogger
from fastapi import FastAPI, Header, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional
import traceback
import myConfig as mc
from .apis import schedulerAPI

app = FastAPI()

job_config_filename = 'job.json'
job_log_key = 'log-file'

jobCfg = mc.MyConfig(job_config_filename).config

log = MyLogger()
log.initLogger(fileName=jobCfg[job_log_key])


def initLog(logFileName: str = job_log_key):
    """

    """
    jobLog = MyLogger()
    jobLogFile = jobCfg[job_log_key]
    if logFileName in jobCfg:
        jobLogFile = jobCfg[logFileName]
    jobLog.initLogger(fileName=jobLogFile)
    return jobLog
