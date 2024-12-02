# -*- coding: utf-8 -*-
from fastapi import Depends

from myClassUtil import BasicApi
from . import log, api_auth, job_cfg
from ..services.jobCfg import JobOptType, SchedulerOptType
from ..services.schedulerMgt import SchedulerMgt


JOB_OPT_TYPE = JobOptType()
SCHEDULER_OPT_TYPE = SchedulerOptType()

class JobAPI(BasicApi):
    """

    """
    def __init__(self):
        """

        """
        super().__init__(job_cfg.apis_cfg_filename)

        if not self.log:
            self.log = log

        self.scheduler_mgt = SchedulerMgt(job_cfg.scheduler_cfg_filename)

    async def get_jobs_api(self, page_no: int, page_size: int,
                           auth: str = Depends(api_auth.authentication)):
        """

        :param page_no:
        :param page_size:
        :param auth:
        :return:
        """
        self.log.debug(f'JobAPI.get_jobs_api===>auth: {auth}')
        self.log.info(f'JobAPI.get_jobs_api===>pageNo/Size: {page_no}/{page_size}')

        jobs = self.scheduler_mgt.get_jobs(page_no, page_size)
        result = {'result': 'ok', 'data': jobs}

        self.log.info(f'JobAPI.get_jobs_api===>result: {result}')
        return result

    async def get_job_api(self, job_id: str,
                          auth: str = Depends(api_auth.authentication)):
        """

        :param job_id:
        :param auth:
        :return:
        """
        self.log.debug(f'JobAPI.get_job_api===>auth: {auth}')
        self.log.info(f'JobAPI.get_job_api===>job_id: {job_id}')

        jobs = self.scheduler_mgt.get_job(job_id)
        result = {'result': 'ok', 'data': jobs}

        self.log.info(f'JobAPI.get_job_api===>result: {result}')
        return result

    async def add_job_api(self, job: dict,
                          auth: str = Depends(api_auth.authentication)):
        """

        :param job:
        :param auth:
        :return:
        """
        self.log.debug(f'JobAPI.add_job_api===>auth: {auth}')
        self.log.info(f'JobAPI.add_job_api===>job: {job}')

        self.scheduler_mgt.add_job(job)
        result = {'result': 'ok', 'data': 'ok'}

        self.log.info(f'JobAPI.add_job_api===>result: {result}')
        return result

    async def opt_jobs_api(self, opt_jobs: dict,
                           auth: str = Depends(api_auth.authentication)):
        """

        :param opt_jobs:
        :param auth:
        :return:
        """
        self.log.debug(f'JobAPI.opt_jobs_api===>auth: {auth}')
        self.log.info(f'JobAPI.opt_jobs_api===>opt_jobs: {opt_jobs}')

        if opt_jobs["opt_type"] == JOB_OPT_TYPE.REMOVE:
            self.scheduler_mgt.remove_jobs(opt_jobs['job_ids'])
        elif opt_jobs["opt_type"] == JOB_OPT_TYPE.PAUSE:
            self.scheduler_mgt.pause_jobs(opt_jobs['job_ids'])
        elif opt_jobs["opt_type"] == JOB_OPT_TYPE.RESUME:
            self.scheduler_mgt.resume_jobs(opt_jobs['job_ids'])
        else:
            raise ValueError(f'undefined opt_type: {opt_jobs["opt_type"]}')
        result = {'result': 'ok', 'data': 'ok'}

        self.log.info(f'JobAPI.opt_jobs_api===>result: {result}')
        return result

    async def opt_scheduler_api(self, opt_type: str,
                                auth: str = Depends(api_auth.authentication)):
        """

        :param opt_type:
        :param auth:
        :return:
        """
        self.log.debug(f'JobAPI.opt_jobs_api===>auth: {auth}')
        self.log.info(f'JobAPI.opt_jobs_api===>opt_type: {opt_type}')

        if opt_type == SCHEDULER_OPT_TYPE.START:
            self.scheduler_mgt.start_scheduler()
        elif opt_type == SCHEDULER_OPT_TYPE.STOP:
            self.scheduler_mgt.stop_scheduler()
        elif opt_type == SCHEDULER_OPT_TYPE.STOP_NOW:
            self.scheduler_mgt.stop_scheduler(False)
        elif opt_type == SCHEDULER_OPT_TYPE.RESUME:
            self.scheduler_mgt.resume_scheduler()
        elif opt_type == SCHEDULER_OPT_TYPE.PAUSE:
            self.scheduler_mgt.pause_scheduler()
        else:
            raise ValueError(f'undefined opt_type: {opt_type}')
        result = {'result': 'ok', 'data': 'ok'}

        self.log.info(f'JobAPI.opt_scheduler_api===>result: {result}')
        return result
