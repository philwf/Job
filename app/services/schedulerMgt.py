from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.job import Job

from datetime import datetime

from .jobCfg import SchedulerCfg, JobTriggerType
from .cronFiled import CronFiled, TiggerCronStr
from .. import MyLogger
from myComUtil import FontColor
from myRedisUtil import RedisClient
from ..jobs.jobs import start_job_by_api


# ---------------------------------------------------------------------------
# 定时任务管理
# ---------------------------------------------------------------------------
#
class SchedulerMgt(object):
    """

    """
    TIME_FMT = '%Y-%m-%d %H:%M:%S'
    JOB_STORE = 'redis'
    TRIGGER_TYPE = JobTriggerType()

    def __init__(self, cfg_filename=None):
        """

        """
        # 初始化定时任务配置
        self.cfg_filename = cfg_filename
        self.scheduler_cfg = SchedulerCfg(self.cfg_filename)

        # 初始化日志
        self.log = MyLogger()
        self.log.initLogger(fileName=self.scheduler_cfg.log_file)

        self._init_scheduler()

    def _init_scheduler(self):
        """
        初始化触发器实例。
        :return:
        """
        # 设置存储
        redis_cfg = RedisClient(self.scheduler_cfg.redis_name).redisCfg
        self.log.debug(f'redisCfg: {redis_cfg}')
        job_stores = {
            self.JOB_STORE: RedisJobStore(**redis_cfg)
        }

        # 根据定时任务类型，初始化对应定时任务
        if self.scheduler_cfg.trigger_type == self.TRIGGER_TYPE.BACKGROUND:
            self.scheduler = BackgroundScheduler(jobstores=job_stores)
        elif self.scheduler_cfg.trigger_type == self.TRIGGER_TYPE.BLOCKING:
            self.scheduler = BlockingScheduler(jobstores=job_stores)
        else:
            raise ValueError(f'type must be Background or Blocking! input: {self.scheduler}')

        self.start_scheduler()

    def start_scheduler(self):
        """

        :return:
        """
        self.scheduler.start()
        self.log.info(FontColor(f'启动定时任务管理器！').green)

    def stop_scheduler(self, wait=True):
        """

        :return:
        """
        self.scheduler.shutdown(wait)
        self.log.info(FontColor(f'停止定时任务管理器！').red)

    def pause_scheduler(self):
        """

        :return:
        """
        self.scheduler.pause()
        self.log.info(FontColor(f'暂停定时任务管理器！').red)

    def resume_scheduler(self):
        """

        :return:
        """
        self.scheduler.resume()
        self.log.info(FontColor(f'恢复定时任务管理器！').green)

    def get_jobs(self, page_no: int, page_size: int):
        """

        :param page_no:
        :param page_size:
        :return:
        """
        # 读取触发器中任务
        scheduled_jobs = self.scheduler.get_jobs(jobstore=self.JOB_STORE)
        total_records = len(scheduled_jobs)
        result = {'total_records': total_records}

        start_no = min((int(page_no) - 1) * int(page_size) + 1, total_records)
        end_no = min(int(page_no) * int(page_size), total_records)

        paginated_data = []
        for job in scheduled_jobs[start_no-1:end_no]:
            paginated_data.append(self._get_job_info(job))

        result['paginated_data'] = paginated_data
        self.log.info(f'====>get_jobs: scheduled_jobs: {scheduled_jobs}, result: {result}')
        return result

    def get_job(self, job_id: str):
        """

        :param job_id:
        :return:
        """
        job = self.scheduler.get_job(job_id=job_id, jobstore=self.JOB_STORE)
        return self._get_job_info(job)

    def _get_job_info(self, job: Job):
        """

        :param job:
        :return:
        """
        job_info = {
            "id": job.id,
            "name": job.name,
            "func": job.func.__name__,
            "kwargs": job.kwargs,
            "trigger": TiggerCronStr(str(job.trigger)).fmt_cron_str,
            "next_run_time": job.next_run_time.strftime(self.TIME_FMT) if job.next_run_time is not None else "paused"
        }
        return job_info

    def add_job(self, job: dict):
        """

        :param job:
        :return:
        """
        function_map = {
            "start_job_by_api": start_job_by_api,
        }

        name = job['name']
        desc = job['desc']

        cron_str = job['cron']
        cron_dict = CronFiled(cron_str).cron
        cron_dict["timezone"] = self.scheduler_cfg.timezone

        func_name = job['func']
        func = function_map.get(func_name)
        if not func:
            raise ValueError(f"Function '{func_name}' not found in function map.")

        para = job['para']

        if not para:
            para = {}

        self.log.info(FontColor(f'添加定时任务, name={name}, cron={cron_str}').green)
        self.scheduler.add_job(id=name, name=desc, func=func, jobstore=self.JOB_STORE, replace_existing=True,
                               trigger='cron', kwargs=para, **cron_dict)

    def start_jobs(self, job_ids: list):
        """

        :param job_ids:
        :return:
        """
        for job_id in job_ids:
            self.log.info(FontColor(f'立即执行定时任务, name={job_id}').green)

            # 立即执行任务
            self.scheduler.modify_job(job_id=job_id, jobstore=self.JOB_STORE, next_run_time=datetime.now())

    def remove_jobs(self, job_ids: list):
        """

        :param job_ids:
        :return:
        """
        for job_id in job_ids:
            self.scheduler.remove_job(job_id=job_id, jobstore=self.JOB_STORE)
            self.log.info(FontColor(f'删除定时任务, name={job_id}').red)

    def pause_jobs(self, job_ids: list):
        """

        :param job_ids:
        :return:
        """
        for job_id in job_ids:
            self.scheduler.pause_job(job_id=job_id, jobstore=self.JOB_STORE)
            self.log.info(FontColor(f'暂停定时任务: {job_id}').red)

    def resume_jobs(self, job_ids: list):
        """

        :param job_ids:
        :return:
        """
        for job_id in job_ids:
            self.scheduler.resume_job(job_id=job_id, jobstore=self.JOB_STORE)
            self.log.info(FontColor(f'恢复定时任务: {job_id}').green)
