from . import job_com_request, log
from myComUtil import FontColor, formatSeconds
import datetime


def start_job_by_api(api_call_str: str, api_para: dict=None):
    """

    """
    # 开始时间
    t_start = datetime.datetime.now()
    log.info(f'JobByAPI==>{FontColor(api_call_str).green} start: {t_start}')

    job_com_request(api_call_str, api_para)

    # 结束时间
    t_end = datetime.datetime.now()
    t_spend = t_end - t_start
    log.info(f'JobByAPI==>{FontColor(api_call_str).red} end: {t_end}, spend：{formatSeconds(t_spend.seconds)}')
