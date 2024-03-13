from . import jobComRequest, log
import datetime
import myComUtil as cu


def startJobByAPI(apiCallStr: str):
    """

    """
    # 开始时间
    tStart = datetime.datetime.now()
    log.info(f'JobByAPI==>{cu.FontColor(apiCallStr).green} start: {tStart}')

    jobComRequest(apiCallStr)

    # 结束时间
    tEnd = datetime.datetime.now()
    tSpend = tEnd - tStart
    log.info(f'JobByAPI==>{cu.FontColor(apiCallStr).red} end: {tEnd}, spend：{cu.formatSeconds(tSpend.seconds)}')
