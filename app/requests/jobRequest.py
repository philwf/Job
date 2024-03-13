from . import myRequestConfig


def jobComRequest(apiCall: str):
    """
    默认爬取。
    """
    apiCalls = apiCall.split(':')
    apiName = apiCalls[0]
    apiMethodName = apiCalls[1]
    return myRequestConfig.request(apiName=apiName, apiMethodName=apiMethodName)
