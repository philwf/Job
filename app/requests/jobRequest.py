from . import request


def job_com_request(api_call: str, api_para: dict=None):
    """
    通用发起请求方法。
    """
    api_calls = api_call.split(':')
    api_name = api_calls[0]
    api_method_name = api_calls[1]
    return request.request(api_name=api_name, api_method_name=api_method_name, params=api_para)
