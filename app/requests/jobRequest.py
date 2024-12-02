from . import request


def job_com_request(api_call: str):
    """
    通用发起请求方法。
    """
    api_calls = api_call.split(':')
    api_name = api_calls[0]
    api_method_name = api_calls[1]
    return request.request(api_name=api_name, api_method_name=api_method_name)
