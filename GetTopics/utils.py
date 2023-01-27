import re
import time



def getCookieDict(cookiestr: str):
    return {
        ele.lstrip().replace('"', '').split('=')[0]: ele.lstrip().replace('"', '').split('=')[1] for ele in
        cookiestr.split(';')
    }



def mapToTid(links_list: list):
    clip_right_string = list(
        map(lambda ele: re.search(pattern='https://www.douban.com/[a-z]*/topic/[0-9]*', string=ele).group()
        if re.match(pattern='https://www.douban.com/[a-z]*/topic/[0-9]*', string=ele) != None
        else "https://www.douban.com/group/topic/", links_list)
    )
    return list(
        map(lambda ele: re.sub(pattern='https://www.douban.com/group/topic/', repl="", string=ele), clip_right_string)
    )


def mapToUnique(links_list):
    que = []
    for ele in links_list:
        if ele in que or ele == "":
            continue
        que.append(ele)
    return que


def getSSEPipeMsg(msg="null", type="info"):
    return {
        "process": "SSE_SubProcess",
        "type": type,
        "time": time.asctime(),
        "msg": msg
    }

def destructSSEPipeMsg(msg:dict)->str:
    return "[{type}]{time}   From {process}:{msg}".format(
        type=msg['type'],
        time=msg['time'],
        process=msg['process'],
        msg=msg['msg']
    )


def SSE_isValueNeedChange(key,dict):
    """约定从主进程传至SSE子进程的Pipe消息为一个字典对象。若需要改变，返回True"""
    # 对于不需要更改的值传递的键值为'null'，因此此处为不等于。
    return dict[key] != "null"
