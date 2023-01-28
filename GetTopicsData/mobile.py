import asyncio
import aiohttp
import hmac
import base64
import time
from urllib.parse import urlparse, quote
from orm.TerminalLogger import LOGGER

headers = {
    "User-Agent": "api-client/1 com.douban.frodo/7.27.0.6(231) Android/25 product/DT1901A vendor/smartisan model/DT1901A brand/smartisan  rom/android  network/wifi  udid/5fe86d1e414d8417ff3ec84c369e28c97a7d9d45  platform/AndroidPad",
}

dm = {
    "channel": "Douban",
    "os_rom": "android",
}


class KEYS:
    TITLE = "title"


def encrypt(api, method, data):
    path = urlparse(api).path
    secret = b"bf7dddc7c9cfe6f7"
    timestamp = int(time.time())
    message = ("&".join([method, quote(path, safe=""), str(timestamp)]))
    h = hmac.new(secret, message.encode(), digestmod="sha1")
    b = base64.b64encode(h.digest())
    return {
        **data,
        "apikey": "0dad551ec0f84ed02907ff5c42e8ec70",
        "_ts": timestamp,
        "_sig": b.decode(),
    }


def parseTopic(topicData: dict):
    theTopicData = {
        'id': topicData['id'],
        'title': topicData['title'],
        'url': topicData['url'],
        'update_time': topicData['update_time'],
        'create_time': topicData['create_time'],
        'content': topicData['content'],
        'author_name': topicData['author']['name'],
        'author_id': topicData['author']['id'],
        'author_uid': topicData['author']['uid'],
        'author_url': topicData['author']['url'],
        'author_true_url': topicData['author']['url'].replace(topicData['author']['uid'], topicData['author']['id']),
        'author_avatar': topicData['author']['avatar']
    }
    return theTopicData


def parseComments(commentData: list[dict]):
    theCommentResultData = []
    for ele in commentData:
        oneCommentData = {
            'author_name': ele['author']['name'],
            'author_avatar': ele['author']['avatar'],
            'author_id': ele['author']['id'],
            'author_uid': ele['author']['uid'],
            'author_url': ele['author']['url'],
            'author_true_url': ele['author']['url'].replace(ele['author']['uid'], ele['author']['id']),
            'author_register_time': ele['author']['reg_time'],
            'reply_text': ele['text'],
            'reply_time': ele['create_time'],
            'reply_id': ele['id']
        }
        if len(ele['photos']) != 0:
            oneCommentData['photo'] = ele['photos'][0]['image']['large']['url']
        if 'ref_comment' in ele:
            # 这里针对的是楼中楼的互相回复
            oneCommentData['ref_id'] = ele['ref_comment']['id']
            # 以防万一，存储一下对应的回复数据
            # 面对回复量巨大的帖子，如果将这项数据指向前边的帖，索引时间成本太高
            oneCommentData['ref_storge'] = ele['ref_comment']

        theCommentResultData.append(oneCommentData)
    return theCommentResultData


async def get(api, params=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(api, params=params, headers=headers) as resp:
            data = await resp.json()
            return data


async def getTopic(tid) -> dict:
    """获取主题帖内容
    """
    LOGGER.DEBUG("Getting content of the topic,tid is [{tid}]".format(tid=tid))
    api = "https://frodo.douban.com/api/v2/group/topic/{}".format(tid)
    params = encrypt(api, "GET", dm)
    return await get(api, params)



async def getComment(tid, start=0, count=20):
    """获取指定范围的回复内容。由于豆瓣移动端通常是一页20条回复，故而应限制一次获取20条以避免被gank
    """
    api = "https://frodo.douban.com/api/v2/group/topic/{}/comments".format(tid)
    params = encrypt(api, "GET", {**dm, "start": start, "count": count})
    return await get(api, params)



async def getAllComments(tid):
    """获取所有评论。一次20条
    """
    comments = []
    i = 0
    r = await getComment(tid)
    while r["comments"]:
        i += 20
        comments += r["comments"]
        r = await getComment(tid, i)
        LOGGER.DEBUG('Get comment in Topic {tid}] ,Page {page}'.format(
            tid=tid, page=int(i / 20)))
        await asyncio.sleep(0.5)
    print('\u001b[32m [SUCCESS]Finish! \n')
    return comments


async def getTopics(tid_list: list):
    """通过帖子id列表获取帖子内容与回复"""
    results = {}
    for one_tid in tid_list:
        topicContent = await getTopic(one_tid)
        # 先通过获取主题帖内容，判断帖子是否还存在。如果不存在则略过
        if 'code' in topicContent:
            LOGGER.WARNING('The Topic [{tid}] is not exist or not be public'.format(tid=one_tid))
            continue

        # 获取帖子回复内容
        topicComments = await getAllComments(one_tid)

        tid_keyname = str(one_tid)
        results[tid_keyname] = {}
        results[tid_keyname]['topic'] = parseTopic(topicContent)
        results[tid_keyname]['comments'] = parseComments(topicComments)
    return results

async def getTopicsData(tid_list:list):
    results = {}
    for one_tid in tid_list:
        topicContent = await getTopic(one_tid)

        # 先通过获取主题帖内容，判断帖子是否还存在。如果不存在则略过
        if 'code' in topicContent:
            LOGGER.WARNING('The Topic [{tid}] is not exist or not be public'.format(tid=one_tid))
            continue

        tid_keyname = str(one_tid)
        results[tid_keyname] = parseTopic(topicContent)
    return results

async def getTopicsComments(tid_list:list):
    results = {}
    for one_tid in tid_list:
        topicContent = await getTopic(one_tid)
        # 先通过获取主题帖内容，判断帖子是否还存在。如果不存在则略过
        if 'code' in topicContent:
            LOGGER.WARNING('The Topic [{tid}] is not exist or not be public'.format(tid=one_tid))
            continue

        # 获取帖子回复内容
        topicComments = await getAllComments(one_tid)

        tid_keyname = str(one_tid)
        results[tid_keyname] = parseComments(topicComments)
    return results

def createTopicDataTask(tid_list:list):
    data = asyncio.run(getTopicsData(tid_list))
    result = []
    for ele in data.keys():
        result.append(data[ele])
    return result

def createTopicsCommentDataTask(tid_list:list):
    result = asyncio.run(getTopicsComments(tid_list))
    return result

def test():
    createTopicDataTask([269962261,276675253])


# 276423355,276413817,276675253,
# 273241798屎楼
# createNewTask([269962261,276675253])
if __name__ == "__main__":
    test()
