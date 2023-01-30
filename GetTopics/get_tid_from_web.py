"""从网页逐页分析获取帖子数据

url:https://www.douban.com/group/a-soul/discussion?start=25

start后跟的参数以25增长,根据2023年1月27日本人人工粗略核算，大概截至412175
"""
import time,random

import requests

from pyquery import PyQuery as pq
from utils import getCookieDict
from TerminalLogger import LOGGER
from orm import TidDataBaseConnector


# cookie自己从网页端查询获取。使用cookie能够尽量避免出现验证码的问题。
# 以及豆瓣的cookie过期挺快的x这一点可能需要注意一下
cookie = r''

c = getCookieDict(cookie)

requestsHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Connection": "keep-alive"
}
url = 'https://www.douban.com/group/a-soul/discussion?start={nums}'

conn = requests.session()

nums = 0

dbConnecter = TidDataBaseConnector()

while nums != 412575:
    url='https://www.douban.com/group/a-soul/discussion?start={nums}'.format(nums=nums)

    html = conn.get(
        url=url, cookies=c, headers=requestsHeaders
    ).content

    data = pq(html)
    topics = data('.olt tr')

    if topics.length == 0:
        LOGGER.WARNING(f'Pyquery parse get 0 value,now the nums is:{nums}')
        break

    storge_data = []
    for ele in topics.items():
        # 去除表头
        if ele.has_class('th'):
            continue

        # 根据a标签获取对应内容。此时第一个元素对应主题帖的链接，第二个元素对应发帖人的链接。
        a_tags = [one_a_tag for one_a_tag in ele.find('a').items()]

        tid = a_tags[0].attr('href').split('https://www.douban.com/group/topic/')[1].split('/')[0]
        title = a_tags[0].attr('title')
        author = a_tags[1].text()
        author_id = a_tags[1].attr('href').split('https://www.douban.com/people/')[1].split('/')[0]
        storge_data.append({
            "tid":tid,
            "title":title,
            "author":author,
            "author_id":author_id
        })
    LOGGER.DEBUG('Finish data parse,now try storge data...')
    dbConnecter.addTidRecords(storge_data)
    # 防止频率过高被封
    rand_nums = random.randint(6,20)
    LOGGER.DEBUG('Sleeping.....,sleep time is {seconds} secs'.format(seconds=rand_nums))
    time.sleep(6)
    LOGGER.DEBUG('Recovering...')
    nums = nums + 25;



