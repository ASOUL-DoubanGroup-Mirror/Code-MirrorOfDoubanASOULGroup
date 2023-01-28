"""根据tid获取主题帖内容

"""

from orm.TopicDataBase import TopicDataBaseConnector
from orm.TidDataBase import TidDataBaseConnector
from orm.TerminalLogger import LOGGER
from mobile import createTopicDataTask

# 总帖数：444807
target_count = 1

tid_conn = TidDataBaseConnector()
topic_data_conn = TopicDataBaseConnector()

while target_count < 444907:
    LOGGER.DEBUG(f"Getting data,the target count is {target_count}")
    tid_list = tid_conn.getTidsArrangedById(target_count)

    if len(tid_list) == 0:
        LOGGER.WARNING(f"The tid_list get no data.The target_count is {target_count}")
        break

    topic_data = createTopicDataTask(tid_list)
    topic_data_conn.addTopicRecord(topic_data)

    target_count = target_count+20










