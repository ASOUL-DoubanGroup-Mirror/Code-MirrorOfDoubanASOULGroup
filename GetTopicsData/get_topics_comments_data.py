"""运行获取主题帖回复的数据的脚本。

Coded by Wendaolee https://github.com/WendaoLee
"""

from orm.CommentsDataBase import CommentDataBaseConnector
from orm.TidDataBase import TidDataBaseConnector
from orm.TerminalLogger import LOGGER
from mobile import createTopicsCommentDataTask

tid_conn = TidDataBaseConnector()
comment_conn = CommentDataBaseConnector()

target_count = 1
stop_count = 500000

try:
    while target_count < stop_count:
        LOGGER.INFO("Target_Count is [{target_count}]".format(target_count=target_count))
        tid_list = tid_conn.getTidsArrangedById(target_count)
        data = createTopicsCommentDataTask(tid_list)

        if len(tid_list) == 0:
            LOGGER.WARNING(f"The tid_list get no data.The target_count is {target_count}")
            break

        LOGGER.INFO(f"Finish getting comments data of topic.The first tid is {tid_list[0]}")

        for tid in data:
            comment_conn.createCommentsTable(tid)
            comment_conn.addComments(tid, data[tid])

        target_count = target_count + 20
except Exception as e:
    LOGGER.ERROR(f"Error occurs,msg is {e}")
    LOGGER.ERROR(f"The target_count is {target_count}")
