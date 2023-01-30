"""用于与存储主题帖内容的数据库的连接

该数据库只有一张表，名为topic。包含主题帖内容。
数据存储设计参考 枝江存档姬
"""

from sqlalchemy import create_engine,Integer,String
from sqlalchemy.orm import registry, mapped_column,Mapped
from sqlalchemy.orm import Session
from .TerminalLogger import LOGGER

mapper_registry = registry()

def EncodeErrorHandler(ele_dict:dict):
    """针对编码错误的数据，尝试进行忽略处理"""
    for key in ele_dict:
        if type(ele_dict[key]) is str:
            ele_dict[key] = ele_dict[key].encode('utf-8','ignore').decode('utf-8')
    return ele_dict

@mapper_registry.mapped
class Topic(object):
    """
    :Member
            __tablename__:该表的名称。

            tid:该帖的id。即'topic id'的缩写。

            title:该帖的标题。
            url:该帖的链接。
            create_time:该帖的创建时间。
            content:该帖的主楼内容。为一个由html字符串。

            author_name:楼主的用户名称。请注意，这存储的是数据收集时该用户的用户名，而不是用户不会改变的id。该用户名可被用户自己更改。
            author_id:楼主的用户id。为唯一标识，不会改变。
            author_uid:楼主的用户uid。可被用户改变。如我的uid便是leewendao。
            author_url:楼主的用户主页链接。请注意，该链接是以uid标识的，存在过期风险。二次使用建议使用下面的true_url。
            author_true_url:楼主的用户的主页链接，以id标识。不会改变。
            author_avatar:楼主的用户头像的url链接。请注意，这存储的是数据收集时该用户的头像url链接，其可被用户自己更改。
    """

    __tablename__ = "Topic"

    tid = mapped_column(Integer,primary_key=True)

    title: Mapped[str]
    url: Mapped[str]
    create_time: Mapped[str]
    content: Mapped[str]
    author_name: Mapped[str]
    author_id: Mapped[str]
    author_uid: Mapped[str]
    author_url: Mapped[str]
    author_true_url: Mapped[str]
    author_avatar: Mapped[str]


class TopicDataBaseConnector(object):

    db_path = r'sqlite:///./topic_content_data.db'
    engine = create_engine(db_path)
    add_count = 0

    def __init__(self):
        mapper_registry.metadata.create_all(bind=self.engine)

    def addTopicRecord(self, data: list[dict]):
        """添加一条或多条主题帖记录

        :Args
            data:主题帖记录数据，应该是一个包括'tid''title''url'等键的字典组成的列表。
        """
        with Session(self.engine) as session:
            for ele in data:
                try:
                    one_data = Topic(
                        tid=ele['id'],
                        title=ele['title'],
                        url=ele['url'],
                        create_time=ele['create_time'],
                        content=ele['content'],
                        author_name=ele['author_name'],
                        author_id=ele['author_id'],
                        author_uid=ele['author_uid'],
                        author_url=ele['author_url'],
                        author_true_url=ele['author_true_url'],
                        author_avatar=ele['author_avatar']
                    )
                    session.add(one_data)
                    session.commit()
                except UnicodeEncodeError as e:
                    """一些帖子存在乱码数据。对此需要进行对应的乱码字符的忽略处理。"""
                    LOGGER.WARNING("Detect UnicodeEncodeError,try to encode with ignoring.....")
                    LOGGER.WARNING(f"The target tid is {ele['id']}")
                    ele = EncodeErrorHandler(ele)
                    one_data = Topic(
                        tid=ele['id'],
                        title=ele['title'],
                        url=ele['url'],
                        create_time=ele['create_time'],
                        content=ele['content'],
                        author_name=ele['author_name'],
                        author_id=ele['author_id'],
                        author_uid=ele['author_uid'],
                        author_url=ele['author_url'],
                        author_true_url=ele['author_true_url'],
                        author_avatar=ele['author_avatar']
                    )
                    session.rollback()
                except Exception as e:
                    LOGGER.ERROR(f"Errors occur,msg is {e}")
                    LOGGER.ERROR(f"The target tid is {ele['id']}")
                    break
                finally:
                    session.add(one_data)
                    session.commit()
                    continue
            session.commit()
        return self



