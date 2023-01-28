"""用于与存储主题帖内容的数据库的连接

该数据库只有一张表，名为topic。包含主题帖内容。
数据存储设计参考 枝江存档姬
"""

from sqlalchemy import create_engine,Integer,String
from sqlalchemy.orm import registry, mapped_column,Mapped
from sqlalchemy.orm import Session

mapper_registry = registry()

@mapper_registry.mapped
class Topic(object):

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
        return self



