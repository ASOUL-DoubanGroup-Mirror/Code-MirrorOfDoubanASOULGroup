"""用于与存储主题帖回复的数据库的连接

该数据库有多张表，其中一类表存储回复数据，由帖子的tid命名；另一类表只有一张，名为Index，存储数据库中存储的帖子tid索引。
数据存储设计参考 枝江存档姬
数据存储键名及说明请参考下方代码。

Coded by Wendaolee https://github.com/WendaoLee
"""

from sqlalchemy import create_engine, Integer, String
from sqlalchemy.orm import registry, mapped_column, Mapped
from sqlalchemy.orm import Session
from .TerminalLogger import LOGGER

mapper_registry = registry()


@mapper_registry.mapped
class Index(object):
    """储存的索引表。列出所有该数据库中存储的帖子的tid

    :Member
        __tablename__:该表的名称。
        id:主键。id自增。主要用于判断存储的记录总数量。
        tid:存储的回复的对应帖子的tid。
    """
    __tablename__ = "Index"

    id = mapped_column(Integer, primary_key=True)
    tid = mapped_column(Integer)

    #将类示例转为字典的自带方法
    def to_dict(self):
        return {ele.name: getattr(self, ele.name) for ele in self.__table__.columns}


def get_comment_model(tid):
    """用于需要SQL语句的代码块中调用，获取对应tid表的ORM定义"""

    class comment_model(object):
        """存储某个主题帖的所有回复的ORM的定义。

        :Member
            __tablename__:该表的名称。以表的tid命名。
            reply_id:该回复的id。总是唯一。故作为主键。
            author_name:发出该回复的用户名称。请注意，这存储的是数据收集时该用户的用户名，而不是用户不会改变的id。该用户名可被用户自己更改。
            author_avatar:用户头像的url链接。请注意，这存储的是数据收集时该用户的头像url链接，其可被用户自己更改。
            author_id:发出该回复的用户id。为唯一标识，不会改变。
            author_uid:发出该回复的用户uid。可被用户改变。如我的uid便是leewendao。
            author_url:发出该回复的用户的主页链接。请注意，该链接是以uid标识的，存在过期风险。二次使用建议使用下面的true_url。
            author_true_url:发出该回复的用户的主页链接，以id标识。不会改变。
            author_register_time:发出该回复的用户的注册时间。
            reply_text:回复内容。
            reply_time:回复时间。

            ref_id:如果该回复是回复其他回复的回复，那么会存在对应回复的id，否则为空。
            ref_storge:如果该回复是回复其他回复的回复，那么该项存储对应回复的json数据。否则为空。

            photo:如果该回复带有图片，那么该项存储图片的url链接。
        """

        __tablename__ = tid

        reply_id = mapped_column(String, primary_key=True)

        author_name: Mapped[str]
        author_avatar: Mapped[str]
        author_id: Mapped[str]
        author_uid: Mapped[str]
        author_url: Mapped[str]
        author_true_url: Mapped[str]
        author_register_time: Mapped[str]
        reply_text: Mapped[str]
        reply_time: Mapped[str]

        ref_id = mapped_column(String, nullable=True)
        ref_storge = mapped_column(String, nullable=True)
        photo = mapped_column(String, nullable=True)

        def to_dict(self):
            return {ele.name: getattr(self, ele.name) for ele in self.__table__.columns}

    mapper_registry.mapped(comment_model)

    return comment_model


class CommentDataBaseConnector(object):

    db_path = r'sqlite:///./topic_comments_data.db'
    engine = create_engine(db_path)
    target_topic_schema = None
    target_comment_class = None
    table_schemas = []

    def __init__(self):
        LOGGER.DEBUG('DataBaseConnector has been create....')

        mapper_registry.metadata.create_all(bind=self.engine)

        self.target_topic_schema = mapper_registry.metadata.tables['Index']

        for key in mapper_registry.metadata.tables:
            self.table_schemas.append(
                mapper_registry.metadata.tables[key]
            )

    def addComments(self, tid: str, data: list[dict]):
        """添加一条主题帖的回复记录

        :Args
            tid:帖子id
            data:commentsData。格式为[commentsDict]
        """
        model = get_comment_model(tid)
        with Session(self.engine) as session:
            for ele in data:
                ref_id = 'null'
                ref_storge = 'null'
                photo = 'null'
                if 'ref_id' in ele:
                    ref_id = ele['ref_id']
                if 'ref_storge' in ele:
                    ref_storge = str(ele['ref_storge'])
                if 'photo' in ele:
                    photo = ele['photo']
                one_data = model(
                    reply_id=ele['reply_id'],

                    author_name=ele['author_name'],
                    author_avatar=ele['author_avatar'],
                    author_id=ele['author_id'],
                    author_uid=ele['author_uid'],
                    author_url=ele['author_url'],
                    author_true_url=ele['author_true_url'],
                    author_register_time=ele['author_register_time'],
                    reply_text=ele['reply_text'],
                    reply_time=ele['reply_time'],

                    ref_id=ref_id,
                    ref_storge=ref_storge,
                    photo=photo
                )
                session.add(one_data)
            session.commit()

        mapper_registry.metadata.remove(
            mapper_registry.metadata.tables[tid]
        )

        return self

    def createCommentsTable(self, tid: str):
        """创建一张评论表，并自清定义"""
        model = get_comment_model(tid)
        mapper_registry.metadata.create_all(bind=self.engine)
        mapper_registry.metadata.remove(
            mapper_registry.metadata.tables[tid]
        )
        with Session(self.engine) as session:
            session.add(
                Index(tid=tid)
            )
            session.commit()
        return self