""" 定义存储小组帖子tid的数据库表的表模型定义以及ORM连接
"""
from sqlalchemy import create_engine,Integer,String,Select
from sqlalchemy.orm import registry, mapped_column
from sqlalchemy.orm import Session
from .TerminalLogger import LOGGER

mapper_registry = registry()

@mapper_registry.mapped
class TidData(object):
    __tablename__ = "Tid"

    id = mapped_column(Integer,primary_key=True,autoincrement=True)
    topic_id = mapped_column(Integer)
    title = mapped_column(String)
    author = mapped_column(String)
    author_id = mapped_column(String)


class TidDataBaseConnector(object):

    db_path = r'sqlite:///./tid_data.db'
    engine = create_engine(db_path)
    add_count = 0

    def __init__(self):
        mapper_registry.metadata.create_all(bind=self.engine)
        LOGGER.DEBUG('TidDataBase Instance Construct')

    def addTidRecords(self,data:list[dict]):
        """添加主题帖tid等数据

        :Args
            data:由存在'tid''title''author''author_id'这四个键的字典组成的列表。
        """
        with Session(self.engine) as session:
            for ele in data:
                one_data = TidData(
                    topic_id=ele['tid'],
                    title=ele['title'],
                    author=ele['author'],
                    author_id=ele['author_id']
                )
                session.add(one_data)
            session.commit()
            LOGGER.DEBUG('Add one tid record.Add_Count = :{add_count}'.format(add_count=self.add_count))
            self.add_count = self.add_count + 25
        return self

    def getTidsArrangedById(self,start_id:int):
        """根据指定id，返回自该id起的后20条tid记录"""
        final_id = start_id + 20
        sql = Select(TidData.topic_id).where(TidData.id >= start_id,TidData.id < final_id)
        with Session(self.engine) as session:
            results = session.scalars(sql).all()
            return results

