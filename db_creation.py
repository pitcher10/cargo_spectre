from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

database_path = 'sqlite:///cargodata.db'

engine = create_engine(database_path)

Base = declarative_base()

class SearchResults(Base):
    __tablename__ = 'SearchResults'

    cargoID = Column(Integer, primary_key=True)
    barcode = Column(String)
    scanDate = Column(DateTime, nullable=False)
    length = Column(Float)
    width = Column(Float)
    height = Column(Float)
    weight = Column(Float)
    updated = Column(String)
    report_url = Column(String)
    detail_line_id = Column(Integer)

Base.metadata.create_all(engine)