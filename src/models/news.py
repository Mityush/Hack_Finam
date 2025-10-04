from pgvector.sqlalchemy import VECTOR
from sqlalchemy import Column, Boolean, Integer, ForeignKey, String, DateTime, BigInteger, Text, \
    Float, \
    UniqueConstraint
from sqlalchemy.orm import relationship

from src.models.base import Base


class SourceNews(Base):
    __tablename__ = "source_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dttm = Column(DateTime, nullable=False, index=True)
    url = Column(String, nullable=False)
    source_title = Column(String)
    other_id = Column(BigInteger)
    content = Column(Text)
    embedding = Column(VECTOR(384))

    is_original = Column(Boolean)


class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    news_id = Column(Integer, ForeignKey("source_news.id"))
    duplicate_count = Column(Integer, default=0)
    timeline_length = Column(Integer, default=1)
    sources_count = Column(Integer, default=1)

    tickers = relationship(
        "NewsTickerValue",
        back_populates="news",
        cascade="all, delete-orphan"
    )


class Ticker(Base):
    __tablename__ = "tickers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String, nullable=True)


class NewsTickerValue(Base):
    __tablename__ = "news_ticker_values"

    id = Column(Integer, primary_key=True, autoincrement=True)
    news_id = Column(Integer, ForeignKey("news.id", ondelete="CASCADE"), nullable=False)
    ticker_id = Column(Integer, ForeignKey("tickers.id", ondelete="CASCADE"), nullable=False)
    hotness = Column(Float, nullable=False)

    news = relationship("News", back_populates="tickers", uselist=False)
    ticker = relationship("Ticker", uselist=False)

    __table_args__ = (
        UniqueConstraint("news_id", "ticker_id", name="uq_news_ticker"),
    )
