from sqlalchemy.ext.asyncio import AsyncSession

from src.repo.source_news import SourceNewsRepo


class DB:
    def __init__(self, session: AsyncSession):
        self.source_news = SourceNewsRepo(session)
