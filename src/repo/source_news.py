import datetime
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import SourceNews
from src.repo.base_repo import BaseRepo


class SourceNewsRepo(BaseRepo[SourceNews]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SourceNews)

    async def get_used(self, source_title: str) -> Sequence[int]:
        return (await self.session.scalars(select(SourceNews.other_id).filter(
            SourceNews.source_title == source_title,
        ))).all()

    async def get_last_for_n_days(self, days: int) -> Sequence[SourceNews]:
        now = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
        prev = now - datetime.timedelta(days=days)
        return (await self.session.scalars(select(SourceNews).filter(
            SourceNews.dttm > prev
        ))).all()
