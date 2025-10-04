from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models import Base


class BaseRepo[T: Base]:
    def __init__(self, session: AsyncSession, model: type[T]):
        self.session = session
        self.model = model

    async def create(self, **kwargs) -> T:
        model = self.model(**kwargs)

        self.session.add(model)
        await self.session.flush()
        await self.session.commit()

        return model

    async def get_by_id(self, model_id: int | UUID) -> T:
        stmt = select(self.model).filter(self.model.id == model_id)

        for rel in self.model.__mapper__.relationships:
            stmt = stmt.options(selectinload(rel.key))

        return await self.session.scalar(stmt)

    async def update(self, model: T, **kwargs) -> T:
        for k, v in kwargs.items():
            setattr(model, k, v)

        self.session.add(model)
        await self.session.flush()
        await self.session.commit()

        return model

    async def update_by_id(self, model_id: int | UUID, **kwargs) -> T:
        model: T = await self.session.execute(update(self.model).values(**kwargs).filter(
            self.model.id == model_id,
        ).returning(T))
        await self.session.flush()
        await self.session.commit()

        return model
