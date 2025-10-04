from fastapi import Request, FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker
from starlette.middleware.base import BaseHTTPMiddleware

from src.repo import DB


class GetDBMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, session_pool: async_sessionmaker):
        super().__init__(app)
        self.session_pool = session_pool

    async def dispatch(self, request: Request, call_next):
        async with self.session_pool() as session:
            request.state.db = DB(session)

            response = await call_next(request)
            return response


async def get_db(request: Request) -> DB:
    return request.state.db
