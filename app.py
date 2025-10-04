import logging

import uvicorn
from config.config import load_config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine
from src.core.get_db import GetDBMiddleware

from src.models import Base
from src.router import routers

logger = logging.getLogger(__name__)
config = load_config()

main_engine = create_async_engine(config.db.alchemy_url)

MainAsyncSessionLocal = async_sessionmaker(
    bind=main_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

app = FastAPI()
app.include_router(routers)

app.add_middleware(GetDBMiddleware, session_pool=MainAsyncSessionLocal)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(filename)s:%(lineno)d #%(levelname)-8s '
           '[%(asctime)s] - %(name)s - %(message)s',
)


@app.on_event("startup")
async def main():
    async with main_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info('Starting services_api')


if __name__ == '__main__':
    uvicorn.run('app:app', host="0.0.0.0", port=8000, reload=True)
