from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from src.core.get_db import get_db
from src.repo import DB

router = APIRouter(prefix="/api/v1")


@router.get("/hot")
async def get_hottest(
        start_dttm: datetime,
        end_dttm: datetime,
        k: int = 10,
        db: DB = Depends(get_db),
):
    if end_dttm >= start_dttm:
        raise HTTPException(400, "date interval is invalid")

    result = await db.source_news.get_hottest(start_dttm, end_dttm, k)

    return result

