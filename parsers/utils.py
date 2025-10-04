import csv
import re
import urllib.parse
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, List, Set

import aiohttp
import torch
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from src.models import SourceNews
from src.repo import DB

labels = ["other_id", "published_dttm", "content", "url"]
device = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                            device=device)
INTERFAX_RE = re.compile(r"^.*?interfax\.ru\s*[-—–:]*\s*", flags=re.IGNORECASE)

# 2) Приветствия
GREETINGS_RE = re.compile(
    r"^\s*(доброе утро|добрый день|добрый вечер|здравствуйте|привет|уважаемые|коллеги)[\s\!\,\.\-—:]{0,5}",
    flags=re.IGNORECASE
)

# 3) URL
URL_RE = re.compile(r"https?://[^\s\)\]\}\,]+", flags=re.IGNORECASE)

# 4) Эмодзи
EMOJI_RE = re.compile("["
                      "\U0001F600-\U0001F64F"
                      "\U0001F300-\U0001F5FF"
                      "\U0001F680-\U0001F6FF"
                      "\U0001F1E0-\U0001F1FF"
                      "\U00002700-\U000027BF"
                      "\U0001F900-\U0001F9FF"
                      "\U00002600-\U000026FF"
                      "\U00002B00-\U00002BFF"
                      "]+", flags=re.UNICODE)


# ------------------- Функции -------------------

def normalize_source_token(src):
    if not isinstance(src, str) or not src.strip():
        return None
    s = src.strip().lower()
    m = re.search(r'([a-z0-9\.-]+\.[a-z]{2,})', s)
    if m:
        return m.group(1).replace('www.', '')
    return s


def remove_interfax_prefix(text):
    if not isinstance(text, str):
        return text
    return INTERFAX_RE.sub("", text, count=1).lstrip()


def remove_greeting_prefix(text):
    if not isinstance(text, str):
        return text
    return GREETINGS_RE.sub("", text, count=1).lstrip()


def remove_source_urls(text, source_value):
    if not isinstance(text, str) or not source_value:
        return text
    ns = normalize_source_token(source_value)
    if not ns:
        return text
    urls = URL_RE.findall(text)
    for u in urls:
        try:
            dom = urllib.parse.urlparse(u).netloc.lower().replace('www.', '')
        except:
            dom = u.lower()
        if ns in dom:
            text = text.replace(u, "")
    return re.sub(r"\s{2,}", " ", text).strip()


def remove_emoji(text):
    if not isinstance(text, str):
        return text
    return EMOJI_RE.sub("", text)


def generate_news_embedding(content: str, source_title) -> list[float]:
    content = remove_interfax_prefix(content)  # 1. всё до interfax.ru
    content = remove_greeting_prefix(content)  # 2. приветствия
    content = remove_source_urls(content, source_title)  # 3. ссылки на source
    content = remove_emoji(content)  # 4. эмодзи
    if isinstance(content, str):
        content = content.lower()
    embeddings = model.encode(
        [content],
        show_progress_bar=True,
        convert_to_numpy=True,
    )
    embeddings = normalize(embeddings)
    return embeddings[0].tolist()


class BaseParser(ABC):
    BASE_URL: Optional[str] = None
    headers: Dict[str, str] = {}
    cookies: Dict[str, str] = {}

    def __init__(self, source_title: str, dump_to_type: str, dump_pointer: str, *,
                 db_engine_url: Optional[str] = None,
                 concurrency: int = 5,
                 batch_size: int = 50):
        """
        :param source_title: source_title источника в БД
        :param dump_to_type: "file" or "db"
        :param dump_pointer: если file -> путь к csv; если db -> строка подключения (DSN) к БД (async)
        :param db_engine_url: альтернативная точка для создания engine (если dump_to_type == 'db')
        """
        self.source_title = source_title
        self.dump_to_type = dump_to_type  # "file" / "db"
        self.dump_pointer = dump_pointer
        self.concurrency = concurrency
        self.batch_size = batch_size

        if self.dump_to_type == "db":
            engine_url = db_engine_url or dump_pointer
            self.engine = create_async_engine(engine_url, future=True)
            self.session_maker = async_sessionmaker(bind=self.engine, class_=AsyncSession,
                                                    expire_on_commit=False)
        else:
            self.engine = None
            self.session_maker = None

        self._aio_timeout = aiohttp.ClientTimeout(total=30)

    def _get_used_file(self) -> Set[int]:
        already_parsed: Set[int] = set()
        try:
            with open(self.dump_pointer, "r", encoding="utf-8", newline="\n") as file:
                reader = csv.DictReader(file, fieldnames=labels, delimiter="|")
                next(reader)
                for r in reader:
                    try:
                        already_parsed.add(int(r["other_id"]))
                    except Exception:
                        continue
        except FileNotFoundError:
            pass
        return already_parsed

    async def _get_used_db(self) -> Set[int]:
        async with self.session_maker() as session:
            db = DB(session)
            used = await db.source_news.get_used(self.source_title)
            return set(used)

    async def get_used(self) -> Set[int]:
        """Возвращает множество использованных other_id в зависимости от режима дампа."""
        if self.dump_to_type == "file":
            return self._get_used_file()
        else:
            return await self._get_used_db()

    def _dump_file(self, data: List[Dict]):
        if not data:
            return
        write_header = False
        try:
            with open(self.dump_pointer, "r", encoding="utf-8"):
                pass
        except FileNotFoundError:
            write_header = True

        with open(self.dump_pointer, "a", encoding="utf-8", newline="\n") as file:
            writer = csv.DictWriter(file, delimiter="|", fieldnames=labels)
            if write_header:
                writer.writeheader()
            writer.writerows(data)

    async def _dump_db(self, data: List[Dict]) -> list[SourceNews]:
        if not data:
            return []
        items = []

        async with self.session_maker() as session:
            db = DB(session)
            for r in data:
                item = await db.source_news.create(
                    dttm=datetime.fromisoformat(r.get("published_dttm")),
                    source_title=self.source_title,
                    url=r.get("url"),
                    other_id=r.get("other_id"),
                    content=r.get("content"),
                    embedding=generate_news_embedding(r.get("content"), self.source_title),
                )
                items.append(item)

        return items

    async def dump(self, data: List[Dict]):
        if self.dump_to_type == "file":
            self._dump_file(data)
        else:
            return await self._dump_db(data)

    async def fetch_json(self, session: aiohttp.ClientSession, url: str,
                         params: dict = None) -> dict:
        async with session.get(url, params=params, headers=self.headers, cookies=self.cookies,
                               timeout=self._aio_timeout) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_html(self, session: aiohttp.ClientSession, url: str,
                         params: dict = None) -> str:
        async with session.get(url, params=params, headers=self.headers, cookies=self.cookies,
                               timeout=self._aio_timeout) as resp:
            raw = await resp.read()
            # пытаемся cp1251 → если не вышло, то utf-8
            try:
                return raw.decode("cp1251")
            except UnicodeDecodeError:
                return raw.decode("utf-8", errors="ignore")

    def normalize_content(self, html: str) -> str:
        """Базовый нормалайзер: собирает текст из <p> и убирает лишние пробелы."""
        soup = BeautifulSoup(html, "html.parser")
        paragraphs = [p.get_text(separator=" ", strip=True) for p in soup.find_all("p") if
                      p.get_text(strip=True)]
        return " ".join(paragraphs)

    async def run(self):
        print(f"[start-parsing] {self.source_title}")
        used = await self.get_used()
        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            new_data = await self.collect_data(session, used)
        return new_data

    @abstractmethod
    async def collect_data(self, session: aiohttp.ClientSession, used: set[int]) -> List[
        SourceNews]:
        """
        Конкретный парсер:
        - сам решает, как идти по страницам
        - сам решает, когда и чем (dump_file или dump_db) сохранять
        - обязан фильтровать по used, чтобы не грузить дубликаты
        """
        raise NotImplementedError
