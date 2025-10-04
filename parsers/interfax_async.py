import asyncio
from datetime import datetime, date as datetime_date
from typing import Set, List, Dict

from bs4 import BeautifulSoup

from parsers.utils import BaseParser


class InterfaxParser(BaseParser):
    BASE_URL = "https://www.interfax.ru/news/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Referer': 'https://www.interfax.ru/',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Connection': 'keep-alive',
        'Priority': 'u=0, i',
    }

    async def collect_data(self, session, used: Set[int]):
        """
        Собираем новости по дням и сами решаем, когда делать дамп (по дню)
        """

        today = datetime_date.today()
        return await self.parse_day(session, today.year, today.month, today.day, used)

    async def parse_day(self, session, year: int, month: int, day: int, used: Set[int]):
        """
        Парсим все страницы одного дня, делаем дамп после обработки дня
        """
        to_dump: List[Dict] = []
        total_items = []

        queue = set()
        year_s, month_s, day_s = str(year), str(month), str(day)

        html = await self.fetch_html(session,
                                     f'{self.BASE_URL}{year_s}/{month_s}/{day_s}/all/page_1')
        if not html:
            print(f"[error] cannot fetch page 1 ({year}-{month}-{day})")
            return

        soup = BeautifulSoup(html, "html.parser")
        queue.update(self.extract_news_from_soup(soup, used, year_s, month_s, day_s))

        pages_div = soup.find("div", {"class": "pages"})
        pages_count = len(pages_div.find_all("a")) if pages_div else 1

        for i in range(2, pages_count + 1):
            html = await self.fetch_html(session,
                                         f'{self.BASE_URL}{year_s}/{month_s}/{day_s}/all/page_{i}')
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            queue.update(self.extract_news_from_soup(soup, used, year_s, month_s, day_s))

        print(f"[date: {year}-{month}-{day}] total: {len(queue)}")
        total = len(queue)
        sem = asyncio.Semaphore(15)

        async def process(news_id, published_dt, url):
            async with sem:
                html = await self.fetch_html(session, url)
                if not html:
                    print(f"[skipped] {url}")
                    return None
                soup = BeautifulSoup(html, "html.parser")
                news_box = soup.find("article", {"itemprop": "articleBody"})
                if news_box is None:
                    print(f"[skipped] {url}")
                    return None
                content = " ".join(p.text for p in news_box.find_all("p") if p.text)
                return {
                    "other_id": news_id,
                    "published_dttm": published_dt,
                    "content": content,
                    "url": url,
                }

        tasks = [process(nid, dt, url) for nid, dt, url in queue]
        results = await asyncio.gather(*tasks)

        for item in results:
            if item:
                to_dump.append(item)
                used.add(item["other_id"])

        if to_dump:
            items = await self.dump(to_dump)
            total_items.extend(items)
            print(f"[date: {year}-{month}-{day}] dumped {len(to_dump)}")
            to_dump.clear()
        return total_items

    @staticmethod
    def extract_news_from_soup(soup, used: Set[int], year_s: str, month_s: str, day_s: str):
        """
        Собираем (id, dt, url) из HTML страницы
        """
        queue = set()
        for d in soup.find_all("div", {"data-id": lambda x: x}):
            _id = int(d.get("data-id"))
            if _id in used:
                continue
            a_tag = d.find("a")
            url = a_tag.get("href")
            if url.startswith("http"):
                continue
            dt_span = d.find("span")
            dt = datetime.strptime(f"{year_s}-{month_s}-{day_s} {dt_span.text}", "%Y-%m-%d %H:%M") \
                .strftime("%Y-%m-%dT%H:%M:%S")
            queue.add((_id, dt, "https://www.interfax.ru" + url))
        return queue


if __name__ == "__main__":
    parser = InterfaxParser(
        source_title="www.interfax.ru",
        dump_to_type="file",
        dump_pointer="interfax2025.csv"
    )
    asyncio.run(parser.run())
