import asyncio

from parsers.utils import BaseParser
from src.models import SourceNews


class SBRParser(BaseParser):
    BASE_URL = "https://www.cbr.ru/FPEventAndPress/"
    cookies = {
        '__ddg1_': 'XHymmnxMmKIFAhUsvMUS',
        '__ddg8_': 'NZ51YsIord6h9Agf',
        '__ddg10_': '1759523293',
        '__ddg9_': '45.135.38.65',
        'ASPNET_SessionID': '1vacw4pqobcncjsnc0gy5chy',
        'accept': '1',
        'SelectedTab': '0',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br, zstd',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
        'Referer': 'https://www.cbr.ru/',
        # 'Cookie': '__ddg1_=XHymmnxMmKIFAhUsvMUS; __ddg8_=NZ51YsIord6h9Agf; __ddg10_=1759523293; __ddg9_=45.135.38.65; ASPNET_SessionID=1vacw4pqobcncjsnc0gy5chy; accept=1; SelectedTab=0',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0',
        # Requests doesn't support trailers
        # 'TE': 'trailers',
    }

    async def collect_data(self, session, used: set[int]) -> list[SourceNews]:
        queue = set()
        to_dump = []
        total_models: list[SourceNews] = []
        for i in range(1, 100):
            params = {"page": str(i), "IsEng": "false", "type": "0", "pagesize": "10"}

            data = await self.fetch_json(session, self.BASE_URL, params=params)

            for d in data:
                _id = int(d.get("doc_htm"))
                if _id in used:
                    continue
                queue.add((_id, d.get("DT"), f"https://www.cbr.ru/press/event/?id={_id}"))

            while queue:
                _id, dt, url = queue.pop()

                html = await self.fetch_html(session, url)
                content = self.normalize_content(html)

                to_dump.append({
                    "other_id": _id,
                    "published_dttm": dt,
                    "content": content,
                    "url": url,
                })
                if len(to_dump) == 10:
                    news_models = await self.dump(to_dump)
                    total_models.extend(news_models)
                    print(f"[dump][{i}] {to_dump}")
                    to_dump.clear()

        if to_dump:
            news_models = await self.dump(to_dump)
            total_models.extend(news_models)

            print(f"[dump][-] {to_dump}")
            to_dump.clear()

        return total_models


if __name__ == "__main__":
    parser = SBRParser(
        source_title="www.crb.ru",
        dump_to_type="file",
        dump_pointer="cbr-test-1.csv"
    )
    asyncio.run(parser.run())
