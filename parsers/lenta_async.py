import argparse
import asyncio
import csv
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count

import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(name="LentaParser")


class LentaParser:
    default_parser = "html.parser"

    def __init__(self, *, max_workers: int, outfile_name: str, from_date: str):
        self._endpoint = "https://lenta.ru/news"

        self._sess = None
        self._connector = None

        self._executor = ProcessPoolExecutor(max_workers=max_workers)

        self._outfile_name = outfile_name
        self._outfile = None
        self._csv_writer = None
        self.timeouts = aiohttp.ClientTimeout(total=60, connect=60)

        self._n_downloaded = 0
        self._from_date = datetime.strptime(from_date, "%d.%m.%Y")

    @property
    def dates_countdown(self):
        date_start, date_end = self._from_date, datetime.today()

        while date_start <= date_end:
            yield date_start.strftime("%Y/%m/%d")
            date_start += timedelta(days=1)

    @property
    def writer(self):
        if self._csv_writer is None:
            self._outfile = open(self._outfile_name, "w", 1, newline="\n", encoding="utf-8")
            self._csv_writer = csv.DictWriter(
                self._outfile, fieldnames=["datetime", "url", "text"]
            )
            self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def session(self):
        if self._sess is None or self._sess.closed:
            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=1024
            )
            self._sess = aiohttp.ClientSession(
                connector=self._connector, timeout=self.timeouts
            )
        return self._sess

    async def fetch(self, url: str):
        response = await self.session.get(url, allow_redirects=False)
        response.raise_for_status()
        return await response.text(encoding="utf-8")

    @staticmethod
    def parse_article_html(html: str):
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        body = doc_tree.find("div", attrs={"class": "topic-body__content"})

        if not body:
            raise RuntimeError("Article body is not found")

        text = " ".join([p.get_text() for p in body.find_all("p")])
        return text

    @staticmethod
    def _extract_urls_from_html(html: str):
        """Возвращает список словарей: [{url, datetime}]"""
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        news_list = doc_tree.find_all("li", {"class": "archive-page__item _news"})

        results = []
        for news in news_list:
            url = f"https://lenta.ru{news.find('a')['href']}"
            time_text = news.find("time").text.split(",")[0]
            results.append({"url": url, "datetime": time_text})
        return tuple(results)

    async def _fetch_all_news_on_page(self, html: str):
        loop = asyncio.get_running_loop()
        news_items = await loop.run_in_executor(
            self._executor, self._extract_urls_from_html, html
        )

        tasks = {item["url"]: asyncio.create_task(self.fetch(item["url"])) for item in news_items}
        results = []

        for item in news_items:
            url = item["url"]
            dt = item["datetime"]

            try:
                html_page = await tasks[url]
            except Exception as exc:
                logger.error(f"Cannot fetch {url}: {exc}")
                continue

            try:
                text = await loop.run_in_executor(self._executor, self.parse_article_html, html_page)
            except Exception:
                logger.exception(f"Cannot parse {url}")
                continue

            results.append({"datetime": dt, "url": url, "text": text})

        if results:
            self.writer.writerows(results)
            self._n_downloaded += len(results)

        return len(results)

    async def shutdown(self):
        if self._sess is not None:
            await self._sess.close()
        await asyncio.sleep(0.5)

        if self._outfile is not None:
            self._outfile.close()

        self._executor.shutdown(wait=True)
        logger.info(f"{self._n_downloaded} news saved at {self._outfile_name}")

    async def _producer(self):
        for date in self.dates_countdown:
            i = 1
            while True:
                news_page_url = f"{self._endpoint}/{date}/page/{i}/"

                try:
                    html = await asyncio.create_task(self.fetch(news_page_url))
                except Exception as exc:
                    logger.error(f"Cannot fetch {news_page_url}: {exc}")
                    break

                n_proccessed_news = await self._fetch_all_news_on_page(html)
                if n_proccessed_news == 0:
                    logger.info(f"News not found at {news_page_url}.")
                    break

                logger.info(
                    f"{news_page_url} processed ({n_proccessed_news} news). "
                    f"{self._n_downloaded} news saved totally."
                )
                i += 1

    async def run(self):
        try:
            await self._producer()
        finally:
            await self.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Downloads news from Lenta.Ru")

    parser.add_argument(
        "--outfile", default="lenta-ru-news.csv", help="name of result file"
    )

    parser.add_argument(
        "--cpu-workers", default=cpu_count(), type=int, help="number of workers"
    )

    parser.add_argument(
        "--from-date",
        default="30.08.1999",
        type=str,
        help="download news from this date. Example: 30.08.1999",
    )

    args = parser.parse_args()

    parser = LentaParser(
        max_workers=args.cpu_workers,
        outfile_name=args.outfile,
        from_date=args.from_date,
    )

    try:
        asyncio.run(parser.run())
    except KeyboardInterrupt:
        asyncio.run(parser.shutdown())
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()
