import asyncio
import time
from itertools import chain

import hdbscan
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_distances
from sklearn.preprocessing import normalize
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from config.config import load_config
from parsers.cbr_sync import SBRParser
from parsers.interfax_async import InterfaxParser
from src.models import SourceNews
from src.repo import DB


async def get_all_last_news(db_url: str) -> list[SourceNews]:
    sbr = SBRParser(
        source_title="www.cbr.ru",
        dump_to_type="db",
        dump_pointer=db_url
    )

    interfax = InterfaxParser(
        source_title="www.interfax.ru",
        dump_to_type="db",
        dump_pointer=db_url
    )

    results = await asyncio.gather(
        # sbr.run(),
        interfax.run()
    )
    print(results)
    return list(chain.from_iterable(results))


async def get_duplicate_count(news: SourceNews, prevs: list[SourceNews]) -> int:
    """
    Определяет, является ли news новой или дубликатом среди prevs.
    Все объекты уже содержат поле .embedding (numpy-массив или list).
    Возвращает количество дубликатов (0 — если новая).
    """
    if not prevs:
        return 0

    # Преобразуем эмбеддинги в numpy
    prev_embs = [np.array(p.embedding) for p in prevs if p.embedding is not None]
    news_emb = np.array(news.embedding)

    if len(prev_embs) == 0:
        return 0

    # Собираем все эмбеддинги вместе
    all_embs = np.vstack([prev_embs, news_emb[None, :]])
    all_embs = normalize(all_embs)

    # Кластеризация HDBSCAN
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=2,
        metric="euclidean",  # на нормализованных векторах ≈ косинусная
        cluster_selection_method="leaf"
    )
    labels = clusterer.fit_predict(all_embs)

    news_label = labels[-1]  # последняя — текущая новость

    # Если выброс (новая новость)
    if news_label == -1:
        return 0

    # Считаем количество прошлых новостей в том же кластере
    duplicate_count = int(np.sum(labels[:-1] == news_label))
    return duplicate_count


async def get_line(target_news: SourceNews, news: list[SourceNews]) -> list[SourceNews] | None:
    """
    Находит сюжетную линию, связанную с target_news.
    1. Внутри каждого источника строятся кластеры новостей за последние 7 дней.
    2. Кластеры усредняются и между источниками ищутся связи (глобальные мета-кластеры).
    3. Возвращает цепочку новостей (самые ранние упоминания) или None.
    """

    if not news:
        return None

    # ------------------------------
    # 1. Фильтрация новостей по времени
    # ------------------------------
    target_time = pd.to_datetime(target_news.dttm)
    news_7d = [
        n for n in news
        if n.dttm >= target_time - pd.Timedelta(days=7) and n.dttm <= target_time
    ]

    if not news_7d:
        print("no 7d news")
        return None

    # ------------------------------
    # 2. Формируем DataFrame
    # ------------------------------
    df = pd.DataFrame([{
        'id': n.id,
        'source_title': n.source_title,
        'dttm': pd.to_datetime(n.dttm),
        'embedding': np.array(n.embedding),
        'is_target': n.id == target_news.id
    } for n in news_7d + [target_news]])

    df = df.reset_index(drop=True)
    embeddings = np.vstack(df['embedding'].to_numpy())
    embeddings_norm = normalize(embeddings)

    # ------------------------------
    # 3. Кластеризация внутри источников
    # ------------------------------
    subclusters = []
    for source, idxs in df.groupby('source_title').groups.items():
        idxs = np.array(idxs)
        if len(idxs) < 3:
            continue
        emb_sub = embeddings_norm[idxs]
        clusterer = hdbscan.HDBSCAN(
            metric='euclidean',
            min_cluster_size=2,
            min_samples=3,
            cluster_selection_method='leaf'
        )
        labels = clusterer.fit_predict(emb_sub)
        df.loc[idxs, 'subcluster'] = labels
        subclusters.append((source, np.unique(labels).tolist()))

    # ------------------------------
    # 4. Если target_news не попала ни в один кластер — сюжет не найден
    # ------------------------------
    row_target = df[df['is_target']].iloc[0]
    if pd.isna(row_target.get('subcluster')) or row_target['subcluster'] == -1:
        print("no target")
        return None

    # ------------------------------
    # 5. Средние эмбеддинги подкластеров (сюжетов)
    # ------------------------------
    cluster_embs = []
    cluster_meta = []
    for (src, subcl), grp in df.groupby(['source_title', 'subcluster']):
        if subcl == -1 or pd.isna(subcl):
            continue
        idx = grp.index
        mean_emb = embeddings_norm[idx].mean(axis=0)
        cluster_embs.append(mean_emb)
        cluster_meta.append({
            'source_title': src,
            'subcluster': subcl,
            'size': len(grp)
        })

    if not cluster_embs:
        print("no cluster_embs")
        return None

    cluster_embs = np.vstack(cluster_embs)
    cluster_meta = pd.DataFrame(cluster_meta)

    # ------------------------------
    # 6. Кластеризация между источниками
    # ------------------------------
    dist_matrix = cosine_distances(cluster_embs)
    clusterer_global = hdbscan.HDBSCAN(
        metric='precomputed',
        min_cluster_size=2,
        min_samples=2,
        cluster_selection_method='eom'
    )
    meta_labels = clusterer_global.fit_predict(dist_matrix)
    cluster_meta['meta_cluster'] = meta_labels

    # ------------------------------
    # 7. Присоединяем мета-кластеры обратно к df
    # ------------------------------
    df = df.merge(cluster_meta, on=['source_title', 'subcluster'], how='left')

    target_meta_cluster = df.loc[df['is_target'], 'meta_cluster'].values[0]
    if pd.isna(target_meta_cluster) or target_meta_cluster == -1:
        print("no target_meta_cluster == -1")
        return None

    # ------------------------------
    # 8. Формируем сюжетную линию (по времени)
    # ------------------------------
    storyline = (
        df[df['meta_cluster'] == target_meta_cluster]
        .sort_values('dttm')
        .to_dict('records')
    )

    # Восстанавливаем оригинальные объекты
    id_to_news = {n.id: n for n in news_7d + [target_news]}
    chain = [id_to_news[r['id']] for r in storyline if r['id'] in id_to_news]
    if chain is None:
        print("chain is None")
    return chain if chain else None


async def process_model(
        target_news: SourceNews,
        line: list[SourceNews],
        duplicate_count: int,
) -> {}:
    ...
    return {
        "tickers": {
            "ticker_name": {
                "hotness": float,
                "sector": str,
                "shap": dict,
            }
        },
        "target_news_id": int,
        "description": str,
    }


async def main():
    config = load_config()
    engine = create_async_engine(config.db.alchemy_url, future=True)
    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession,
                                       expire_on_commit=False)

    while True:
        news = await get_all_last_news(config.db.alchemy_url)
        print(news)
        originals = []
        for n in sorted(news, key=lambda x: x.dttm, reverse=True):

            async with session_maker() as session:
                db = DB(session)

                prev_news = await db.source_news.get_last_for_n_days(2)
            async with session_maker() as session:
                db = DB(session)
                duplicate_count = await get_duplicate_count(n, prev_news)
                if duplicate_count == 0:
                    n = await db.source_news.update(n, is_original=True)
                    originals.append((n, duplicate_count))

        if not originals:
            print("no originals")
            continue
        print(originals)
        print(21312313)
        prev_10_days = await db.source_news.get_last_for_n_days(10)
        for o, dup_cnt in originals:
            line = await get_line(o, prev_10_days)
            if line is not None:
                print('===start line')
                for l in line:
                    print(l.content)
                print('===end line')

            res = await process_model(o, line, dup_cnt)


        time.sleep(5 * 60)


if __name__ == '__main__':
    asyncio.run(main())
