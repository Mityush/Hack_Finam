
import csv
import time
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest
import asyncio
import sys

# === ВАШИ ДАННЫЕ ===
api_id = 26607213
api_hash = 'febf5611110a86d33f8ca2bd755edb95'

# === УКАЖИТЕ ДАТУ НАЧАЛА ПАРСИНГА (в UTC!) ===
START_DATE = datetime(2022, 1, 1, tzinfo=timezone.utc)

# === СПИСОК КАНАЛОВ ===
channels = [
    'markettwits',
    'cbrstocks',
    'warningbuffet',
    'tb_invest_official',
    't_analytics_official',
    'bitkogan',
    'AK47pfl',
    'centralbank_russia',
    'tass_agency',
    'rbc_news',
    'ProfitGate',
    'FatCat18',
    'usa100cks',
    'russianmacro',
    'BIoomberg',
    'smartlabnews',
    'banksta',
    'headlines_for_traders',
    'bitkogan_hotline',
    'thewallstreetpro',
    'bankrollo',
    'economica',
    'alfa_investments',
    'preemnik',
    'SberInvestments',
    'forbesrussia',
    'bezposhady',
    'lentadnya',
    'rian_ru',
    'vedomosti'
]

client = TelegramClient('session_name', api_id, api_hash)

# === ГЛОБАЛЬНЫЙ СЕМАФОР для ограничения параллелизма ===
# Ограничиваем одновременные запросы к Telegram (рекомендуется 3–5)
MAX_CONCURRENT = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# === ФУНКЦИЯ: получить число подписчиков ===
async def get_subscribers(channel_username):
    try:
        channel = await client.get_entity(channel_username)
        full = await client(GetFullChannelRequest(channel))
        return full.full_chat.participants_count
    except Exception as e:
        print(f"⚠️ Не удалось получить подписчиков для {channel_username}: {e}")
        return None

# === АСИНХРОННАЯ ФУНКЦИЯ: сохранить сообщения в CSV ===
async def save_messages_to_csv(channel, messages, subs):
    filename = f"{channel}.csv"
    fieldnames = ['channel', 'sender_id', 'message_id', 'date', 'text', 'subs', 'views']

    def _write_csv():
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for msg in messages:
                date_str = msg.date.strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow({
                    'channel': channel,
                    'message_id': msg.id,
                    'sender_id': msg.sender_id,
                    'date': date_str,
                    'text': msg.text or '',
                    'subs': subs,
                    'views': msg.views
                })

    await asyncio.to_thread(_write_csv)
    print(f"✅ Сохранено {len(messages)} сообщений в {filename}")

# === ФУНКЦИЯ: собрать и сохранить сообщения с даты (с защитой семафором) ===
async def fetch_and_save_channel(channel):
    filename = f"{channel}.csv"

    min_id = 10000000000000
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)  # fieldnames не нужны, если есть заголовок
            rows = list(reader)
            if rows:
                min_id = min(int(r.get("message_id", 0)) for r in rows if r.get("message_id") and r["message_id"].isdigit())
    except Exception as e:
        pass
    async with semaphore:  # Ограничиваем параллелизм
        print(f"\n📥 Начинаю обработку канала: {channel} c сообщения {min_id}")
        subs = await get_subscribers(channel)
    messages = []
    i = 0
    try:
        async for message in client.iter_messages(channel):
            if message.id > min_id:
                continue
            i += 1
            if i % 1000 == 0:
                print(f"{channel}: обработано {i} сообщений")
                await save_messages_to_csv(channel, messages, subs)
                messages.clear()

            if message.date < START_DATE:
                break
            messages.append(message)

    except FloodWaitError as e:
        print(f"⏳ FloodWait в {channel}: ждём {e.seconds} секунд...")
        await asyncio.sleep(e.seconds)
        # После ожидания можно попробовать продолжить, но для простоты прерываем
        return
    except Exception as e:
        print(f"❌ Ошибка при получении сообщений из {channel}: {e}")
        return

    if not messages:
        print(f"ℹ️ Нет сообщений в {channel} с {START_DATE.date()}")
        return

