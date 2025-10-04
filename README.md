
# Запуск проекта в Docker

1. **Создать `.env`** с переменными проекта, например:

```env
DB__USERNAME=postgres
DB__PASSWORD=postgres
DB__HOST=db
DB__PORT=5432
DB__TABLE=radar
DB__DO_BACKUP=no
```

2. **Запустить контейнеры:**

```bash
docker-compose up --build -d
```

3. **Доступ к сервисам:**

* API: `http://localhost:8000`
* PostgreSQL: `localhost:5432` (пользователь: `postgres`, БД: `radar`)

4. **Перезапуск парсеров после изменений:**

```bash
docker-compose restart parsers
```

5. **Остановка и очистка контейнеров:**

```bash
docker-compose down
```

> Данные PostgreSQL хранятся в volume `postgres_data`.

