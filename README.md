# Каскадирование платежей — тестовое задание

Проект системы, которая при неудаче у одного платёжного провайдера автоматически
переводит платёж на следующего, не теряя деньги и не списывая их дважды.

## Документы

| # | Документ | О чём |
|---|----------|-------|
| 1 | [01-providers.md](docs/01-providers.md) | Выбор тестовых провайдеров, песочницы, сценарии для демонстрации |
| 2 | [02-technical-design.md](docs/02-technical-design.md) | Техническое решение: домен, машины состояний, классификация ошибок, маршрутизация, надёжность, **две реализации** (Taskiq + Redis и RabbitMQ) |

## Кратко о решении

- **Провайдеры:** NOWPayments (основной) → CoinGate (резервный) → Mock (для отказов,
  которые песочницы не умеют). Карточный каскад Stripe → Braintree описан как
  расширение того же движка.
- **Стек:** Python 3.13, FastAPI, PostgreSQL (SQLAlchemy + asyncpg + Alembic),
  Redis, Taskiq, Docker, Poetry, pydantic-settings.
- **Архитектура:** модуль `payments` внутри модульного монолита, Clean Architecture
  (domain / application / infrastructure / presentation), провайдеры — адаптеры за
  портом `PaymentProviderPort`, сущности — Pydantic-модели.
- **Ключевые решения:**
  1. Каскадируем только по *классифицированным* ошибкам; бизнес-отказы и неоднозначные
     состояния каскад останавливают.
  2. Платёж (`Payment`) и попытка (`PaymentAttempt`) — разные сущности; у каждой
     попытки свой ключ идемпотентности и свой `order_id` у провайдера.
  3. Состояние здоровья провайдеров (circuit breaker, success rate, capabilities) — в
     Redis; источник истины по деньгам — PostgreSQL.
  4. Вебхуки → inbox-таблица → асинхронная обработка; периодическая сверка (reconciliation)
     закрывает всё, что вебхуки не донесли.
  5. Две реализации оркестрации с общим доменом: **синхронный каскад + Taskiq/Redis**
     (на текущем стеке, рекомендуется для старта) и **событийная на RabbitMQ**
     (путь к микросервисам). Переход — замена infrastructure-адаптеров, домен не меняется.

## Прототип: реализация 1 (sync-каскад + Taskiq/Redis)

Код — в [src/](src/).

```
src/app/                      # FastAPI app factory, настройки, JWT, БД
src/modules/payments/
  domain/                     # агрегат Payment, статусы, классы ошибок, политика каскада
  application/                # порты, Router, CascadeOrchestrator, use cases
  infrastructure/             # Postgres (SQLAlchemy+Alembic), Redis, провайдеры, Taskiq
  presentation/               # /api/v1/payments, /webhooks/{provider}
  container.py                # сборка модуля
src/mock_provider/            # отдельный mock-сервис провайдера (сценарии отказов)
```

### Запуск и демо

```bash
docker compose up -d --build          # postgres, redis, migrate, api, worker, scheduler, mock
poetry install
poetry run python scripts/demo.py     # сценарии D1–D8 → PASS/FAIL
```

Без sandbox-ключей роли провайдеров играют два экземпляра mock (`mock_primary`, `mock_backup`);
демо-скрипт ставит маршрут `usdttrc20 → [mock_primary, mock_backup]` в `cascade_routes`.

Создать платёж вручную:

```bash
TOKEN=$(poetry run python scripts/mint_jwt.py)
curl -s localhost:8000/api/v1/payments -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' \
  -d '{"price":{"amount":"9.99","currency":"USD"},"pay_currency":"usdttrc20","purpose":{"domain":"demo","ref":"1"}}'
```

Суммы передаются **строкой** — `float` отклоняется (422).

### Реальные песочницы

В `.env` (см. [.env.example](.env.example)): `NOWPAYMENTS__ENABLED=true`, `NOWPAYMENTS__API_KEY`,
`NOWPAYMENTS__IPN_SECRET`, `NOWPAYMENTS__SANDBOX_CASE=success|partially_paid|failure`;
`COINGATE__ENABLED=true`, `COINGATE__API_TOKEN`, `COINGATE__CALLBACK_SECRET`. Маршрут по
умолчанию (`*`) — `[nowpayments, coingate]`. Вебхуки песочниц должны доходить до API:
`PUBLIC_BASE_URL` = адрес туннеля (`cloudflared tunnel --url http://localhost:8000` или ngrok).

### Тесты и проверки

```bash
docker compose up -d postgres redis
poetry run pytest                         # unit
poetry run pytest -m integration          # Postgres + Redis + e2e через mock
poetry run ruff check . && poetry run mypy src && PYTHONPATH=src poetry run lint-imports
```

### Фоновые задачи (Taskiq, расписание)

| Задача | Когда | Что делает |
|---|---|---|
| `payments.process_provider_event` | на каждый вебхук | перепроверяет статус у провайдера и применяет его |
| `payments.continue_cascade` | если каскад не уложился в дедлайн запроса | доводит каскад до конца |
| `payments.recover` | каждую минуту | зависшие шаги каскада (попытка в `CREATING` после падения процесса, `ROUTING` без движения) и необработанные вебхуки из inbox |
| `payments.reconcile` | каждую минуту | сверка: активные попытки — раз в 2 мин, замещённые/истёкшие — раз в 15 мин (ради опоздавших платежей), параллельно, с замком от перекрытия |
| `payments.expire_stale` | каждую минуту | закрывает счета, которые провайдер держит в ожидании после `expires_at` |
| `payments.relay_outbox` | каждую минуту и после изменений | публикует доменные события в Redis Stream `payments.events` |
| `payments.refresh_capabilities` | каждые 10 мин | монеты и минимальные суммы провайдеров |

Ретраи задач — с экспоненциальной паузой (`SmartRetryMiddleware` + Redis-источник расписания).
`TASK_QUEUE=inline` (без воркера и расписания) разрешён только при `ENV=dev|test`.

### Отличия прототипа от техрешения

- Маршрутизация только `PriorityStrategy`; `ScoreStrategy` (success rate) не реализована.
- Bulkhead — семафор внутри процесса, без общего лимита в Redis.
- Вместо таблицы `failed_tasks` — лог и метрика `payments_task_failed_total`.
- Наблюдаемость — Prometheus `/metrics` и логи; OpenTelemetry не подключён.
- Сверяются только попытки с известным `provider_payment_id`: счёт, созданный «вслепую»
  (таймаут), покупателю не показывался (§2.4).
- CoinGate: коды монет переводятся через `COINGATE__PAY_CURRENCY_MAP` (`usdttrc20 → USDT`);
  сеть USDT у CoinGate нужно сверить в песочнице.
