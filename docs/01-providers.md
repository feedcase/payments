# 1. Тестовые провайдеры

## 1.1. Критерии выбора

1. **Бесплатная песочница с API**, без реальных денег.
2. **Возможность вызвать отказы по требованию** — иначе каскад нечем продемонстрировать.
3. **Вебхуки в песочнице** — финальный статус криптоплатежа приходит асинхронно.
4. **Релевантность заказчику** — NOWPayments уже используется в их стеке.

## 1.2. Итоговый набор

| Роль в каскаде | Провайдер | Песочница | Как симулируются исходы |
|---|---|---|---|
| Основной | **NOWPayments** | `https://api-sandbox.nowpayments.io/v1`, отдельный sandbox-аккаунт и API-ключ | Параметр `case` при создании платежа: `success`, `partially_paid`, `failure` — песочница сама прогоняет статусы и шлёт IPN |
| Резервный | **CoinGate** | `https://api-sandbox.coingate.com/v2`, отдельный sandbox-аккаунт | Оплата тестовыми BTC (testnet) или отмена/истечение заказа на hosted-странице |
| Технические отказы | **Mock-провайдер** (свой) | Внутренний HTTP-сервис в docker-compose | Сценарий задаётся конфигом/заголовком: таймаут, 5xx, медленный ответ, обрыв соединения, «призрачный» платёж, дубли и перестановка вебхуков |
| Расширение (карты) | **Stripe** (test mode) → **Braintree** (sandbox) | Бесплатные, доступны сразу | Stripe — тестовые payment methods (`pm_card_chargeDeclined`, `pm_card_visa_chargeDeclinedInsufficientFunds` и т.п.); Braintree — ответ процессора задаётся суммой (диапазон 2000–2999 — отказы) |

## 1.3. NOWPayments — что используем

| Операция | Endpoint | Зачем каскаду |
|---|---|---|
| Доступность API | `GET /status` | health-probe для circuit breaker (half-open) |
| Список монет | `GET /currencies` | pre-filter: провайдер не поддерживает `pay_currency` → пропускаем без вызова |
| Минимальная сумма | `GET /min-amount?currency_from=&currency_to=` | pre-filter: сумма ниже минимума → пропускаем |
| Оценка | `GET /estimate` | показать покупателю сумму в монете |
| Создать платёж | `POST /payment` (`price_amount`, `price_currency`, `pay_currency`, `order_id`, `ipn_callback_url`, в sandbox — `case`) | основная операция попытки; `order_id` = наш `attempt_id` |
| Статус | `GET /payment/{payment_id}` | сверка (reconciliation), проверка вебхука |
| IPN | POST на наш `ipn_callback_url`, подпись `x-nowpayments-sig` = HMAC-SHA512 от JSON с отсортированными ключами, ключ — IPN secret | вебхуки |

Статусы: `waiting`, `confirming`, `confirmed`, `sending`, `partially_paid`, `finished`,
`failed`, `refunded`, `expired`. Аутентификация — заголовок `x-api-key`.

## 1.4. CoinGate — что используем

| Операция | Endpoint | Зачем |
|---|---|---|
| Создать заказ | `POST /orders` (`order_id`, `price_amount`, `price_currency`, `receive_currency`, `callback_url`, `token`) | попытка; `order_id` = `attempt_id`, `token` — наш секрет для проверки callback |
| Статус | `GET /orders/{id}` | сверка, проверка callback |
| Callback | POST на `callback_url` | вебхук; проверяем `token` **и** перезапрашиваем заказ по API |

Статусы: `new`, `pending`, `confirming`, `paid`, `invalid`, `expired`, `canceled`,
`refunded`. Аутентификация — `Authorization: Token <api_token>`.

## 1.5. Mock-провайдер

Нужен, потому что ни одна песочница не умеет надёжно воспроизводить инфраструктурные
сбои — а именно на них каскад чаще всего и срабатывает.

Сценарии (выбираются по `X-Mock-Scenario` или по сумме, как у Braintree):

| Сценарий | Поведение | Что проверяем |
|---|---|---|
| `ok` | 201, потом вебхук `finished` | базовый путь |
| `http_500` | 500 на создание | переход к следующему провайдеру, счётчик breaker |
| `timeout` | ответ дольше read-timeout | дедлайн попытки, каскад, «призрачный» платёж у провайдера |
| `slow` | ответ за 80% таймаута | бюджет всего каскада |
| `conn_refused` | порт закрыт | ошибка соединения |
| `unsupported` | 400 «currency not supported» | пропуск без штрафа breaker'у |
| `ghost_success` | 500 на создание, но платёж создан и позже приходит вебхук `finished` | обработка «опоздавшего» платежа по замещённой попытке |
| `webhook_dup` / `webhook_reorder` | вебхуки дублируются / приходят не по порядку | идемпотентность inbox и монотонность статусов |

## 1.6. Демонстрационные сценарии каскада

| # | Настройка | Ожидаемый результат |
|---|---|---|
| D1 | NOWPayments `case=success` | Успех у первого провайдера, глубина каскада 1 |
| D2 | NOWPayments недоступен (mock на его месте отдаёт 500 / breaker открыт) → CoinGate | Счёт выставлен у CoinGate, покупатель разницы не видит |
| D3 | `pay_currency`, которой нет у NOWPayments | NOWPayments отфильтрован до вызова, сразу CoinGate |
| D4 | NOWPayments `case=failure` | Платёж `failed`; покупателю предлагается перевыставить счёт → новая попытка у следующего провайдера |
| D5 | NOWPayments `case=partially_paid` | Каскад **не** запускается; платёж `partially_paid`, срабатывает политика недоплаты |
| D6 | Счёт истёк → `reinvoice` у резервного → оплачен → покупатель оплачивает **старый** счёт | `PaymentSucceeded`, затем `PaymentOverpaid`; деньги учтены в `received_total` |
| D7 | Все провайдеры недоступны | Платёж `failed` с причиной `no_provider_available`, алерт |
| D8 | `ghost_success`: провайдер создал счёт, но ответил 500 | Каскад уходит к резервному; адрес «призрачного» счёта покупателю не показан, оплатить его нельзя (§2.4) |
