# Binance + Aster order books

Небольшой сервис на Rust: два независимых публичных стакана **ETHUSDT perpetual, USDT-margined**, локальное хранение и игрушечный сигнал относительно Aster. Только market-data; ордера и приватные API не используются.

## Запуск

```sh
cargo run --release --locked
RUST_LOG=info,hft_test_task=debug cargo run --release --locked
cargo test --locked
```

Rust stable, версия зафиксирована в `rust-toolchain.toml`. CLI/ENV:

| Аргумент | ENV | По умолчанию |
| --- | --- | --- |
| `--max-age-ms` | `HFT_MAX_AGE_MS` | 500 |
| `--max-skew-ms` | `HFT_MAX_SKEW_MS` | 250 |
| `--min-depth-qty` | `HFT_MIN_DEPTH_QTY` | 1 ETH на каждую сторону |
| `--threshold-bps` | `HFT_THRESHOLD_BPS` | 5 |

Параметры проверяются перед подключением. SIGINT/SIGTERM останавливают все потоки и сбрасывают очередь логов. Exit code: 0 — штатный выход, 1 — ошибка конфигурации/сервиса, 2 — ошибка синтаксиса CLI.

## Источники

| | Binance USDⓈ-M | Aster Futures |
| --- | --- | --- |
| WS | `wss://fstream.binance.com/public/stream?streams=ethusdt@depth@100ms` | `wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms` |
| Snapshot | `https://fapi.binance.com/fapi/v1/depth?symbol=ETHUSDT&limit=1000` | `https://fapi.asterdex.com/fapi/v3/depth?symbol=ETHUSDT&limit=1000` |
| Metadata | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `https://fapi.asterdex.com/fapi/v3/exchangeInfo` |

Поток — `ethusdt@depth@100ms`. Metadata проверяет рынок, валюты и tick/lot size выбранного инструмента.

Правила синхронизации: [Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly), [Aster](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md#how-to-manage-a-local-order-book-correctly).

## Книга и восстановление

Сначала буферизуем WS, получаем snapshot с ID `L`, отбрасываем diff с `u < L`. Первый diff должен удовлетворять `U <= L <= u`, последующие — `pu == previous_u`. Количество абсолютное; ноль удаляет уровень. Точный дубль игнорируется, конфликтующий дубль/регрессия/gap вызывают resync.

Каждый адаптер единолично пишет в свои отсортированные `Vec`: bids по убыванию, asks по возрастанию. Декодер один раз проверяет и сортирует уровни. Merge выполняется через предвыделенный scratch; обе стороны меняются только после успешной проверки. Полная известная книга копируется в предвыделенный слот triple buffer. Робот читает целостную версию до следующего `update()`. Две биржи не образуют общий атомарный снимок.

Snapshot конечен: достоверны bids не ниже его нижней границы и asks не выше верхней. Для сигнала нужны пять достоверных уровней на каждой стороне; потеря покрытия приводит к Offline/resync. Книга хранит также известные уровни за этими границами.

При обнаружении разрыва, плохого кадра, переполнения или потери покрытия публикуется **Offline до ожидания восстановления**. Второй фид продолжает работать. Повторная синхронизация выполняется через snapshot + diff; обрыв соединения дополнительно требует reconnect. Молчание обнаруживает watchdog за 3 с; Ping и дубли не освежают данные. Таймаут подключения/HTTP — 10 с, синхронизации — 15 с после backoff. База backoff растёт от 250 мс до 30 с, jitter — половина базы…база; учитывается `Retry-After`.

## Сигнал

Робот использует только опубликованные книги. Если одна Offline, данные устарели, skew превышен или объём top-5 недостаточен — `Hold`. Иначе сравнивает mid Aster с mid Binance:

- Aster дешевле более чем на порог → `LongBias` относительно Aster.
- Aster дороже более чем на порог → `ShortBias` относительно Aster.
- В пределах порога, включая равенство → `Hold`.

Гипотеза: Aster догоняет Binance. Прибыльность не проверялась, исполнения нет. Решение сопровождается mid, возрастом и объёмом bid/ask top-5 обеих книг. Сравнение выполняется точно через целочисленные cross-products.

Робот работает в отдельном OS-потоке с busy polling и `spin_loop()`, без паузы 5–10 мс. Пересчитывает при обновлении книги, истечении свежести и сроке секундного отчёта. Это занимает логический CPU; измерений задержек нет. Короткий Offline между чтениями может быть пропущен: triple buffer хранит последнее состояние, а не очередь событий. Сам переход остаётся в логах адаптера.

## Числа, время и ограничения

`fixnum::FixedPoint<i64, U9>`, newtype `Price` и `Qty`. Входные числа разбираются точно, потеря точности логируется как ошибка, без округления. Для сумм и сравнения mid используется `i128`; границы `i64` и число уровней гарантируют вместимость промежуточных результатов.

`exchange_time` — биржевое `E`, `platform_time` — наше время получения до парсинга/очереди, оба Unix ms. Свежесть и skew считаются по монотонному `Instant`. Replay сохраняет исходные метки; смена статуса не обновляет их.

| Ограничение | Значение |
| --- | --- |
| Книга / snapshot / diff, на сторону | 8192 / 1000 / 4096 уровней |
| WS сообщение / snapshot HTTP body | 1 MiB |
| Metadata HTTP body | 8 MiB |
| Sync-очередь | 2048 сообщений и 8 MiB |
| Replay за итерацию | 32 сообщения |

Превышение лимита приводит к явной ошибке и восстановлению. На горячем пути нет mutex, блокирующего I/O и покадрового логирования; буферы книги предвыделены. WS/TLS и обработка ошибок могут аллоцировать.

## Логи и стек

`LEVEL:timestamp: событие параметры`, время UTC, stderr. По умолчанию `RUST_LOG=info`:

| Уровень | Содержание |
| --- | --- |
| INFO | Online (синий фон), Offline (красный), смена bias Aster с числами книг (зелёный) |
| WARN/ERROR | Сбой, восстановление или фатальная ошибка; красный фон |
| DEBUG | Подключение/shutdown, `feed state` с последним применённым `u`, секундный отчёт робота |

Фон только в терминале. `tracing-appender` пишет в отдельном потоке, очередь ограничена 4096 записями; при переполнении записи отбрасываются. Форматирование выполняется в вызывающем потоке. Начальные Offline/Hold не считаются сменой состояния.

Основные библиотеки: `tokio` (current-thread runtime на каждый фид и main), `tokio-tungstenite` + `rustls` (WS), `reqwest` (REST), `serde`/`serde_json` (типизированный разбор), `arrayvec` (ограниченные уровни), `triple_buffer` (публикация), `fixnum` (числа), `clap` (CLI), `tracing`/`tracing-subscriber`/`tracing-appender` (логи), `thiserror` (ошибки), `fastrand`/`httpdate` (backoff). `proptest` используется только для сверки merge с `BTreeMap`. Точные версии — `Cargo.lock`.

[Архитектура](docs/ARCHITECTURE.md) · [Проверки](docs/VALIDATION.md) · [План реализации](docs/IMPLEMENTATION_PLAN.md).
