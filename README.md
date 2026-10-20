# Binance + Aster: стаканы и сигнал на Rust

[Задание](rust-test.pdf): два независимых публичных market-data фида, локальные стаканы и сигнал без исполнения ордеров.

**Статус:** этапы 1–7 реализованы: ядро, robot, протоколы обеих бирж и сетевой Binance с восстановлением. 41 тест, включая локальные WS/HTTP mocks. Aster и общий запуск — этапы 8–9; live-подключения и задержки ещё не проверены.
[Архитектура](docs/ARCHITECTURE.md) · [План реализации](docs/IMPLEMENTATION_PLAN.md).

## Проверка текущей версии

Rust 1.99.1 / edition 2024 закреплён в `rust-toolchain.toml`, зависимости — в `Cargo.lock`.

```bash
cargo run --locked -- --help
cargo run --locked -- --check-config
cargo test --locked
```

CLI проверяет параметры сигнала (`--max-age-ms`, `--max-skew-ms`, `--min-depth-qty`, `--threshold-bps`). Запуск без `--check-config` пока завершается с явной ошибкой: сетевой сервис ещё не подключён.

## Основные решения

- Rust stable, Linux, один Cargo package и исполняемый сервис.
- ETHUSDT perpetual futures на Binance USDⓈ-M и Aster; книги из WS diff + REST snapshot.
- По OS-потоку с Tokio current-thread на гейт, отдельный OS-поток для busy polling в `robot`; основной поток — supervision.
- Предвыделенные `Vec<Level>`; все известные уровни публикуются через `triple_buffer`.
- Десятичный fixed-point `fixnum`, типы `Price`, `Qty`, `Amount`.
- Для робота книга имеет только статусы `Online` / `Offline`. Диагностика принадлежит гейтам и `metrics`.

## Рынки и источники

| Назначение | Binance USDⓈ-M | Aster Futures |
| --- | --- | --- |
| Символ / поток | `ETHUSDT` / `ethusdt@depth@100ms` | `ETHUSDT` / `ethusdt@depth@100ms` |
| WS combined | `wss://fstream.binance.com/public/stream?streams=ethusdt@depth@100ms` | `wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms` |
| Metadata | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `https://fapi.asterdex.com/fapi/v3/exchangeInfo` |
| Snapshot | `https://fapi.binance.com/fapi/v1/depth?symbol=ETHUSDT&limit=1000` | `https://fapi.asterdex.com/fapi/v3/depth?symbol=ETHUSDT&limit=1000` |

Документация: [Binance WS `/public`](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams), [синхронизация Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly), [Aster Futures V3](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md#websocket-market-streams).

При старте проверяем символ, торговый статус, тип контракта, единицы, `PRICE_FILTER.tickSize` и `LOT_SIZE.stepSize`. `pricePrecision`/`quantityPrecision` не заменяют шаги. Несовместимые metadata останавливают инициализацию; контрактный множитель требует нормализации объёма в ETH.

### Синхронизация diff-stream и snapshot

1. Открыть WS, буферизовать diff с исходными `exchange_time`, `platform_time` и `received_at`, параллельно запросить snapshot.
2. Для `L = lastUpdateId` отбросить события с `u < L`.
3. Первое применяемое событие должно иметь `U <= L <= u`. При отсутствии ждать до deadline; при `U > L` повторить синхронизацию.
4. Далее проверять `pu == previous_u`. Числовые пропуски внутри диапазона ID сами по себе не означают gap.
5. Количество абсолютное: положительное вставляет/заменяет уровень, ноль удаляет. Неупомянутые цены сохраняются; удаление неизвестной цены допустимо.
6. После replay и проверки книги опубликовать `Online`.

Точный повтор последнего нормализованного события игнорируем без обновления свежести. Конфликтующий повтор, регресс и некорректный диапазон ID требуют resync. Каждая попытка получает `sync_epoch`; поздние HTTP-ответы прежних попыток отбрасываются. При resync публикуем `Offline`, очищаем очередь и начинаем новую синхронизацию; при разрыве WS сначала переподключаемся.

### Глубина и границы достоверности

Snapshot ограничен 1000 уровнями на сторону: дальняя глубина может быть неизвестна. Храним и публикуем все известные уровни без обрезки.

Границы исходного snapshot — `bid_floor` и `ask_ceiling`. При непрерывных diff достоверны bids `price >= bid_floor` и asks `price <= ask_ceiling`. Если после целого события внутри границ осталось меньше пяти уровней любой стороны — `Offline` и resync. Проверка применяется и к короткому snapshot. Top-5 ограничивает только расчёт сигнала.

## Библиотеки

Сборка проверяется закреплённым toolchain с `--locked`. Tokio/HTTP/WS уже включены; `fastrand`, `hdrhistogram` и `criterion` подключаются на этапах recovery и измерений.

| Слой | Библиотека | Настройка |
| --- | --- | --- |
| Async I/O | [`tokio` 1.x](https://docs.rs/tokio/latest/tokio/) | `rt`, `net`, `time`, `macros`, `signal`, `sync`; current-thread runtime |
| WS | [`tokio-tungstenite` 0.30.x](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/) | Без default features; `connect`, `rustls-tls-webpki-roots` |
| Stream/Sink | [`futures-util` 0.3.x](https://docs.rs/futures-util/latest/futures_util/) | Feature `sink` |
| HTTPS | [`reqwest` 0.13.x](https://docs.rs/reqwest/latest/reqwest/) | Без default features; `rustls`, `json`; переиспользуемый `Client` |
| TLS | `rustls` через клиентов | Общий AWS-LC provider; HTTPS — системный verifier, WSS — WebPKI roots |
| JSON | [`serde` 1.x](https://serde.rs/) + [`serde_json` 1.x](https://docs.rs/serde_json/latest/serde_json/) | `derive`, `raw_value`, типизированные модели и числовые visitors |
| Книга / sync-очередь | `Vec` / `VecDeque` | Предвыделение и явные лимиты |
| Diff | [`arrayvec` 0.7.x](https://docs.rs/arrayvec/latest/arrayvec/) | `serde`; ограниченные массивы изменений |
| Публикация | [`triple_buffer` 9.x](https://docs.rs/triple_buffer/latest/triple_buffer/) | По SPSC-каналу последнего состояния на биржу |
| Fixed-point | [`fixnum` 0.9.5](https://docs.rs/fixnum/latest/fixnum/) | Без default features; `std`, `i64`; собственный visitor |
| Ошибки | [`thiserror` 2.x](https://docs.rs/thiserror/latest/thiserror/) | Типизированные enum и числовые поля |
| CLI | [`clap` 4.x](https://docs.rs/clap/latest/clap/) | `derive`, `env` |
| Jitter | [`fastrand` 2.x](https://docs.rs/fastrand/latest/fastrand/) | Отдельный RNG на гейт |
| Трассировка | [`tracing` 0.1.x](https://docs.rs/tracing/latest/tracing/) | События жизненного цикла, span на соединение |
| Формат логов | [`tracing-subscriber` 0.3.x](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/) | `env-filter`, `fmt`, `json` |
| Writer | [`tracing-appender` 0.2.x](https://docs.rs/tracing-appender/latest/tracing_appender/) | Ограниченная lossy-очередь, `WorkerGuard` до shutdown |
| Задержки | [`hdrhistogram` 7.x](https://docs.rs/hdrhistogram/latest/hdrhistogram/) | Фиксированный диапазон без auto-resize |
| Эталон чисел | [`rust_decimal` 1.x](https://docs.rs/rust_decimal/latest/rust_decimal/) | Dev-only; без default features, `std` |
| Property tests | [`proptest` 1.x](https://docs.rs/proptest/latest/proptest/) | Dev-only |
| Микробенчмарки | [`criterion` 0.8.x](https://docs.rs/criterion/latest/criterion/) | Dev-only |

## Владение данными и конкурентность

Каждый гейт владеет соединением, рабочей книгой, scratch, очередью синхронизации и входом triple buffer. `BookView` содержит обе стороны, покрытие, `sequence`, `exchange_time`, `platform_time`, `received_at` и `BookStatus`. Биржевые ID, epoch и ошибки остаются в гейте.

Diff полностью разбирается, сортируется и сливается с книгой в scratch за `O(N + M)`. Обе стороны проверяются до commit; между изменением книги и публикацией нет `.await`. Затем все уровни копируются в предвыделенный слот и публикуются за `O(N)`. Все три слота переиспользуются без роста capacity и покадрового `clone()`.

`robot` владеет выходами публикации и читает стабильные версии. Общего атомарного снимка двух бирж нет. Промежуточные публикации можно пропускать, входные diff — нельзя. Короткий переход через `Offline` тоже может быть пропущен; факт ошибки сохраняется в метриках.

`robot` запускается через `std::thread::spawn`, без Tokio runtime и `spawn_blocking`. В busy loop отдельно вызывает `update()` обоих выходов triple buffer; если хотя бы один обновился, пересчитывает решение. Вызовы не объединяются через короткозамыкающий `||`. При отсутствии работы — `std::hint::spin_loop()`. Адаптеры только публикуют слоты, дополнительных уведомлений нет.

Цикл постоянно занимает логический CPU; выделение физического ядра и влияние на адаптеры проверяются измерениями. Он проверяет stop-флаг `Arc<AtomicBool>` с `Relaxed`: флаг означает только остановку и не публикует другие данные. Синхронизацией книг управляет triple buffer.

`md::spawn_adapter` запускает `MDAdapter::run` через `block_on` в отдельном OS-потоке с Tokio current-thread. Реализован `binance::BinanceAdapter::default()`. `select!` обслуживает WS, HTTP, таймеры и shutdown; HTTP future сохраняется между итерациями. Разбор snapshot — не более одной незавершённой `spawn_blocking`-работы на гейт. Replay выполняется порциями по 32 сообщения с возвратом к обслуживанию WS. Очередь хранит исходный текст и время получения; старый результат разбора snapshot отбрасывается по `sync_epoch`. Отдельные tasks на каждый diff не создаются.

### Временные метки

| Поле | Значение |
| --- | --- |
| `exchange_time: Option<u64>` | Биржевое время события `E`, Unix time в миллисекундах; не время транзакции `T` |
| `platform_time: Option<u64>` | Наше UTC-время получения полного WS-сообщения до парсинга, Unix time в миллисекундах |
| `received_at: Option<Instant>` | Монотонная отметка того же получения для возраста, skew и измерения задержек |

В `BookView` метки относятся к последнему применённому diff; до него — `None`. Через очередь/replay переносим исходные значения. Повторы, Ping/Pong, публикация и смена статуса их не меняют. Порядок обновлений определяется ID, не timestamp.

`platform_time - exchange_time` — диагностический lag с погрешностью рассинхронизации часов; отрицательную разницу не обнуляем. Свежесть и skew робот считает по `received_at`, чтобы скачки системных часов не влияли на решение. Оба Unix timestamp сохраняются в обосновании решения.

## Fixed-point и округление

### Выбор представления

```rust
use fixnum::{FixedPoint, typenum::U9};

type Fp = FixedPoint<i64, U9>;

#[repr(transparent)]
struct Price(Fp);  // USDT/ETH
#[repr(transparent)]
struct Qty(Fp);    // ETH
#[repr(transparent)]
struct Amount(Fp); // USDT
```

Поля закрыты. Проверяем `Price > 0`, `Qty/Amount >= 0`; сохранённый уровень имеет `Qty > 0`. Неявных преобразований между величинами и `Deref` нет.

Число равно `raw / 10^9`: `3500.10` → `3_500_100_000_000`. Максимум — `9_223_372_036.854775807`. Шаг инструмента проверяется отдельно: при `tickSize = 0.05` raw-цена кратна `50_000_000`. Непредставимые metadata отклоняются.

### Политика арифметики

**Не округляем входные значения и результаты операций.** Если точное представление невозможно, возвращаем `PrecisionLoss` и явно пишем ошибку через `tracing::error!` с контекстом операции. Ошибка входных данных переводит книгу в `Offline` и запускает resync; ошибка расчёта даёт `Hold`. Запись выполняет отдельный writer.

- Вход: ограниченная ASCII-строка `digits[.digits]`, без знаков, пробелов и экспоненты. Хвостовые нули дробной части убираются срезом перед `from_str_exact`; ненулевая точность сверх девяти разрядов отклоняется. Через `f64` не преобразуем.
- Сложение/вычитание — `cadd/csub`, умножение на целое — `cmul`. Перед `rmul/rdiv` проверяем точную представимость результата через checked `i128` и остаток деления с учётом масштаба. Ненулевой остаток — `PrecisionLoss`; округлённый результат не используется.
- Доменные операции: `Qty + Qty → Qty`, `Amount ± Amount → Amount`, `Price × Qty → Amount`. Ошибка диапазона или отрицательный результат не маскируются wrapping/saturating арифметикой.
- Агрегаты и сравнение порогов — raw-значения, расширенные до checked `i128`; масштаб суммы `10^9`, произведения `10^18`. Ошибка расчёта даёт `Hold`.
- Отклонение bps выводим точной дробью `N/D`, без округления. `rust_decimal` служит эталоном только в тестах.

[Представление и операции fixnum](https://docs.rs/fixnum/latest/fixnum/) · [точный парсер](https://docs.rs/fixnum/latest/src/fixnum/string.rs.html).

## JSON, память и горячий путь

Горячий путь: WS frame → parse → sequence check → merge → publish. Без REST, файлового I/O, общей блокировки книги и покадрового логирования.

Отдельные модели Binance/Aster проверяют wrapper, символ, событие, `U/u/pu` и числа. Binance также проверяет `st`/`ps`, если они присутствуют. Массивы уровней заполняются на месте через serde seed; metadata ограничены 4096 инструментами и 32 фильтрами на выбранный инструмент. Используем visitors без `serde_json::Value`. Bids сортируются по убыванию, asks — по возрастанию; `sort_unstable` не требует heap-буфера. Дубликаты цены в payload отклоняются. После целого diff проверяем порядок, вместимость и `best_bid < best_ask`.

| Ресурс | Лимит |
| --- | --- |
| REST snapshot | 1000 уровней на сторону |
| Книга, scratch, каждый из трёх слотов публикации | 8192 уровня на сторону |
| Один diff | 4096 изменений на сторону |
| WS frame / собранное message | По 1 MiB |
| Sync-очередь на гейт | 2048 сообщений и 8 MiB payload |
| HTTP snapshot / metadata body | 1 MiB / 8 MiB |
| Очередь логов | 4096 записей |

Превышение входного лимита переводит книгу в `Offline` и запускает восстановление; oversized metadata — ошибка инициализации. HTTP body ограничивается при чтении chunks. Уровни не отбрасываются ради места; `with_capacity` дополняется явной проверкой лимита перед записью.

Прикладные буферы переиспользуются. Аллокации WS/TLS, парсинга и ошибок измеряются отдельно: общей гарантии zero-allocation нет. Альтернативные allocator, `simd-json` и CPU affinity рассматриваются после профиля.

## Восстановление и свежесть

Внутренний автомат: `Connecting → Buffering → SnapshotLoading → Replaying → Live`; ошибки ведут через backoff к resync или reconnect. Роботу публикуется только:

- `Offline` — старт, gap, плохой кадр, overflow, потеря покрытия, разрыв, восстановление, остановка.
- `Online` — успешные snapshot/replay и проверка книги.

`Offline` публикуется до ожидания восстановления. Следующий корректный diff сам по себе книгу не восстанавливает. Новый WS увеличивает `connection_epoch`; новый snapshot — `sync_epoch`.

| Параметр | Значение |
| --- | --- |
| Connect/handshake / HTTP timeout | По 10 с |
| Deadline синхронизации / закрытия WS | 15 с после backoff / 1 с |
| Запуск расчёта robot | Busy polling обоих буферов, расчёт при обновлении книги или статуса |
| Контроль свежести | Проверка ближайшего deadline в polling-цикле, даже без обновлений |
| Периодический вывод решения | 1 с по `Instant`, без догоняющих запусков |
| Максимальный возраст / skew книг | 500 мс / 250 мс |
| Watchdog новых валидных данных | 3 с |
| Backoff | База 250 мс, удвоение до 30 с, equal jitter `[base/2, base]` |
| Сброс backoff | После 30 с стабильных данных |

Возраст считается от `received_at` последнего применённого WS-события. `exchange_time` и `platform_time` описаны в [разделе временных меток](#временные-метки). Локальный возраст не измеряет задержку до получения сообщения; пороги доступны через CLI.

Ping/Pong/flush обслуживаются постоянно, HTTP 429 учитывает `Retry-After`. При shutdown supervision устанавливает stop-флаг робота и сигнал гейтам: `Offline`, отмена ожиданий/backoff, закрытие соединений с deadline, присоединение всех workers, завершение writer. Panic/неожиданный выход любого worker завершает сервис с ошибкой. Уже начатый `spawn_blocking` не останавливается отменой ожидания; его завершение учитывается отдельно.

## Сигнал первой версии

`robot::evaluate(&BookView, &BookView, &RobotConfig, now) -> Decision` читает только опубликованные `book::BookView`. Направление задаётся относительно Aster, Binance — ориентир.

Условия расчёта: обе книги `Online`, свежие, с допустимым skew и пятью достоверными уровнями каждой стороны. Каждая из четырёх сумм top-5 bids/asks должна быть ≥ `min_depth_qty`, исходно 1 ETH. Иначе — `Hold`.

```text
mid2_A = bid_A + ask_A
mid2_B = bid_B + ask_B
N = 10_000 * (mid2_A - mid2_B)
D = mid2_B                              # D > 0
N < -theta * D → LongBias               # Aster дешевле
N >  theta * D → ShortBias              # Aster дороже
иначе          → Hold
```

Порог `theta = 5` bps. Сравнения — checked `i128` без деления; `N / D` используется для отображения. Это исследовательский сигнал без оценки комиссий, funding и исполнения.

`Decision`: направление, статусы/sequence, bid/ask, mid, объёмы, `exchange_time`, `platform_time` и возраст обеих книг, skew, отклонение и порог. Без отдельных классификаций причины и площадки сбоя: робот получает только `Online/Offline` и числа книг. Отсутствующие данные — `None`; сохранённые данные `Offline` не участвуют в расчёте. Если проверки не пройдены, результат — `Hold`, отклонение — `None`.

Первый расчёт выполняется при запуске. Далее polling-цикл проверяет оба буфера, stop-флаг и deadline по `Instant`. `evaluate` вызывается при новой публикации, превышении max-age или наступлении срока секундного вывода. Публикация во время расчёта подхватывается на следующем проходе. Обработанный deadline свежести исключается до нового состояния книги: устаревание даёт `Hold`, но не запускает расчёт на каждой пустой итерации.

Вывод — при изменении направления или статуса любой книги и раз в секунду, включая `Hold`. Искусственной паузы между проверками нет; задержка включает применение diff, копирование книги, доступ к кешу, планирование OS и расчёт. Выигрыш busy polling подтверждается replay.

## Трассировка и статистика

Span на соединение: `venue`, `symbol`, `connection_epoch`. Логируем подключения, восстановление, смену статуса, решения и агрегаты ошибок; не каждый тик. `tracing-appender`: `lossy(true)`, `buffered_lines_limit(4096)`; потери — через `ErrorCounter`. Подготовка записи выполняется вызывающим потоком, запись — отдельным writer.

`FeedMetrics` передаётся supervision через отдельный предвыделенный triple buffer, выводится раз в секунду. Содержит состояние/epoch/последнюю ошибку, received frames, applied diffs, snapshots, stale snapshots, gaps, resyncs, overflows, decode errors, coverage lost, duplicates, reconnects и возраст данных. Счётчики сохраняются между reconnect/resync. Отдельно считаем пропуски публикаций и потери логов.

## Структура и проверка реализации

| Модуль | Ответственность |
| --- | --- |
| `main`, `config` | CLI, запуск, supervision, shutdown, проверенные параметры |
| `numeric` | `Fp`, newtype, парсинг и арифметика |
| `book` | Diff-книга, merge, покрытие, `BookView`, статусы и публикация |
| `md` | Трейт `MDAdapter`, внутренние состояния и ошибки |
| `binance`, `aster` | Модули первого уровня: WS/REST, синхронизация, reconnect |
| `robot` | Чтение опубликованной книги, свежесть и решение |
| `metrics` | Счётчики и tracing |

`MDAdapter` получает данные, публикует книгу/статус, восстанавливается и останавливается. `lib.rs` экспортирует модули для `tests/` и `benches/`; `main.rs` запускает сервис.

Проверяем числа и overflow, merge против `BTreeMap`, отсутствие частичного commit, snapshot bridge/gaps/повторы, поздние ответы, лимиты, независимость фидов, медленного читателя, статусы и shutdown. Для сигнала — пороги, freshness/skew, объём и условия `Hold`. Сценарии перечислены в [плане](docs/IMPLEMENTATION_PLAN.md).

```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --release --locked
cargo bench --locked
```

Release replay измеряет `frame_received → book_published`, `book_published → robot_decision` и полный путь, p50/p99/p99.9, throughput, аллокации и загрузку CPU робота/адаптеров. Нагрузка подаётся независимо от скорости читателя; отдельно учитываются пропуски публикаций, steady state и resync. В отчёте — CPU/OS, affinity, compiler, lockfile, параметры, длительность и размер выборки. Busy polling сравнивается с вариантом на `Notify` в benchmark при одинаковом workload; измерений пока нет.
