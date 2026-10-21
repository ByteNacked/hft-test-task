# Fixtures

Синтетические ETHUSDT сообщения по официальным схемам, проверенным 2026-10-21; это не запись live-фида. Цены, объёмы, ID, timestamps и шаги выбраны для воспроизводимых тестов, не описывают текущие параметры бирж.

- Binance: [diff и combined wrapper](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams), [REST metadata/snapshot](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
- Aster: [Futures V3 — Exchange Information, Order Book, Diff Book Depth Streams](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md).

В каждой папке: `metadata.json`, `snapshot.json` (L=100), `diff.json` (U=100, u=102, pu=99), повреждённый `invalid_precision.json`. Дополнительные повреждения и превышения лимитов создаются тестами. `E != T` намеренно; локальное время задаётся тестом отдельно. Binance содержит новые необязательные `st=1` / `ps`; иной рынок отклоняется.

Правила Aster отдельно сверены для этапа 8: [local order book](https://github.com/asterdex/api-docs/blob/master/V3%28Recommended%29/EN/aster-finance-futures-api-v3.md#how-to-manage-a-local-order-book-correctly) требует отбросить `u < L`, связать snapshot первым `U <= L <= u`, далее проверять `pu == previous_u`. Общий recovery-набор запускается для каждой биржи с её адаптером, metadata и snapshot.
