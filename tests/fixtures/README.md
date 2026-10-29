# Provider contract samples

These fixtures verify field names, identifiers, date labels, and units without
requiring network access during CI. They do not certify historical prices.

- `efinance`: the 600519 row from the `efinance 0.5.9` `get_quote_history`
  docstring. Eastmoney CN volume is lots of 100 shares.
- `sina_equivalent`: a constructed equivalent of that row in Sina's documented
  format, using shares. It is not a recorded Sina response.
- `tickflow`: three anonymous `/v1/klines` responses captured on 2026-10-30,
  with `adjust=none`, `count=1`, and `end_time=1784332800000`. The `trade_date`
  column follows TickFlow's market-local DataFrame conversion. CN amount /
  volume is about 100 times the price; HK and US volume is already in shares.

References:

- https://github.com/Micro-sheep/efinance
- https://github.com/akfamily/akshare/blob/master/docs/data/stock/stock_quote.md
- https://free-api.tickflow.org/v1/klines
