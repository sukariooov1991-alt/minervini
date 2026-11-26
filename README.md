# MINERVINI Platform (minervini_v2 Structure)

## Structure
- `analysis/`: SEPA strategy modules (`trend_template.py`, `relative_strength.py`, `vcp.py`, `pullback.py`, `accumulation_distribution.py`, `earnings.py`, `fundamental_analysis.py`, `market_filter.py`).
- `data/`: Storage (`storage_manager.py`), multi-source daily market-data provider (`data_provider.py`), S&P 500 symbols (`sp500_symbols.py`), trade levels (`trade_levels.py`), Longbridge realtime client (`longbridge_stream.py`), and Telegram notifier (`telegram_notifier.py`).
- `screener.py`: Daily SEPA candidate scanner.
- `app.py` & `index.html`: Flask backend with `/api/stream` and dark mobile-first UI.
- `Dockerfile`: Container build configuration.

## Daily market-data provider failover

Daily OHLCV candles are fetched automatically in this order:

1. **Yahoo Finance** — primary source.
2. **Stooq** — best-effort public daily CSV fallback. The current endpoint may throttle or deny automated requests and some access paths may require registration/API credentials; this code does not send a Stooq key.
3. **Nasdaq public historical data** — additional automatic fallback with no API key or credentials; uses Nasdaq's public delayed historical endpoint.
4. **Alpha Vantage** — optional final fallback. Set `ALPHAVANTAGE_API_KEY` in the service environment to enable it.

Longbridge is reserved exclusively for realtime quotes through its WebSocket client. It subscribes to eligible watchlist symbols and retries missing subscriptions; card prices are updated from WebSocket pushes. The app retains the evolving session OHLC in memory between ticks so open/high/low are not reset on each push; individual ticks are not written into historical SQLite candles. No REST quote-snapshot fallback is currently used. Longbridge is never called by the daily or historical candle provider. US pre-market and post-market quotes are selected when their timestamps are newer than the regular-session quote. US overnight quotes are enabled with `LONGBRIDGE_ENABLE_OVERNIGHT=true` (already set in `render.yaml`); the same Longbridge LV1 quote permission covers pre-market, regular, after-hours, and overnight US stock quotes.

A failed or empty provider no longer aborts the symbol download: the next provider is attempted, and the successful source is written to the application log. If all providers fail, the error lists each provider's failure. The provider failover does not remove symbols from the S&P 500 universe or alter the screening criteria.

Nasdaq's public endpoint does not require a key, but it is an undocumented public endpoint and may change or throttle automated requests; its prices may be delayed. Stooq access is best-effort: automated downloads may be blocked, throttled, or require a key/CAPTCHA, so the application automatically continues to Nasdaq if Stooq fails. Alpha Vantage requires an API key and its free-tier limits may restrict large scans. Price-adjustment conventions can differ between providers, so a fallback source should be treated as an independent price history rather than assumed identical to Yahoo's adjusted series.


## Pullback entry path

Pullback is evaluated independently from VCP. The screener keeps a recent-decline candidate visible, then waits for the current candle to turn upward before confirming a Pullback entry. Because Minervini's published material does not define one universal numeric turn-up trigger or fixed stop, the current candle test is an explicit implementation proxy (close above its open and previous close), not a quoted Minervini formula. The pullback's recent low anchors the structural risk level. Saved cards are revalidated on startup so old Pullback triggers cannot remain active after the rule changes. Trade targets require a valid calculated ATR; the application does not substitute a made-up percentage when ATR is unavailable.
