# MINERVINI Platform (minervini_v2 Structure)

## Structure
- `analysis/`: SEPA strategy modules (`trend_template.py`, `relative_strength.py`, `vcp.py`, `pullback.py`, `accumulation_distribution.py`, `earnings.py`, `fundamental_analysis.py`, `market_filter.py`).
- `data/`: Storage (`storage_manager.py`), Yahoo Finance provider (`data_provider.py`), S&P 500 symbols (`sp500_symbols.py`), trade levels (`trade_levels.py`), Longbridge realtime client (`longbridge_stream.py`), and Telegram notifier (`telegram_notifier.py`).
- `screener.py`: Daily SEPA candidate scanner.
- `app.py` & `index.html`: Flask backend with `/api/stream` and dark mobile-first UI.
- `Dockerfile`, `.dockerignore`, `.gitignore`: Container build, ignore rules, and Git exclusions.

## Daily market-data provider failover

Daily OHLCV candles are fetched automatically in this order:

1. **Yahoo Finance** — primary source.
2. **Nasdaq public historical data** — automatic fallback with no API key or additional credentials; uses Nasdaq's public delayed historical endpoint.
3. **Longbridge OpenAPI** — existing historical daily-candle integration is retained unchanged; uses the existing `LONGBRIDGE_APP_KEY`, `LONGBRIDGE_APP_SECRET`, and `LONGBRIDGE_ACCESS_TOKEN` settings.
4. **Stooq** — additional public CSV fallback; availability and automated access are not guaranteed.
5. **Alpha Vantage** — optional final fallback. Set `ALPHAVANTAGE_API_KEY` in the service environment to enable it.

A failed or empty provider no longer aborts the symbol download: the next provider is attempted, and the successful source is written to the application log. If all providers fail, the error lists each provider's failure. No symbols are removed from the S&P 500 universe and the Minervini screening rules are unchanged.

Nasdaq's public endpoint does not require a key, but it is an undocumented public endpoint and may change or throttle automated requests; its prices may be delayed. Longbridge's historical-candlestick endpoint has account-based monthly unique-symbol quotas. Stooq is an additional public source with no availability guarantee. Alpha Vantage requires an API key and its free-tier limits may restrict large scans.
