# MINERVINI Platform (minervini_v2 Structure)

## Structure
- `analysis/`: SEPA strategy modules (`trend_template.py`, `relative_strength.py`, `vcp.py`, `pullback.py`, `accumulation_distribution.py`, `earnings.py`, `fundamental_analysis.py`, `market_filter.py`).
- `data/`: Storage (`storage_manager.py`), multi-source daily market-data provider (`data_provider.py`), S&P 500 symbols (`sp500_symbols.py`), trade levels (`trade_levels.py`), Longbridge realtime client (`longbridge_stream.py`), and Telegram notifier (`telegram_notifier.py`).
- `screener.py`: Daily SEPA candidate scanner.
- `app.py` & `index.html`: Flask backend with `/api/stream` and dark mobile-first UI.
- `Dockerfile`, `.dockerignore`, `.gitignore`: Container build, ignore rules, and Git exclusions.

## Daily market-data provider failover

Daily OHLCV candles are fetched automatically in this order:

1. **Yahoo Finance** — primary source.
2. **Stooq** — best-effort public daily CSV fallback. The current endpoint may throttle or deny automated requests and some access paths may require registration/API credentials; this code does not send a Stooq key.
3. **Nasdaq public historical data** — additional automatic fallback with no API key or credentials; uses Nasdaq's public delayed historical endpoint.
4. **Alpha Vantage** — optional final fallback. Set `ALPHAVANTAGE_API_KEY` in the service environment to enable it.

Longbridge remains connected through its existing separate price/realtime integration and is not changed by this daily-history fallback order.

A failed or empty provider no longer aborts the symbol download: the next provider is attempted, and the successful source is written to the application log. If all providers fail, the error lists each provider's failure. No symbols are removed from the S&P 500 universe and the Minervini screening rules are unchanged.

Nasdaq's public endpoint does not require a key, but it is an undocumented public endpoint and may change or throttle automated requests; its prices may be delayed. Stooq access is best-effort: automated downloads may be blocked, throttled, or require a key/CAPTCHA, so the application automatically continues to Nasdaq if Stooq fails. Alpha Vantage requires an API key and its free-tier limits may restrict large scans. Price-adjustment conventions can differ between providers, so a fallback source should be treated as an independent price history rather than assumed identical to Yahoo's adjusted series.
