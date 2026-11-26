# MINERVINI Platform (minervini_v2 Structure)

## Structure
- `analysis/`: strategy modules (`trend_template.py`, `relative_strength.py`, `vcp.py`, `pullback.py`, `accumulation_distribution.py`, `earnings.py`, `market_filter.py`). `fundamental_analysis.py` is a legacy annual-statement compatibility helper; the active screener uses quarterly `earnings.py`.
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

Longbridge is reserved exclusively for realtime quotes through its WebSocket client. It subscribes to eligible watchlist symbols, retries missing subscriptions, and reconnects a stream after 5 minutes without any push during regular hours or 15 minutes during extended sessions. Volume-only PushQuote messages count as connection activity but are not treated as price ticks. When sequence numbers are absent, incoming and queued quotes are ordered by their timestamps. The app retains the evolving session OHLC in memory between ticks so open/high/low are not reset on each push; individual ticks are not written into historical SQLite candles. No REST quote-snapshot fallback is currently used. Longbridge is never called by the daily or historical candle provider. US pre-market and post-market quotes are selected when their timestamps are newer than the regular-session quote. US overnight quotes are enabled with `LONGBRIDGE_ENABLE_OVERNIGHT=true` (already set in `render.yaml`); the same Longbridge LV1 quote permission covers pre-market, regular, after-hours, and overnight US stock quotes.

A failed or empty provider no longer aborts the symbol download: the next provider is attempted, and the successful source is written to the application log. If all providers fail, the error lists each provider's failure. The provider failover does not remove symbols from the S&P 500 universe or alter the screening criteria.

Nasdaq's public endpoint does not require a key, but it is an undocumented public endpoint and may change or throttle automated requests; its prices may be delayed. Stooq access is best-effort: automated downloads may be blocked, throttled, or require a key/CAPTCHA, so the application automatically continues to Nasdaq if Stooq fails. Alpha Vantage requires an API key and its free-tier limits may restrict large scans. Price-adjustment conventions can differ between providers, so a fallback source should be treated as an independent price history rather than assumed identical to Yahoo's adjusted series.


## Permanent historical and event-data cache

The SQLite database on Render's persistent disk is the source of truth for daily history and cached research data. The cache is independent of watchlist cards; deleting a card never deletes its candles, fundamentals, news, or earnings events.

- **Daily OHLCV:** retain the latest 300 completed daily candles per symbol (including the market index); symbols with fewer than 300 sessions since IPO retain all available completed candles. A recurring US equity holiday/early-close calendar controls the session label and the one-hour post-close completion buffer. The first load fetches enough history to fill the cache. Later scans read from SQLite and do not re-download the two-year history. When the cache is stale, the provider fetches a short recent window, upserts by (symbol, date), and trims the oldest rows to 300. The source is recorded per symbol; if the vendor changes or a legacy cache has no source tag, the system rebases the full history from the new vendor before writing, avoiding a mixture of differently adjusted price series. The current, unfinished US session candle is excluded.
- **Refresh cadence:** a successful daily-history refresh is reused for 20 hours, with one additional check after the actual regular US close (including early-close days) if the latest completed candle is still from an earlier session. The next eligible scan refreshes the recent 10-day window, allowing missed sessions to be filled without replacing the stored history. Failed refreshes preserve the last good candles and are retried after a 15-minute cooldown; failed attempts do not reset the successful-refresh clock.
- **Quarterly fundamentals / earnings:** cache valid quarterly EPS and revenue data for 7 days. Empty/unavailable responses are cached for 6 hours to avoid repeating failed requests on every scan. When refreshed, reported-quarter records are also persisted in the events table and deduplicated.
- **News:** refresh non-empty news for stocks that qualify for the watchlist every 6 hours; empty/error responses are retried after 1 hour. Both the cached response and individual articles are stored in SQLite. Duplicate articles are ignored, and the event-history table no longer deletes older news or earnings records after a fixed count.
- **Retention:** news and earnings event history has no rolling row cap and is separate from watchlist_cards. The database can grow over time and should be monitored.

A scan therefore uses local 300-bar history and cached quarterly fundamentals; external providers are called only when the relevant cache needs a refresh or is missing. These time-to-live checks run when a scan is executed; there is currently no independent cron/background scheduler, so the user must run a scan for stale data to refresh.

## Pullback entry path

Pullback is evaluated independently from VCP. The screener keeps a recent-decline candidate visible, then waits for the current candle to turn upward before confirming a Pullback entry. Because Minervini's published material does not define one universal numeric turn-up trigger or fixed stop, the current candle test is an explicit implementation proxy (close above its open and previous close), not a quoted Minervini formula. The first confirmed Pullback trigger and its support low are frozen in the saved card across subsequent price ticks; they are cleared when the turn-up confirmation is lost and reset if a new lower structural low forms. The pullback's recent low anchors the structural risk level. Saved cards are revalidated on startup so old Pullback triggers cannot remain active after the rule changes. Trade targets are calculated as a 2R-to-3R ladder from the actual shared stop; Wilder ATR is used only when a structural low is unavailable for stop placement. The plan is rejected if any displayed entry tranche would require more than a 10% stop.


## Strategy source alignment and implementation boundaries

The application distinguishes rules explicitly described by Mark Minervini from numeric proxies required to automate chart interpretation. A numeric threshold below that is not stated in the cited material is a project setting, not a claim about a universal Minervini rule.

### Source-defined principles

- **Trend Template:** all eight Stage 2 criteria are applied, including price above the 50/150/200-day averages in the specified order, a rising 200-day average, at least 30% above the 52-week low, within 25% of the 52-week high, and RS rank at least 70. Source: *Trade Like a Stock Market Wizard*, Chapter 5.
- **VCP:** volatility contracts from left to right, usually across 2–6 contractions; successive contractions are roughly half the previous one, with reasonable variation; volume dries up at key points; the entry is the pivot break with expanding volume. Source: *Trade Like a Stock Market Wizard*, Chapter 10.
- **Pullback:** Minervini waits for price to begin turning upward and does not buy while it is falling. Pullbacks can occur within a base, at a prior breakout level, or occasionally at the 20/50-day averages after a breakout. The source does not prescribe one universal numeric turn-up formula. Source: *Momentum Masters* (2015), Minervini interview, and *Trade Like a Stock Market Wizard*, Chapter 11.
- **Risk:** establish a predetermined stop before entry; keep the maximum loss no greater than 10%; maintain at least a 2:1 win/loss ratio and aim for 3:1. Stop placement and profit-taking are tied to actual trading results and market conditions, not a universal ATR target formula. Source: *Trade Like a Stock Market Wizard*, Chapters 12–13.

### Explicit project heuristics (not presented as Minervini's exact rules)

- RS rating is a percentile rank calculated across this repository's current 493-symbol universe from a 40/20/20/20 weighted 63/126/189/252-session return score (253 closes are required for the full 252-session leg); it is not the proprietary IBD RS Rating or a published Minervini-specific RS calculation.
- VCP swing-pivot detection, the 0.75 maximum contraction ratio, 10% final-contraction cap, and 75%/55% volume-dry-up thresholds are mechanical screening proxies for the qualitative VCP footprint.
- Breakout confirmation's 1.35x volume, 0.65 close-location, and 0.25% buffer are project thresholds for "expanding volume" and a confirmed pivot break.
- Earnings growth minimums (20% EPS and 10% revenue), accumulation/distribution windows and ratios, market breadth minimum (25%), and five-distribution-day limit are project filters; they are not stated as universal fixed thresholds in the cited Minervini source material.
- Pullback's five-bar decline window and live-price turn-up candle test are operational proxies. When the official session open is not available from PushQuote, the live candle uses the previous completed close as its reference instead of treating an arbitrary first tick as the open.
- The 5% maximum entry range, five entry tranches, intermediate 2R-to-3R target steps, and the 0.5% structural-low buffer are application trade-planning outputs. They are not represented as Minervini's prescribed five-target or fixed stop-placement method. Wilder ATR is used as a fallback when no structural low is available.

These distinctions are deliberate: preserve the source-defined concepts, keep necessary automation parameters visible, and avoid attributing implementation choices to the author. The built-in calendar covers recurring US equity holidays and common early closes; exceptional one-off exchange closures still require a calendar update.
