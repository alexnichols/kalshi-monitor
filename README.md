# Kalshi volume monitor

Kalshi volume by category, fees priced fill by fill from Kalshi's fee schedule, and share
against Polymarket and Hyperliquid. A GitHub Actions job rebuilds the data from Kalshi's
public trade tape every night and GitHub Pages serves the dashboard.

## Setup: one command

Needs `git` and the GitHub CLI (`gh`), logged in with `gh auth login`.

```
./setup.sh                    # creates public repo <you>/kalshi-monitor
./setup.sh my-repo-name       # or pick a name
```

It creates the repo, pushes this folder, lets workflows write to the repo, turns on Pages
from `main` `/docs`, and starts the backfill from 2025-01-01 (`BACKFILL_START=...` to change).
Each run fetches for about 2 hours, newest days first, commits progress and restarts itself until every day is
done (it stops chaining if a run makes no progress); after that it runs daily at 09:20 UTC, once Kalshi has published the previous day's report. L7D and L28D appear within a few hours; older months fill in over the following days. The page loads `docs/data.json` automatically and
shows its built-in snapshot until the first run writes it.

## Everyday CLI

```
gh run watch -R <you>/kalshi-monitor                          # follow the current run
gh run list  -R <you>/kalshi-monitor --workflow refresh.yml   # history
gh workflow run refresh.yml -R <you>/kalshi-monitor           # refresh now
gh workflow run refresh.yml -R <you>/kalshi-monitor -f start=2026-01-01   # backfill a range
```

## Tie-out against your own figures (local only)

The site is public, so nothing confidential belongs in this repo; `.gitignore` blocks files
with "deck" in the name and `setup.sh` refuses to publish if one is in the folder. To check
the pipeline against your figures, keep `deck_tieout.json` outside the repo and run:

```
git pull
python3 kalshi_pipeline.py --skip-trades --skip-perps --cache data/cache \
  --out /tmp/kalshi_check.json --deck ~/deck_tieout.json
```

That prints, month by month, computed volume against the volume your figures imply and
computed trading fees against your trading fees, without fetching anything.

## Throughput

A busy day's trade report is ~20M rows (~290 MB gzipped) and takes about 3 minutes to download
and price; 2025 days take seconds. Runs process three days at a time, so the whole history since
January 2025 takes one or two runs. Perps come from Kalshi's API, limited to about 4 requests a
second per IP (measured; more returns 429), so runs use `--rps 4`; a perps day is ~500 pages.
Tests: `python -m unittest discover -s tests`.

## Cost

Public repo: free. Actions minutes are unlimited for public repos and Pages is free.
(A private repo would need GitHub Pro for Pages, and the site would still be public.)

## Method

Event contracts
- Every fill from Kalshi's daily trade report, the file behind kalshi.com/market-data
  (`kalshi-public-docs.s3.amazonaws.com/reporting/trade_data_YYYY-MM-DD.json[.gz]`): one row per
  fill with ticker, series, contracts and Yes price, by Kalshi's reporting day in Eastern time.
  Checked against the API trade tape (`GET /historical/trades`) for 2025-01-15: the same 39,040 fills,
  row for row. Its contracts sum to the day's volume in Kalshi's market report exactly. One
  download per day replaces tens of thousands of rate-limited API pages.
- Categorized with Kalshi's series metadata; series missing from `GET /series` are looked up
  through the market and event endpoints. Day files keep per-series totals.
- Fees per Kalshi's fee schedule: taker `round up(M × 0.07 × C × P × (1−P))`, maker at 0.0175
  (combo maker multiplier 2), using each series' and event's fee terms in force on the trade date:
  Kalshi's change log (`GET /series/fee_changes`, from Oct 4, 2025), current series terms for
  series the log never mentions, and before the log the published schedules in force (archived
  copies from Jan 2025 on): S&P 500 / Nasdaq-100 at 0.035 until Jul 3, 2026, flat maker fees of
  `round up(0.0025 × C)` May 13 to Jul 7, 2025, then the dated maker-fee series lists.
- Rounding as each schedule defines it: the fee rounded up to the cent before Jul 7, 2026; from
  then, fee + positionCost rounded up to a whole centicent. Integer arithmetic; matches all 42
  figures in Kalshi's General Trading Fees Table. Kalshi rounds per order and the report shows fills,
  so the headline rounds each fill, and every fee figure carries an exact rounding range: unrounded
  to whole-cent rounding on every fill. The report has
  no taker side, which only matters for centicent rounding from Jul 7, 2026 (at most $0.0001 a fill;
  the headline takes the larger side), and newer reports have no time of day, which only matters
  when a series' terms change mid-day (those fills are priced under both terms).
- These are list-price fees, and the range covers rounding only. Some fills pay Kalshi something
  else, and the public report doesn't say which: customers trading through a broker pay a flat
  $0.01 per contract per side (Robinhood Derivatives' fee schedule), market makers with a Kalshi
  agreement may pay reduced fees (Kalshi's Market Maker Program), and Kalshi runs fee-waiver
  programs. None of these is applied.
- The premium measure is contracts × Yes price; "dollars paid by the taker" needs the taker side,
  which the report doesn't carry.

Perps (Kalshi)
- Every fill from the public `GET /margin/trades`, with contract specs from `GET /margin/markets`.
- Notional per fill is derived, not assumed: each run compares fills over the last 24 hours with
  Kalshi's own reported 24-hour notional per market and stops perps if no rule matches within 1%.
- Fees: notional × Kalshi's published perps tiers (fee schedule, effective Jul 7, 2026; taker
  12.0 to 2.6 bps, maker 5.0 to 0.6 bps). A fill's tier depends on the account's 30-day volume,
  which isn't public, so the dashboard shows the full tier range unless you choose tiers.
  Days before Jul 7, 2026 have notional but no fees (no published schedule in hand).
  Funding payments are not fees and are not included.

Hyperliquid
- Its free API has candles with size but not dollars, so daily notional is reported as exact
  bounds: size × candle low to size × candle high. Recent days use 1-minute candles (bounds
  within a few hundredths of a percent); older days use 1-hour candles. Days older than the API's
  5,000-candle window are marked unavailable rather than zero.

Still third-party: Polymarket volumes in the share chart come from DeFi Rate's venue data.

If GitHub ever pauses the nightly schedule (it does this to public repos after 60 days without
activity), re-enable it with `gh workflow enable refresh.yml -R <you>/kalshi-monitor`.
