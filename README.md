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
done; after that it runs nightly at 00:20 UTC. L7D and L28D appear within a few hours; older months fill in over the following days. The page loads `docs/data.json` automatically and
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

## Cost

Public repo: free. Actions minutes are unlimited for public repos and Pages is free.
(A private repo would need GitHub Pro for Pages, and the site would still be public.)

## Method

Event contracts
- Every fill from `GET /markets/trades` and `GET /historical/trades`, kept by its own timestamp so
  day boundaries are exact, categorized with Kalshi's series metadata.
- Fees per Kalshi's fee schedule: taker `round up(M × 0.07 × C × P × (1−P))`, maker at 0.0175
  (combo maker multiplier 2), using each series' and event's fee terms in force on the trade date.
  "Round up" is as the schedule defines it: fee + positionCost lands on a whole centicent.
  Computed in integer arithmetic; matches every row of Kalshi's published fee table.
  The public tape has fills, not orders, so rounding is applied per fill.

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
