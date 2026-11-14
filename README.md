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
Each run fetches for about 5.5 hours, commits progress and restarts itself until every day is
done; after that it runs nightly at 00:20 UTC. The page loads `docs/data.json` automatically and
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

- Every fill from `GET /markets/trades` and `GET /historical/trades`, categorized with Kalshi's
  series metadata.
- Fees: taker `roundup(0.07 × M × C × P × (1−P))`, maker at 0.0175 (0.035 on combo-maker series),
  using each series' and event's fee terms in force on the trade date, from Kalshi's fee-change
  logs. Fills are rounded up to the centicent; Kalshi rounds per order, which the tape doesn't show.
- Perps: 24-hour notional from Kalshi's `/margin/markets` and Hyperliquid's info API, recorded daily.
- Still third-party: Polymarket volumes in the share chart come from DeFi Rate's venue data.
  Perps fees use an assumed rate (5 bps, editable on the page).

If GitHub ever pauses the nightly schedule (it does this to public repos after 60 days without
activity), re-enable it with `gh workflow enable refresh.yml -R <you>/kalshi-monitor`.
