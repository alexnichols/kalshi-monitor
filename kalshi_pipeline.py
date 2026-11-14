#!/usr/bin/env python3
"""
Kalshi primary-data pipeline for the Kalshi volume monitor.

Builds volume, dollars paid and fees from Kalshi's own public trade tape, pricing
every fill with Kalshi's published fee schedule. No third-party aggregators, no
assumed basis-point rates.

  Trades     GET /markets/trades and GET /historical/trades (pre-cutoff), every fill
  Taxonomy   GET /series (category, fee_type, fee_multiplier)
  Fee terms  GET /series/fee_changes?show_historical=true, GET /events/fee_changes
  Perps      GET /margin/markets (24h notional), Hyperliquid info API (24h notional)

Fee per fill (Kalshi fee schedule, effective Jul 7, 2026):
  taker = roundup(0.07   x M x C x P x (1 - P))
  maker = roundup(0.0175 x M x C x P x (1 - P))  on quadratic_with_maker_fees series
  maker = roundup(0.035  x M x C x P x (1 - P))  on quadratic_with_combo_maker_fees series
P(1-P) is symmetric, so the Yes price works whichever side the taker bought.
Kalshi rounds up per order (to the centicent for direct members, to the cent for
brokered accounts). The public tape has fills, not orders, so the default rounds
each fill up to the centicent; --rounding cent gives an upper bound.

Each UTC day is aggregated to cache/days/YYYY-MM-DD.json, so runs are resumable and
a daily cron only fetches yesterday. A full backfill from Jan 2025 pages through
billions of fills and takes many hours on Kalshi's basic rate tier.

  python3 kalshi_pipeline.py --start 2025-01-01            # begin a backfill (remembered; resumes)
  python3 kalshi_pipeline.py                                # daily: yesterday first, then any backfill gap
  python3 kalshi_pipeline.py --max-minutes 330              # stop fetching after 5.5 h, save progress
  python3 kalshi_pipeline.py --deck deck_tieout.json        # also print the tie-out (keep the deck local)
"""
import argparse, datetime as dt, json, math, os, sys, time
from collections import defaultdict

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("pip install requests")

KALSHI = "https://external-api.kalshi.com/trade-api/v2"
HYPERLIQUID = "https://api.hyperliquid.xyz/info"
COMBO_PREFIX = "KXMVE"
CATS = ["Combos", "Sports", "Crypto", "Commodities", "Elections", "Politics",
        "Financials", "Economics", "Weather", "Other"]
NATIVE = {"sports": "Sports", "crypto": "Crypto", "commodities": "Commodities",
          "elections": "Elections", "politics": "Politics", "financials": "Financials",
          "economics": "Economics", "climate and weather": "Weather", "weather": "Weather",
          "exotics": "Combos", "combos": "Combos"}
TAKER_COEF = 0.07
MAKER_COEF = {"quadratic_with_maker_fees": 0.0175, "quadratic_with_combo_maker_fees": 0.035}
METRICS = ["contracts", "usd", "taker_fees", "maker_fees", "trades"]


def log(m):
    print(m, file=sys.stderr, flush=True)


def num(x):
    try:
        v = float(x)
        return 0.0 if math.isnan(v) else v
    except (TypeError, ValueError):
        return 0.0


def parse_ts(s):
    if s is None or s == "":
        return None
    if isinstance(s, (int, float)):
        return float(s) / (1000.0 if s > 1e12 else 1.0)
    return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()


def round_up(x, mode):
    if mode == "none":
        return x
    unit = 0.01 if mode == "cent" else 0.0001
    return math.ceil(round(x / unit, 9)) * unit


# ------------------------------------------------------------------ fee schedule
class FeeBook:
    """Fee terms in force for a ticker at a time, from Kalshi's own change logs."""

    def __init__(self, series, series_changes, event_changes):
        self.series = series                      # ticker -> {category, fee_type, fee_multiplier}
        self.sc = defaultdict(list)               # series -> [(ts, fee_type, mult)]
        for c in series_changes:
            self.sc[c.get("series_ticker")].append(
                (parse_ts(c.get("scheduled_ts")), c.get("fee_type"), c.get("fee_multiplier")))
        for v in self.sc.values():
            v.sort(key=lambda r: r[0] or 0)
        self.ec = defaultdict(list)               # event -> [(ts, type_override, mult_override)]
        for c in event_changes:
            self.ec[c.get("event_ticker")].append(
                (parse_ts(c.get("scheduled_ts")), c.get("fee_type_override"), c.get("fee_multiplier_override")))
        for v in self.ec.values():
            v.sort(key=lambda r: r[0] or 0)
        self.unknown_series = set()

    @staticmethod
    def series_of(ticker):
        return ticker.split("-", 1)[0]

    def category(self, ticker):
        if ticker.startswith(COMBO_PREFIX):
            return "Combos"
        s = self.series.get(self.series_of(ticker))
        if not s:
            self.unknown_series.add(self.series_of(ticker))
            return "Other"
        return NATIVE.get(str(s.get("category") or "").strip().lower(), "Other")

    def terms(self, ticker, ts):
        st = self.series_of(ticker)
        meta = self.series.get(st) or {}
        hist = self.sc.get(st)
        if hist:
            # Before the first recorded change: Kalshi's standard schedule.
            fee_type, mult = "quadratic", 1.0
            for t, ft, m in hist:
                if t is not None and t <= ts:
                    fee_type = ft or fee_type
                    mult = num(m) if m is not None else mult
        else:
            fee_type = meta.get("fee_type") or "quadratic"
            mult = num(meta.get("fee_multiplier")) if meta.get("fee_multiplier") is not None else 1.0
        # Event-level overrides: match the longest event ticker prefix.
        parts = ticker.split("-")
        for k in range(len(parts) - 1, 0, -1):
            ev = "-".join(parts[:k])
            if ev in self.ec:
                for t, ft, m in self.ec[ev]:
                    if t is not None and t <= ts:
                        if ft is None and m is None:          # override cleared
                            continue
                        fee_type = ft or fee_type
                        mult = num(m) if m is not None else mult
                break
        return fee_type, mult

    def price_fill(self, trade, rounding="centicent"):
        ticker = trade["ticker"]
        ts = parse_ts(trade["created_time"])
        c = num(trade.get("count_fp", trade.get("count")))
        p = num(trade.get("yes_price_dollars"))
        if p == 0 and trade.get("yes_price") is not None:
            p = num(trade.get("yes_price")) / 100.0
        q = num(trade.get("no_price_dollars")) or (1.0 - p)
        side = trade.get("taker_outcome_side") or trade.get("taker_side") or "yes"
        taker_price = p if side == "yes" else q
        fee_type, mult = self.terms(ticker, ts)
        core = c * p * (1.0 - p) * mult
        taker = round_up(TAKER_COEF * core, rounding) if core > 0 else 0.0
        maker = round_up(MAKER_COEF.get(fee_type, 0.0) * core, rounding) if core > 0 and fee_type in MAKER_COEF else 0.0
        return {"category": self.category(ticker), "contracts": c, "usd": c * taker_price,
                "taker_fees": taker, "maker_fees": maker, "trades": 1, "flat": fee_type == "flat"}


# ------------------------------------------------------------------- Kalshi API
class Kalshi:
    def __init__(self, session=None, rps=8.0):
        self.s = session or requests.Session()
        self.min_gap, self.last = 1.0 / rps, 0.0

    def get(self, path, params=None):
        for attempt in range(8):
            wait = self.min_gap - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            r = self.s.get(f"{KALSHI}{path}", params=params or {}, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(60, 2 ** attempt))
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Kalshi {path}: gave up after retries")

    def paged(self, path, key, params=None, limit=1000):
        params = dict(params or {}, limit=limit)
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            page = self.get(path, params)
            for row in page.get(key) or []:
                yield row
            cursor = page.get("cursor")
            if not cursor:
                return


def load_feebook(api, cache_dir):
    path = os.path.join(cache_dir, "fee_terms.json")
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < 86400:
        with open(path) as f:
            d = json.load(f)
    else:
        log("Kalshi: series, series fee changes, event fee changes")
        series = {s["ticker"]: {"category": s.get("category"), "fee_type": s.get("fee_type"),
                                "fee_multiplier": s.get("fee_multiplier")}
                  for s in api.paged("/series", "series")}
        sc = api.get("/series/fee_changes", {"show_historical": "true"}).get("series_fee_change_arr") or []
        ec = list(api.paged("/events/fee_changes", "event_fee_changes", {"show_historical": "true"}, limit=200))
        d = {"series": series, "series_changes": sc, "event_changes": ec}
        os.makedirs(cache_dir, exist_ok=True)
        with open(path, "w") as f:
            json.dump(d, f)
    return FeeBook(d["series"], d["series_changes"], d["event_changes"])


def trades_cutoff(api):
    try:
        c = api.get("/historical/cutoff")
    except Exception as e:
        log(f"  historical cutoff unavailable ({e}); using live endpoint only")
        return None
    for k in ("trades_created_ts", "trade_created_ts", "trades_ts"):
        if c.get(k):
            return parse_ts(c[k])
    return None


def aggregate_day(api, book, day, cutoff, rounding):
    start = dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc).timestamp()
    end = start + 86400
    agg = {c: {m: 0.0 for m in METRICS} for c in CATS}
    flat = 0
    windows = []
    if cutoff is not None and start < cutoff:
        windows.append(("/historical/trades", start, min(end, cutoff)))
    if cutoff is None or end > cutoff:
        windows.append(("/markets/trades", max(start, cutoff or start), end))
    for path, a, b in windows:
        for t in api.paged(path, "trades", {"min_ts": int(a), "max_ts": int(b) - 1}):
            f = book.price_fill(t, rounding)
            row = agg[f["category"]]
            for m in METRICS:
                row[m] += f[m]
            flat += f["flat"]
    return {"day": day.isoformat(), "rounding": rounding, "flat_fee_fills": flat, "categories": agg}


# ------------------------------------------------------------------------ perps
def perps_snapshot(session=None):
    s = session or requests
    out = {}
    try:
        r = s.get(f"{KALSHI}/margin/markets", params={"status": "active"}, timeout=30)
        r.raise_for_status()
        out["kalshi"] = sum(num(m.get("volume_24h_notional_value_dollars")) for m in r.json().get("markets", []))
    except Exception as e:
        log(f"  Kalshi perps skipped ({e})")
    try:
        def dex(name=None):
            body = {"type": "metaAndAssetCtxs", **({"dex": name} if name else {})}
            r = s.post(HYPERLIQUID, json=body, timeout=30)
            r.raise_for_status()
            return sum(num(c.get("dayNtlVlm")) for c in r.json()[1])
        total = dex()
        r = s.post(HYPERLIQUID, json={"type": "perpDexs"}, timeout=30)
        for d in (r.json() if r.ok else []) or []:
            if d and d.get("name"):
                total += dex(d["name"])
        out["hyperliquid"] = total
    except Exception as e:
        log(f"  Hyperliquid skipped ({e})")
    return out


# ---------------------------------------------------------------------- output
def month_key(d):
    return d[:7]


def week_key(d):
    x = dt.date.fromisoformat(d)
    return (x - dt.timedelta(days=x.weekday())).isoformat()


def rollup(days, keyfn, keys):
    blocks = {m: {c: [0.0] * len(keys) for c in CATS} for m in METRICS}
    idx = {k: i for i, k in enumerate(keys)}
    for d in days:
        i = idx.get(keyfn(d["day"]))
        if i is None:
            continue
        for c in CATS:
            for m in METRICS:
                blocks[m][c][i] += d["categories"][c][m]
    for b in blocks.values():
        b["total"] = [sum(b[c][i] for c in CATS) for i in range(len(keys))]
        for k in b:
            b[k] = [round(v, 4) for v in b[k]]
    return blocks


def build_output(days, perps_hist, last_day, first_day=None):
    """Only periods whose every day was fetched are emitted, so a backfill in progress
    never shows a partial month or week as if it were complete."""
    days = sorted(days, key=lambda d: d["day"])
    out = {"source": "Kalshi trade tape, fees from Kalshi's fee schedule", "asOf": last_day.isoformat(),
           "feeMethod": "per fill: 0.07 x M x C x P x (1-P) taker; 0.0175 / 0.035 x M x C x P x (1-P) maker; rounding " +
                        (days[-1]["rounding"] if days else "centicent")}
    if days:
        have = {d["day"] for d in days}
        first = first_day or dt.date.fromisoformat(days[0]["day"])
        span = [(first + dt.timedelta(days=k)).isoformat() for k in range((last_day - first).days + 1)]
        out["coverage"] = {"first": first.isoformat(), "last": last_day.isoformat(), "fetched": sum(x in have for x in span),
                           "total": len(span), "complete": all(x in have for x in span)}
        # Daily block (fetched days only; gaps allowed) for L7D / L28D.
        dd = rollup(days, lambda x: x, [d["day"] for d in days])
        out["days"] = [d["day"] for d in days]
        out["daily"] = {"contracts": dd["contracts"],
                        "usd": dd["usd"],
                        "fees": {c: [round(a + b, 4) for a, b in zip(dd["taker_fees"][c], dd["maker_fees"][c])] for c in dd["taker_fees"]}}
        # Months: every day from the 1st (through last_day for the current month) fetched.
        months = []
        for ym in sorted({x[:7] for x in span}):
            md = [x for x in span if x[:7] == ym]
            if md[0].endswith("-01") and all(x in have for x in md):
                months.append(ym)
        partial = {}
        if months and months[-1] == last_day.strftime("%Y-%m") and last_day.month == (last_day + dt.timedelta(days=1)).month:
            partial[months[-1]] = f"{last_day.strftime('%b')} 1–{last_day.day}"
        # Weeks: complete Monday-to-Sunday weeks with all seven days fetched.
        weeks = sorted({week_key(x) for x in have
                        if dt.date.fromisoformat(week_key(x)) + dt.timedelta(days=6) <= last_day
                        and all((dt.date.fromisoformat(week_key(x)) + dt.timedelta(days=k)).isoformat() in have for k in range(7))})
        m = rollup(days, month_key, months)
        w = rollup(days, week_key, weeks)
        fees_m = {c: [round(a + b, 4) for a, b in zip(m["taker_fees"][c], m["maker_fees"][c])] for c in m["taker_fees"]}
        fees_w = {c: [round(a + b, 4) for a, b in zip(w["taker_fees"][c], w["maker_fees"][c])] for c in w["taker_fees"]}
        out.update(months=months, partialMonths=partial, monthly=m["contracts"], monthlyUsd=m["usd"],
                   monthlyFees=fees_m, monthlyTakerFees=m["taker_fees"], monthlyMakerFees=m["maker_fees"],
                   weeks=weeks, weekly=w["contracts"], weeklyUsd=w["usd"], weeklyFees=fees_w)
    if perps_hist:
        ds = sorted(perps_hist)
        start, end = dt.date.fromisoformat(ds[0]), dt.date.fromisoformat(ds[-1])
        k, h, cur = [], [], start
        while cur <= end:
            r = perps_hist.get(cur.isoformat()) or {}
            k.append(round(r["kalshi"] / 1e9, 4) if r.get("kalshi") is not None else None)
            h.append(round(r["hyperliquid"] / 1e9, 4) if r.get("hyperliquid") is not None else None)
            cur += dt.timedelta(days=1)
        out["perps"] = {"start": start.isoformat(), "kalshi": k, "hyperliquid": h}
    return out


def tieout(out, deck):
    """Compare computed months with the deck. The deck is never an input to the build."""
    rows = []
    for i, ym in enumerate(out.get("months", [])):
        if ym not in deck.get("months", []):
            continue
        j = deck["months"].index(ym)
        vol, fees = out["monthly"]["total"][i], out["monthlyFees"]["total"][i]
        tf = deck["tradingFees"][j]
        pct = deck.get("netRevenuePctVolume", [None] * len(deck["months"]))[j]
        implied = deck["netRevenue"][j] / (pct / 100.0) if pct else None
        rows.append({"month": ym, "volume": vol, "deck_implied_volume": implied,
                     "volume_diff": (vol / implied - 1) if implied else None,
                     "fees": fees, "deck_trading_fees": tf, "fees_diff": fees / tf - 1 if tf else None})
    return rows


def main(argv=None, session=None, today=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="first day to (back)fill, YYYY-MM-DD; remembered in the cache for later runs")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop starting new days after this many minutes (0 = no limit)")
    ap.add_argument("--end", help="last day, default yesterday (UTC)")
    ap.add_argument("--cache", default="cache")
    ap.add_argument("--rounding", choices=["centicent", "cent", "none"], default="centicent")
    ap.add_argument("--rps", type=float, default=8.0, help="Kalshi requests per second")
    ap.add_argument("--refetch", action="store_true", help="recompute days already in the cache")
    ap.add_argument("--skip-trades", action="store_true")
    ap.add_argument("--skip-perps", action="store_true")
    ap.add_argument("--deck", help="optional JSON of deck figures, used only for the tie-out printout")
    ap.add_argument("--out", default="kalshi_dashboard_data.json")
    a = ap.parse_args(argv)
    today = today or dt.datetime.now(dt.timezone.utc).date()
    t_start = time.time()
    last = dt.date.fromisoformat(a.end) if a.end else today - dt.timedelta(days=1)
    day_dir = os.path.join(a.cache, "days")
    os.makedirs(day_dir, exist_ok=True)
    bf_path = os.path.join(a.cache, "backfill.json")
    if a.start:
        first = dt.date.fromisoformat(a.start)
        with open(bf_path, "w") as f:
            json.dump({"start": first.isoformat()}, f)
    elif os.path.exists(bf_path):
        first = dt.date.fromisoformat(json.load(open(bf_path))["start"])
    else:
        first = last - dt.timedelta(days=2)
    # Newest first: yesterday, then backwards, so recent L7D/L28D and months complete first.
    todo = [last - dt.timedelta(days=k) for k in range((last - first).days + 1)]
    todo = [d for d in todo if a.refetch or not os.path.exists(os.path.join(day_dir, f"{d.isoformat()}.json"))]
    stopped_early = False

    if not a.skip_trades:
        api = Kalshi(session, a.rps)
        book = load_feebook(api, a.cache)
        cutoff = trades_cutoff(api)
        for d in todo:
            if a.max_minutes and (time.time() - t_start) / 60 >= a.max_minutes:
                stopped_early = True
                log(f"Time budget reached; {len(todo) - todo.index(d)} days left for the next run.")
                break
            p = os.path.join(day_dir, f"{d.isoformat()}.json")
            t0 = time.time()
            rec = aggregate_day(api, book, d, cutoff, a.rounding)
            with open(p, "w") as f:
                json.dump(rec, f)
            tot = sum(rec["categories"][c]["trades"] for c in CATS)
            log(f"{d}: {int(tot):,} fills in {time.time() - t0:.0f}s")
        if book.unknown_series:
            log(f"  {len(book.unknown_series)} series not in /series (counted as Other): {sorted(book.unknown_series)[:8]}")

    perps_path = os.path.join(a.cache, "perps_history.json")
    perps = json.load(open(perps_path)) if os.path.exists(perps_path) else {}
    if not a.skip_perps:
        snap = perps_snapshot(session)
        if snap:
            perps[last.isoformat()] = snap
            with open(perps_path, "w") as f:
                json.dump(perps, f, indent=1, sort_keys=True)

    days = [json.load(open(os.path.join(day_dir, n))) for n in sorted(os.listdir(day_dir)) if n.endswith(".json")]
    out = build_output(days, perps, last, first)
    with open(a.out, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    missing = sum(1 for k in range((last - first).days + 1)
                  if not os.path.exists(os.path.join(day_dir, f"{(first + dt.timedelta(days=k)).isoformat()}.json")))
    with open(os.path.join(a.cache, "status.json"), "w") as f:
        json.dump({"complete": missing == 0, "missing_days": missing, "first": first.isoformat(),
                   "last": last.isoformat(), "stopped_early": stopped_early}, f)
    log(f"Wrote {a.out} ({len(days)} days cached, {missing} still to fetch).")

    if a.deck:
        for r in tieout(out, json.load(open(a.deck))):
            vd = "n/a" if r["volume_diff"] is None else f"{r['volume_diff']*100:+.1f}%"
            log(f"  {r['month']}  volume {vd:>7}  fees {r['fees']/1e6:8.1f}M vs deck {r['deck_trading_fees']/1e6:8.1f}M  ({r['fees_diff']*100:+.1f}%)")
    return out


if __name__ == "__main__":
    main()
