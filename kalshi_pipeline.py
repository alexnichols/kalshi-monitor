#!/usr/bin/env python3
"""
Kalshi primary-data pipeline for the Kalshi volume monitor.

Builds volume, dollars paid and fees from Kalshi's own public trade tape, pricing
every fill with Kalshi's published fee schedule. No third-party aggregators, no
assumed basis-point rates.

  Trades     GET /markets/trades and GET /historical/trades (pre-cutoff), every fill
  Taxonomy   GET /series (category, fee_type, fee_multiplier)
  Fee terms  GET /series/fee_changes?show_historical=true, GET /events/fee_changes
  Perps      GET /margin/trades (every Kalshi perps fill), GET /margin/markets (contract specs);
             notional rule verified each run against Kalshi's own 24h notional
  Hyperliquid info API candles: daily notional as exact bounds, sum(size x low) to sum(size x high)

Fee per fill (Kalshi fee schedule, effective Jul 7, 2026), rounded up so that
fee + positionCost is a whole centicent (cent for brokered accounts):
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
import argparse, datetime as dt, json, math, os, shutil, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
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
from fractions import Fraction
TAKER_COEF = Fraction(7, 100)
MAKER_COEF = {"quadratic_with_maker_fees": Fraction(175, 10000), "quadratic_with_combo_maker_fees": Fraction(35, 1000)}
ROUNDING_UNIT_CC = {"centicent": 1, "cent": 100}          # rounding unit in centicents

# Perpetual futures. Fee tiers from Kalshi's published fee schedule ("Perpetual Futures Fees",
# Tables 1 and 2, effective July 7, 2026). Rates are bps of notional. A fill's tier depends on the
# account's 30-day trailing perps + prediction volume, which the public tape does not show, so
# the output carries notional and these tables, and fees are computed for any tier choice.
PERPS_LAUNCH = dt.date(2026, 5, 29)
PERPS_FEE_SCHEDULES = [{
    "effective": "2026-07-07",
    "source": "Kalshi fee schedule, Perpetual Futures Fees, Tables 1-2 (effective Jul 7, 2026)",
    "volume_threshold_usd": [0, 1e5, 3e5, 1e6, 3e6, 1e7, 3e7, 1e8, 3e8, 1e9, 3e9],
    "taker_bps": [12.0, 10.0, 8.0, 6.0, 5.0, 4.0, 3.5, 3.2, 3.0, 2.8, 2.6],
    "maker_bps": [5.0, 4.0, 3.2, 2.4, 2.0, 1.6, 1.4, 1.2, 1.0, 0.8, 0.6],
}]
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


def units(s, places):
    """Exact decimal string -> integer count of 10**-places units ('0.5600', 6 -> 560000)."""
    s = str(s).strip()
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    a, _, b = s.partition(".")
    if len(b) > places:
        if int(b[places:] or 0) != 0:
            raise ValueError(f"{s} has more than {places} decimals")
        b = b[:places]
    v = int(a or 0) * 10 ** places + int((b + "0" * places)[:places] or 0)
    return -v if neg else v


def as_fraction(x):
    if x is None:
        return Fraction(1)
    return Fraction(str(x)).limit_denominator(10 ** 6)


def kalshi_fee(coef_mult, c_h, p_u, pc_p_u, unit_cc):
    """Kalshi fee schedule: fee = round up(coef x M x C x P x (1-P)), where round up makes
    fee + positionCost a multiple of the rounding unit. Pure integer arithmetic.
    coef_mult: Fraction (coefficient x multiplier); c_h: contracts x 100; p_u: price x 1e6;
    pc_p_u: price paid by this side x 1e6 (for positionCost); unit_cc: rounding unit in centicents.
    Returns the fee in dollars."""
    num, den = coef_mult.numerator, coef_mult.denominator
    R = num * c_h * p_u * (10 ** 6 - p_u)          # fee      x den x 1e14 (dollars)
    if R <= 0:
        return 0.0
    PC = c_h * pc_p_u * den * 10 ** 6               # position x den x 1e14
    U = unit_cc * den * 10 ** 10                    # unit     x den x 1e14
    k = -(-(R + PC) // U)                           # ceil
    return (k * U - PC) / (den * 10 ** 14)


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
            fee_type, mult = "quadratic", 1
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
        c_h = units(trade.get("count_fp", trade.get("count")), 2)
        if trade.get("yes_price_dollars") is not None:
            y_u = units(trade["yes_price_dollars"], 6)
        else:
            y_u = int(trade.get("yes_price", 0)) * 10 ** 4       # legacy cents field
        n_u = units(trade["no_price_dollars"], 6) if trade.get("no_price_dollars") is not None else 10 ** 6 - y_u
        side = trade.get("taker_outcome_side") or trade.get("taker_side") or "yes"
        taker_p, maker_p = (y_u, n_u) if side == "yes" else (n_u, y_u)
        fee_type, mult = self.terms(ticker, ts)
        m = as_fraction(mult)
        unit = ROUNDING_UNIT_CC[rounding]
        taker = kalshi_fee(TAKER_COEF * m, c_h, y_u, taker_p, unit)
        maker = kalshi_fee(MAKER_COEF[fee_type] * m, c_h, y_u, maker_p, unit) if fee_type in MAKER_COEF else 0.0
        c = c_h / 100.0
        return {"category": self.category(ticker), "contracts": c, "usd": c * taker_p / 1e6,
                "taker_fees": taker, "maker_fees": maker, "trades": 1, "flat": fee_type == "flat"}


# ------------------------------------------------------------------- Kalshi API
class Kalshi:
    """Kalshi REST client, safe to share across threads: one global request-rate limit and a
    separate HTTP session per thread."""

    def __init__(self, session=None, rps=8.0):
        self.shared, self.local = session, threading.local()
        self.min_gap, self.next_t, self.lock = 1.0 / rps, 0.0, threading.Lock()

    def _session(self):
        if self.shared is not None:
            return self.shared
        if not hasattr(self.local, "s"):
            self.local.s = requests.Session()
        return self.local.s

    def _wait(self):
        with self.lock:
            now = time.time()
            t = max(now, self.next_t)
            self.next_t = t + self.min_gap
        if t > now:
            time.sleep(t - now)

    def get(self, path, params=None):
        for attempt in range(8):
            self._wait()
            r = self._session().get(f"{KALSHI}{path}", params=params or {}, timeout=60)
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


def in_day(created_time, day_iso):
    """True if a UTC ISO timestamp falls on day_iso. Exact, no second-boundary assumptions."""
    t = str(created_time)
    if t.endswith("Z") or t.endswith("+00:00"):
        return t[:10] == day_iso
    return dt.datetime.fromtimestamp(parse_ts(t), dt.timezone.utc).date().isoformat() == day_iso


def in_window(created_time, hour_prefix, a, b):
    """True if a fill belongs to [a, b). Exact: by its own UTC timestamp, not the query bounds."""
    t = str(created_time)
    if t.endswith("Z") or t.endswith("+00:00"):
        return t[:13] == hour_prefix
    x = parse_ts(t)
    return a <= x < b


def aggregate_slice(api, book, a, b, cutoff, rounding):
    hour_prefix = dt.datetime.fromtimestamp(a, dt.timezone.utc).strftime("%Y-%m-%dT%H")
    agg = {c: {m: 0.0 for m in METRICS} for c in CATS}
    flat = 0
    paths = []
    if cutoff is None or a - 1 < cutoff:
        paths.append("/historical/trades")
    if cutoff is None or b + 1 > cutoff:
        paths.append("/markets/trades")
    for path in paths:
        for t in api.paged(path, "trades", {"min_ts": int(a) - 1, "max_ts": int(b) + 1}):
            if not in_window(t["created_time"], hour_prefix, a, b):
                continue
            f = book.price_fill(t, rounding)
            row = agg[f["category"]]
            for m in METRICS:
                row[m] += f[m]
            flat += f["flat"]
    return {"categories": agg, "flat": flat}


def aggregate_day(api, book, day, cutoff, rounding, slice_root, workers=6, out_of_time=lambda: False):
    """Every fill on one UTC day, as 24 hourly slices fetched in parallel. Each finished slice is
    saved, so a run that runs out of time keeps its progress. Returns None until all 24 are done."""
    start = dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc).timestamp()
    sdir = os.path.join(slice_root, day.isoformat())
    os.makedirs(sdir, exist_ok=True)

    def run(k):
        p = os.path.join(sdir, f"{k:02d}.json")
        if os.path.exists(p):
            return True
        if out_of_time():
            return False
        rec = aggregate_slice(api, book, start + k * 3600, start + (k + 1) * 3600, cutoff, rounding)
        with open(p + ".tmp", "w") as f:
            json.dump(rec, f)
        os.replace(p + ".tmp", p)
        return True

    with ThreadPoolExecutor(max_workers=workers) as ex:
        done = list(ex.map(run, range(24)))
    if not all(done):
        return None
    agg = {c: {m: 0.0 for m in METRICS} for c in CATS}
    flat = 0
    for k in range(24):
        rec = json.load(open(os.path.join(sdir, f"{k:02d}.json")))
        flat += rec["flat"]
        for c in CATS:
            for m in METRICS:
                agg[c][m] += rec["categories"][c][m]
    shutil.rmtree(sdir)
    return {"day": day.isoformat(), "rounding": rounding, "flat_fee_fills": flat, "categories": agg}


# ------------------------------------------------------------------------ perps
def perps_markets(api):
    """Every Kalshi perps market (active, inactive and closed) with its contract spec."""
    out = {}
    for st in ("active", "inactive", "closed"):
        try:
            rows = api.get("/margin/markets", {"status": st}).get("markets") or []
        except Exception as e:
            log(f"  /margin/markets status={st}: {e}")
            rows = []
        for m in rows:
            out[m["ticker"]] = {"asset_class": m.get("asset_class") or "Other",
                                "contract_size": m.get("contract_size"),
                                "underlying_multiplier": m.get("underlying_multiplier"),
                                "volume_24h": num(m.get("volume_24h")),
                                "volume_24h_notional": num(m.get("volume_24h_notional_value_dollars"))}
    return out


NOTIONAL_RULES = {
    "count_x_price": lambda m: 1.0,
    "count_x_price_x_contract_size": lambda m: num(m.get("contract_size")),
    "count_x_price_x_contract_size_x_multiplier": lambda m: num(m.get("contract_size")) * num(m.get("underlying_multiplier")),
}


def verify_perps_notional(api, markets, now_ts, cache_dir, tolerance=0.01):
    """Derive how a fill's notional is computed instead of assuming it. For each market, Kalshi's
    reported 24h notional / 24h contracts is its average notional per contract; the last 24h of
    public fills give the average price per contract. Their ratio identifies the rule, independent
    of small differences in the two 24h windows. Stops the run if no rule matches."""
    px = defaultdict(float)
    ct = defaultdict(float)
    for t in api.paged("/margin/trades", "trades", {"min_ts": int(now_ts - 86400), "max_ts": int(now_ts)}):
        c = units(t["count"], 2) / 100.0
        px[t["ticker"]] += c * units(t["price"], 6) / 1e6
        ct[t["ticker"]] += c
    scores = {r: [0.0, 0.0] for r in NOTIONAL_RULES}       # weighted abs error, weight
    evidence = {}
    for tk, m in markets.items():
        if m["volume_24h"] <= 0 or m["volume_24h_notional"] <= 0 or ct.get(tk, 0) <= 0:
            continue
        kalshi_per_contract = m["volume_24h_notional"] / m["volume_24h"]
        avg_price = px[tk] / ct[tk]
        errs = {}
        for r, f in NOTIONAL_RULES.items():
            k = f(m)
            if k > 0:
                errs[r] = avg_price * k / kalshi_per_contract - 1
                scores[r][0] += abs(errs[r]) * m["volume_24h_notional"]
                scores[r][1] += m["volume_24h_notional"]
        evidence[tk] = errs
    usable = {r: v[0] / v[1] for r, v in scores.items() if v[1] > 0}
    if not usable:
        raise RuntimeError("Perps notional check: no market had both 24h volume and fills to compare.")
    rule = min(usable, key=usable.get)
    if usable[rule] > tolerance:
        raise RuntimeError(f"Perps notional check failed: best rule {rule} is off by {usable[rule]:.2%} "
                           f"against Kalshi's reported 24h notional. Not computing perps until this is resolved.")
    result = {"rule": rule, "weighted_error": usable[rule], "markets_checked": len(evidence),
              "checked_at": dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).isoformat(), "all_rules": usable}
    with open(os.path.join(cache_dir, "perps_notional_rule.json"), "w") as f:
        json.dump({**result, "per_market": evidence}, f, indent=1)
    log(f"Perps notional rule: {rule} (weighted error {usable[rule]:.3%} over {len(evidence)} markets)")
    return result


def aggregate_perps_day(api, day, markets, rule):
    """Every Kalshi perps fill on one UTC day: notional by asset class, contracts and fill count."""
    start = dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc).timestamp()
    iso = day.isoformat()
    f = NOTIONAL_RULES[rule]
    agg = defaultdict(lambda: {"notional": 0.0, "contracts": 0.0, "fills": 0})
    unknown = set()
    for t in api.paged("/margin/trades", "trades", {"min_ts": int(start) - 1, "max_ts": int(start) + 86401}):
        if not in_day(t["created_time"], iso):
            continue
        tk = t["ticker"]
        m = markets.get(tk)
        if m is None:
            try:
                mm = api.get(f"/margin/markets/{tk}").get("market") or {}
                m = markets[tk] = {"asset_class": mm.get("asset_class") or "Other", "contract_size": mm.get("contract_size"),
                                   "underlying_multiplier": mm.get("underlying_multiplier"), "volume_24h": 0, "volume_24h_notional": 0}
            except Exception:
                unknown.add(tk)
                continue
        c = units(t["count"], 2) / 100.0
        a = agg[m["asset_class"]]
        a["notional"] += c * units(t["price"], 6) / 1e6 * f(m)
        a["contracts"] += c
        a["fills"] += 1
    if unknown:
        raise RuntimeError(f"Perps fills on {iso} for markets with no spec: {sorted(unknown)[:5]}")
    return {"day": iso, "rule": rule, "classes": dict(agg)}


class Hyperliquid:
    """Hyperliquid's public info API. Candles carry size, not dollars, so each day's notional is
    reported as exact bounds: sum(size x low) <= notional <= sum(size x high)."""

    def __init__(self, session=None, weight_per_min=1000):
        self.s = session or requests.Session()
        self.budget, self.window = weight_per_min, []

    def post(self, body, weight=20):
        while True:
            now = time.time()
            self.window = [(t, w) for t, w in self.window if now - t < 60]
            if sum(w for _, w in self.window) + weight <= self.budget:
                break
            time.sleep(1)
        for attempt in range(8):
            r = self.s.post(HYPERLIQUID, json=body, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(60, 2 ** attempt))
                continue
            r.raise_for_status()
            self.window.append((time.time(), weight))
            return r.json()
        raise RuntimeError(f"Hyperliquid {body.get('type')}: gave up after retries")

    def coins(self):
        names = [u["name"] for u in self.post({"type": "meta"})["universe"]]
        for d in self.post({"type": "perpDexs"}) or []:
            if d and d.get("name"):
                for u in self.post({"type": "meta", "dex": d["name"]})["universe"]:
                    names.append(u["name"] if ":" in u["name"] else f"{d['name']}:{u['name']}")
        return names

    def candles(self, coin, interval, start_ms, end_ms):
        rows = self.post({"type": "candleSnapshot", "req": {"coin": coin, "interval": interval,
                                                           "startTime": start_ms, "endTime": end_ms}})
        self.window.append((time.time(), len(rows or []) // 60))
        return rows or []


def hl_fill_days(hl, days, now_ts, hdir, out_of_time):
    """Compute Hyperliquid daily notional bounds for the given UTC days. Days whose every minute
    is still inside the 5,000-candle 1m window use 1m candles; older days use 1h candles."""
    if not days:
        return
    coins = hl.coins()
    oldest_1m = now_ts - 4990 * 60
    start_of = lambda d: dt.datetime.combine(d, dt.time(), tzinfo=dt.timezone.utc).timestamp()
    fine = [d for d in days if start_of(d) >= oldest_1m]
    oldest_1h = now_ts - 4990 * 3600
    coarse = [d for d in days if d not in fine and start_of(d) >= oldest_1h]
    for d in days:
        if d not in fine and d not in coarse:   # outside the free API's 5,000-candle window: no data, not zero
            with open(os.path.join(hdir, f"{d.isoformat()}.json"), "w") as f:
                json.dump({"day": d.isoformat(), "method": "unavailable", "low": None, "high": None, "mid": None,
                           "trades": None, "coins": 0}, f)
    for interval, group in (("1m", fine), ("1h", coarse)):
        if not group or out_of_time():
            continue
        lo_ms = int(dt.datetime.combine(min(group), dt.time(), tzinfo=dt.timezone.utc).timestamp() * 1000)
        hi_ms = int(dt.datetime.combine(max(group), dt.time(), tzinfo=dt.timezone.utc).timestamp() * 1000) + 86400000
        acc = {d.isoformat(): {"low": 0.0, "high": 0.0, "mid": 0.0, "trades": 0, "coins": 0} for d in group}
        for coin in coins:
            if out_of_time():
                log("Time budget reached during Hyperliquid candles; those days will be retried.")
                return
            seen = set()
            for c in hl.candles(coin, interval, lo_ms, hi_ms):
                day = dt.datetime.fromtimestamp(c["t"] / 1000, dt.timezone.utc).date().isoformat()
                if day not in acc:
                    continue
                v = num(c["v"])
                if v <= 0:
                    continue
                a = acc[day]
                a["low"] += v * num(c["l"])
                a["high"] += v * num(c["h"])
                a["mid"] += v * (num(c["o"]) + num(c["h"]) + num(c["l"]) + num(c["c"])) / 4
                a["trades"] += int(c.get("n") or 0)
                seen.add(day)
            for day in seen:
                acc[day]["coins"] += 1
        for day, a in acc.items():
            with open(os.path.join(hdir, f"{day}.json"), "w") as f:
                json.dump({"day": day, "method": interval, **a}, f)
            log(f"Hyperliquid {day} ({interval}): ${a['low']/1e9:.2f}B to ${a['high']/1e9:.2f}B")


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


def build_output(days, perps_days, hl_days, last_day, first_day=None, notional_rule=None):
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
    if perps_days:
        pdays = sorted(perps_days, key=lambda d: d["day"])
        hmap = {h["day"]: h for h in hl_days}
        classes = sorted({c for d in pdays for c in d["classes"]})
        out["perps"] = {
            "days": [d["day"] for d in pdays],
            "kalshi": {"total": [round(sum(v["notional"] for v in d["classes"].values()), 2) for d in pdays],
                       "classes": {c: [round(d["classes"].get(c, {}).get("notional", 0.0), 2) for d in pdays] for c in classes},
                       "contracts": [round(sum(v["contracts"] for v in d["classes"].values()), 2) for d in pdays],
                       "fills": [sum(v["fills"] for v in d["classes"].values()) for d in pdays]},
            "hyperliquid": {k: [round(hmap[d["day"]][k], 2) if d["day"] in hmap and hmap[d["day"]][k] is not None else None
                                for d in pdays] for k in ("low", "high", "mid")},
            "hyperliquidMethod": [hmap[d["day"]]["method"] if d["day"] in hmap else None for d in pdays],
            "feeSchedules": PERPS_FEE_SCHEDULES,
            "notionalRule": notional_rule,
        }
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


def main(argv=None, session=None, hl_session=None, today=None, now_ts=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="first event-contract day to (back)fill, YYYY-MM-DD; remembered in the cache")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop starting new work after this many minutes (0 = no limit)")
    ap.add_argument("--end", help="last day, default yesterday (UTC)")
    ap.add_argument("--cache", default="cache")
    ap.add_argument("--rounding", choices=list(ROUNDING_UNIT_CC), default="centicent")
    ap.add_argument("--rps", type=float, default=8.0, help="Kalshi requests per second (shared by all workers)")
    ap.add_argument("--workers", type=int, default=6, help="hourly slices fetched in parallel")
    ap.add_argument("--refetch", action="store_true", help="recompute days already in the cache")
    ap.add_argument("--skip-trades", action="store_true", help="skip event contracts")
    ap.add_argument("--skip-perps", action="store_true", help="skip Kalshi perps and Hyperliquid")
    ap.add_argument("--deck", help="optional JSON of reference figures, used only for the tie-out printout")
    ap.add_argument("--out", default="kalshi_dashboard_data.json")
    a = ap.parse_args(argv)
    t_start = time.time()
    out_of_time = lambda: bool(a.max_minutes) and (time.time() - t_start) / 60 >= a.max_minutes
    today = today or dt.datetime.now(dt.timezone.utc).date()
    now_ts = now_ts or time.time()
    last = dt.date.fromisoformat(a.end) if a.end else today - dt.timedelta(days=1)
    dirs = {k: os.path.join(a.cache, k) for k in ("days", "perps_days", "hl_days")}
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    have = lambda kind, d: os.path.exists(os.path.join(dirs[kind], f"{d.isoformat()}.json"))

    bf_path = os.path.join(a.cache, "backfill.json")
    if a.start:
        first = dt.date.fromisoformat(a.start)
        with open(bf_path, "w") as f:
            json.dump({"start": first.isoformat()}, f)
    elif os.path.exists(bf_path):
        first = dt.date.fromisoformat(json.load(open(bf_path))["start"])
    else:
        first = last - dt.timedelta(days=2)
    newest_first = lambda lo: [last - dt.timedelta(days=k) for k in range((last - lo).days + 1)]
    ev_todo = [d for d in newest_first(first) if a.refetch or not have("days", d)]
    pk_todo = [d for d in newest_first(PERPS_LAUNCH) if a.refetch or not have("perps_days", d)]
    hl_todo = [d for d in newest_first(PERPS_LAUNCH) if a.refetch or not have("hl_days", d)]

    api = Kalshi(session, a.rps)
    stopped_early, notional_rule, perps_error = False, None, None
    book = cutoff = markets = rule = None
    if not a.skip_trades:
        book = load_feebook(api, a.cache)
        cutoff = trades_cutoff(api)
    if not a.skip_perps:
        try:
            markets = perps_markets(api)
            notional_rule = verify_perps_notional(api, markets, now_ts, a.cache)
            rule = notional_rule["rule"]
            hl = Hyperliquid(hl_session)
        except Exception as e:                      # never let perps block event contracts
            perps_error = str(e)
            log(f"PERPS SKIPPED THIS RUN: {perps_error}")

    def do_events(d):
        t0 = time.time()
        rec = aggregate_day(api, book, d, cutoff, a.rounding, os.path.join(a.cache, "slices"), a.workers, out_of_time)
        if rec is None:
            n = len(os.listdir(os.path.join(a.cache, "slices", d.isoformat())))
            log(f"{d}: {n} of 24 hourly slices saved; the rest go to the next run.")
            return
        with open(os.path.join(dirs["days"], f"{d.isoformat()}.json"), "w") as f:
            json.dump(rec, f)
        log(f"{d}: {int(sum(rec['categories'][c]['trades'] for c in CATS)):,} fills in {time.time() - t0:.0f}s")

    def do_perps(d):
        rec = aggregate_perps_day(api, d, markets, rule)
        with open(os.path.join(dirs["perps_days"], f"{d.isoformat()}.json"), "w") as f:
            json.dump(rec, f)
        log(f"{d} perps: ${sum(v['notional'] for v in rec['classes'].values())/1e9:.2f}B over "
            f"{sum(v['fills'] for v in rec['classes'].values()):,} fills")

    # Yesterday for every stream first, then the short perps backfills, then the long event backfill.
    plan = []
    if not a.skip_trades and last in ev_todo:
        plan.append(("events", last))
    if not a.skip_perps and perps_error is None:
        plan += [("perps", d) for d in pk_todo]
        plan.append(("hyperliquid", hl_todo))
    if not a.skip_trades:
        plan += [("events", d) for d in ev_todo if d != last]
    for kind, arg in plan:
        if kind != "events" and perps_error:
            continue
        if out_of_time():
            stopped_early = True
            log("Time budget reached; remaining days go to the next run.")
            break
        try:
            if kind == "events":
                do_events(arg)
            elif kind == "perps":
                do_perps(arg)
            else:
                hl_fill_days(hl, arg, now_ts, dirs["hl_days"], out_of_time)
        except Exception as e:
            if kind == "events":
                raise
            perps_error = f"{kind}: {e}"
            log(f"PERPS STOPPED THIS RUN: {perps_error}")
    if notional_rule is None:
        rp = os.path.join(a.cache, "perps_notional_rule.json")
        if os.path.exists(rp):
            notional_rule = {k: v for k, v in json.load(open(rp)).items() if k != "per_market"}

    load = lambda kind: [json.load(open(os.path.join(dirs[kind], n))) for n in sorted(os.listdir(dirs[kind])) if n.endswith(".json")]
    out = build_output(load("days"), load("perps_days"), load("hl_days"), last, first, notional_rule)
    if perps_error:
        out["perpsError"] = perps_error
    with open(a.out, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    missing = {k: sum(1 for d in v if not have(n, d)) for k, v, n in
               (("events", ev_todo, "days"), ("perps", pk_todo, "perps_days"), ("hyperliquid", hl_todo, "hl_days"))}
    stopped_early = stopped_early or any(missing.values())
    with open(os.path.join(a.cache, "status.json"), "w") as f:
        json.dump({"complete": not any(missing.values()), "missing_days": missing, "first": first.isoformat(),
                   "last": last.isoformat(), "stopped_early": stopped_early}, f)
    log(f"Wrote {a.out}. Still to fetch: {missing}.")

    if a.deck:
        for r in tieout(out, json.load(open(a.deck))):
            vd = "n/a" if r["volume_diff"] is None else f"{r['volume_diff']*100:+.1f}%"
            log(f"  {r['month']}  volume {vd:>7}  fees {r['fees']/1e6:8.1f}M vs reference {r['deck_trading_fees']/1e6:8.1f}M  ({r['fees_diff']*100:+.1f}%)")
    return out


if __name__ == "__main__":
    main()
