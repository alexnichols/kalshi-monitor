#!/usr/bin/env python3
"""
Kalshi primary-data pipeline for the Kalshi volume monitor.

Builds volume and fees from Kalshi's own published data, pricing every fill with Kalshi's
published fee schedule. No third-party aggregators, no assumed basis-point rates.

  Trades     Kalshi's daily trade report (kalshi-public-docs/reporting/trade_data_YYYY-MM-DD.json[.gz],
             the file behind kalshi.com/market-data): one row per fill (ticker, series, contracts,
             Yes price), by Kalshi's reporting day in Eastern time. Matches GET /historical/trades
             fill for fill; one download per day instead of tens of thousands of API pages.
  Taxonomy   GET /series (category, fee_type, fee_multiplier); unknown series via market/event endpoints
  Fee terms  GET /series/fee_changes?show_historical=true, GET /events/fee_changes, and before that
             log begins, Kalshi's published fee schedules (PDF_REGIMES)
  Perps      GET /margin/trades (every Kalshi perps fill, UTC days), GET /margin/markets (contract specs);
             notional rule verified each run against Kalshi's own 24h notional
  Hyperliquid info API candles: daily notional as exact bounds, sum(size x low) to sum(size x high)

Fees per fill: taker = round up(0.07 x M x C x P x (1-P)); maker = round up(0.0175 x M x C x P x (1-P))
on maker-fee series (M x 2 on combo-maker series; round up(0.0025 x C) May 13 - Jul 7, 2025).
Before Jul 7, 2026 the fee rounds up to the cent on its own (exact per fill); from then fee +
positionCost rounds to the centicent. Every figure carries low (unrounded) and high (whole cent per
fill) bounds, since Kalshi rounds per order and the report has neither orders nor the taker side.

  python3 kalshi_pipeline.py --start 2025-01-01            # begin a backfill (remembered; resumes)
  python3 kalshi_pipeline.py                                # daily: yesterday, then any gap
  python3 kalshi_pipeline.py --max-minutes 105              # stop starting new work after 105 min
  python3 kalshi_pipeline.py --deck deck_tieout.json        # also print the tie-out (keep the deck local)
"""
import argparse, datetime as dt, gzip, json, math, os, re, sys, threading, time
from concurrent.futures import Future, ProcessPoolExecutor
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
from zoneinfo import ZoneInfo
TAKER_COEF = Fraction(7, 100)
MAKER_QUAD_COEF = Fraction(175, 10000)                   # x maker multiplier (2 on combo-maker series)
ET = ZoneInfo("America/New_York")


def et_midnight(iso):
    """Kalshi's 'effective <date>' read as 00:00 Eastern on that date, as a UTC timestamp."""
    return dt.datetime.combine(dt.date.fromisoformat(iso), dt.time(), tzinfo=ET).timestamp()


# From Jul 7, 2026: "round up such that the fee + positionCost is rounded to a centicent".
# Before that every published schedule says "round up = rounds to the next cent" on the fee itself.
POSITION_COST_ROUNDING_FROM = et_midnight("2026-07-07")

# Kalshi's published fee schedules before its machine-readable change log (GET /series/fee_changes)
# begins. Each entry is transcribed from an archived copy of https://kalshi.com/docs/kalshi-fee-schedule.pdf
# (Wayback Machine snapshot named in "source"). Maker lists are the series tickers each version names.
_MAKER_2025_05 = ("KXAAAGASM KXGDP KXPAYROLLS KXU3 KXEGGS KXCPI KXCPIYOY KXFEDDECISION KXFED KXNBA KXNBAEAST "
                  "KXNBAWEST KXNBASERIES KXNHL KXNHLEAST KXNHLWEST KXNHLSERIES KXINDY500 KXPGA KXUSOPEN KXPGARYDER "
                  "KXTHEOPEN KXPGASOLHEIM KXFOMENSINGLES KXFOWOMENSINGLES KXWMENSINGLES KXWWOMENSINGLES "
                  "KXUSOMENSINGLES KXUSOWOMENSINGLES").split()
_MAKER_2025_06 = _MAKER_2025_05 + ("KXNBAGAME KXNHLGAME KXAOMENSINGLES KXAOWOMENSINGLES KXNFLGAME KXUEFACL "
                                   "KXNBAFINALSMVP KXCONNSMYTHE KXFOMEN KXFOWOMEN KXNATHANSHD KXNATHANDOGS KXCLUBWC "
                                   "KXTOURDEFRANCE KXNASCARRACE").split()
_MAKER_2025_07 = _MAKER_2025_06 + "KXATPMATCH KXWTAMATCH KXMLBASGAME KXMLBHRDERBY".split()
_MAKER_2025_09 = _MAKER_2025_07 + ("KXMLB KXMLBAL KXMLBNL KXNFLMVP KXNFLOPOTY KXNFLDPOTY KXNFLOROTY KXNFLDROTY "
                                   "KXNFLCPOTY KXNFLCOTY KXNCAAF KXNCAAFACC KXNCAAFSEC KXNCAAFB12 KXNCAAFB10 "
                                   "KXNCAAFGAME KXHEISMAN KXNFLWINS KXNFLAFCNORTH KXNFLAFCEAST KXNFLAFCWEST "
                                   "KXNFLAFCSOUTH KXNFLNFCNORTH KXNFLNFCEAST KXNFLNFCWEST KXNFLNFCSOUTH KXNFLAFCCHAMP "
                                   "KXNFLNFCCHAMP KXSB KXSUPERBOWLHEADLINE KXF1RACE KXF1RACEPODIUM KXBTCMAX150 KXBTCMAX "
                                   "KXBTCMAX125 KXETHMAX KXBTCMIN KXETHMIN KXEMMYDSERIES KXEMMYCSERIES KXEMMYDACTO "
                                   "KXEMMYCACTO KXEMMYDACTR KXEMMYCACTR KXRATECUTCOUNT KXIPO KXINXY KXNASDAQ100Y "
                                   "KXPGATOUR KXEPLGAME KXEPLTOP4 KXEPL KXLLM1").split()
_MAKER_2025_09_18 = [s for s in _MAKER_2025_09 if s not in ("KXAOWOMENSINGLES", "KXBTCMAX", "KXETHMAX", "KXBTCMIN", "KXETHMIN", "KXEPL")] + \
    ("KXBTCMAXY KXETHMAXY KXBTCMINY KXETHMINY KXPREMIERLEAGUE KXNFLSPREAD KXNFLTOTAL KXNFLANYTD KXNFLFIRSTTD "
     "KXNFL2TD KXNASCAR KXUCL KXUCLGAME KXPFAPOY KXPFAROY KXBALLONDOR").split()
PDF_REGIMES = [   # (effective UTC ts, maker kind, maker series, source)
    (et_midnight("2025-01-01"), None, [], "fee schedule archived 2025-01-08 (no maker fees; 2024-election markets fee-free)"),
    (et_midnight("2025-02-01"), None, [], "fee schedule archived 2025-02-01 (2024-election exemption removed)"),
    (et_midnight("2025-05-13"), "flat", _MAKER_2025_05, "fee schedule 'Last Updated: May 13, 2025' (maker = round up(0.0025 x C))"),
    (et_midnight("2025-06-18"), "flat", _MAKER_2025_06, "fee schedule effective June 18, 2025"),
    (et_midnight("2025-07-08"), "quad", _MAKER_2025_07, "fee schedule effective July 8, 2025 (maker = round up(0.0175 x C x P x (1-P)))"),
    (et_midnight("2025-09-02"), "quad", _MAKER_2025_09, "fee schedule effective Sept 2, 2025"),
    (dt.datetime(2025, 9, 18, 3, tzinfo=ET).timestamp(), "quad", _MAKER_2025_09_18, "fee schedule effective Sept 17, 2025 (new series live 09/18/2025 3am ET)"),
]
# KXATPMATCH and KXWTAMATCH: maker fees "until market close on July 12, 2025", then off until the Sept 2 list.
PDF_MAKER_UNTIL = {"KXATPMATCH": (et_midnight("2025-07-13"), et_midnight("2025-09-02")),
                   "KXWTAMATCH": (et_midnight("2025-07-13"), et_midnight("2025-09-02"))}
ELECTION_2024_FREE_UNTIL = et_midnight("2025-02-01")


def is_inx_series(s):
    """S&P 500 and Nasdaq-100 series: Rulebook tickers beginning INX or NASDAQ100 (0.035 schedule)."""
    s = s[2:] if s.startswith("KX") else s
    return s.startswith("INX") or s.startswith("NASDAQ100")

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
FEE_METHOD = ("per fill, with each series' terms in force on the trade date: taker = round up(0.07 x M x C x P x (1-P)), "
              "maker = round up(0.0175 x M x C x P x (1-P)) on maker-fee series (M = 2 on combo-maker series; "
              "round up(0.0025 x C) May 13 - Jul 7, 2025). Rounding: next cent on the fee before Jul 7, 2026; from then "
              "fee + positionCost to the centicent (point) or cent (upper bound). Lower bound: unrounded.")


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


def kalshi_fee(coef_mult, c_h, p_u, pc_p_u, unit_cc, with_position_cost=True):
    """Kalshi fee schedule: fee = round up(coef x M x C x P x (1-P)). Pure integer arithmetic.
    with_position_cost=True (schedule from Jul 7, 2026): round up so fee + positionCost is a multiple
    of the unit. False (earlier schedules): round the fee itself up to the unit.
    coef_mult: Fraction (coefficient x multiplier); c_h: contracts x 100; p_u: price x 1e6;
    pc_p_u: price paid by this side x 1e6 (for positionCost); unit_cc: rounding unit in centicents.
    Returns the fee in dollars."""
    num, den = coef_mult.numerator, coef_mult.denominator
    R = num * c_h * p_u * (10 ** 6 - p_u)          # fee      x den x 1e14 (dollars)
    if R <= 0:
        return 0.0
    PC = c_h * pc_p_u * den * 10 ** 6 if with_position_cost else 0   # position x den x 1e14
    U = unit_cc * den * 10 ** 10                    # unit     x den x 1e14
    k = -(-(R + PC) // U)                           # ceil
    return (k * U - PC) / (den * 10 ** 14)


def flat_maker_fee(c_h):
    """May-Jul 2025 maker fee: round up(0.0025 x C) to the next cent."""
    return -(-(c_h * 25) // 10 ** 4) / 100.0 if c_h > 0 else 0.0


MAKER_KIND = {"quadratic": None, "quadratic_with_maker_fees": "quad", "quadratic_with_combo_maker_fees": "quad"}
MAKER_MULT = {"quadratic_with_combo_maker_fees": 2}       # combo maker = 0.035 = 0.0175 x 2


class FeeBook:
    """Fee terms in force for a series at a time, from Kalshi's own records, in this order:
      1. the latest GET /series/fee_changes entry at or before the trade;
      2. a series with no change entries, after that log begins: its current GET /series terms
         (any change since would be in the log);
      3. otherwise the published fee schedule in force (PDF_REGIMES).
    S&P 500 / Nasdaq-100 series carry the 0.035 schedule until Kalshi's first logged INX change."""

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
        all_ts = [t for v in self.sc.values() for t, _, _ in v if t is not None]
        self.log_start = min(all_ts) if all_ts else float("inf")
        inx_ts = [t for s, v in self.sc.items() if is_inx_series(s) for t, _, _ in v if t is not None]
        self.inx_end = min(inx_ts) if inx_ts else POSITION_COST_ROUNDING_FROM
        self.pdf_maker = [(ts, kind, set(lst)) for ts, kind, lst, _ in PDF_REGIMES]
        self.boundaries = sorted({r[0] for r in PDF_REGIMES} | {self.log_start, self.inx_end, POSITION_COST_ROUNDING_FROM,
                                  ELECTION_2024_FREE_UNTIL} | {b for v in PDF_MAKER_UNTIL.values() for b in v})
        self._cache = {}
        self.hyphenated = {s for s in list(series) + list(self.sc) if s and "-" in s}

    def series_of(self, ticker):
        """Series = ticker prefix before the first '-', unless a known series name itself has a hyphen
        (e.g. KXMVECROSSCATEGORY-SHARD1)."""
        head, _, rest = ticker.partition("-")
        if rest and self.hyphenated:
            two = head + "-" + rest.split("-", 1)[0]
            if two in self.hyphenated:
                return two
        return head

    def native_category(self, series):
        s = self.series.get(series)
        return None if s is None else (s.get("category") or "")

    def _pdf_terms(self, s, ts):
        maker_kind = None
        for t0, kind, members in self.pdf_maker:
            if t0 <= ts:
                maker_kind = kind if s in members else None
        if maker_kind and s in PDF_MAKER_UNTIL and PDF_MAKER_UNTIL[s][0] <= ts < PDF_MAKER_UNTIL[s][1]:
            maker_kind = None
        return 1.0, maker_kind, (1.0 if maker_kind else 0.0)

    def _series_terms(self, s, ts):
        """(taker multiplier, maker kind, maker multiplier) for a series at ts, before event overrides."""
        hist = self.sc.get(s)
        logged = [r for r in hist if r[0] is not None and r[0] <= ts] if hist else []
        if logged:
            _, ft, m = logged[-1]
            m = num(m) if m is not None else 1.0
            kind = MAKER_KIND.get(ft)
            return m, kind, (m * MAKER_MULT.get(ft, 1) if kind else 0.0)
        if ts >= self.log_start and not hist and s in self.series:
            meta = self.series[s]
            ft = meta.get("fee_type") or "quadratic"
            m = num(meta.get("fee_multiplier")) if meta.get("fee_multiplier") is not None else 1.0
            kind = MAKER_KIND.get(ft)
            t, mk, mm = m, kind, (m * MAKER_MULT.get(ft, 1) if kind else 0.0)
        else:
            t, mk, mm = self._pdf_terms(s, ts)
        if is_inx_series(s) and ts < self.inx_end:
            t = 0.5
        return t, mk, mm

    def terms(self, ticker, ts, series=None):
        s = series or self.series_of(ticker)
        if ts < ELECTION_2024_FREE_UNTIL and ticker.startswith("PRES-2024"):
            return 0.0, None, 0.0
        # Terms only change at known boundaries, so cache per series between them.
        hist = self.sc.get(s)
        c = self._cache.get(s)
        if c and c[0] <= ts < c[1]:
            base = c[2]
        else:
            bs = self.boundaries + ([t for t, _, _ in hist if t is not None] if hist else [])
            lo = max([b for b in bs if b <= ts], default=float("-inf"))
            hi = min([b for b in bs if b > ts], default=float("inf"))
            base = self._series_terms(s, ts)
            self._cache[s] = (lo, hi, base)
        if not self.ec:
            return base
        t, mk, mm = base
        parts = ticker.split("-")                 # event-level overrides: longest event-ticker prefix
        for k in range(len(parts) - 1, 0, -1):
            ev = "-".join(parts[:k])
            if ev in self.ec:
                for t0, ft, m in self.ec[ev]:
                    if t0 is not None and t0 <= ts and not (ft is None and m is None):
                        if ft:
                            mk = MAKER_KIND.get(ft)
                        if m is not None:
                            t = num(m)
                        mm = t * MAKER_MULT.get(ft, 1) if mk else 0.0
                break
        return t, mk, mm


# ------------------------------------------------------------------- Kalshi API
class Kalshi:
    """Kalshi REST client, safe to share across threads: one global request-rate limit and a
    separate HTTP session per thread."""

    def __init__(self, session=None, rps=4.0, sleep=time.sleep):
        self.shared, self.local = session, threading.local()
        self.min_gap, self.next_t, self.lock = 1.0 / rps, 0.0, threading.Lock()
        self.sleep = sleep
        self.stats = {"requests": 0, "pages": 0, "http_429": 0, "http_5xx": 0, "network_errors": 0}

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
            self.sleep(t - now)

    def _count(self, k):
        with self.lock:
            self.stats[k] += 1

    def get(self, path, params=None, max_failures=8, max_429=60):
        """GET with retries. 429 means slow down, not failure: it backs off and pushes back every
        worker's next request, and only gives up after max_429 in a row. Network exceptions and
        5xx are retried with exponential backoff up to max_failures."""
        failures = throttled = 0
        while True:
            self._wait()
            self._count("requests")
            try:
                r = self._session().get(f"{KALSHI}{path}", params=params or {}, timeout=60)
            except requests.RequestException as e:
                self._count("network_errors")
                failures += 1
                if failures >= max_failures:
                    raise RuntimeError(f"Kalshi {path}: {failures} network errors in a row ({type(e).__name__})") from e
                self.local.s = requests.Session() if self.shared is None else self.shared
                self.sleep(min(60, 2 ** failures))
                continue
            if r.status_code == 429:
                self._count("http_429")
                throttled += 1
                if throttled >= max_429:
                    raise RuntimeError(f"Kalshi {path}: rate-limited {throttled} times in a row")
                pause = min(30, 1.0 * throttled)
                with self.lock:
                    self.next_t = max(self.next_t, time.time() + pause)
                self.sleep(pause)
                continue
            if r.status_code >= 500:
                self._count("http_5xx")
                failures += 1
                if failures >= max_failures:
                    raise RuntimeError(f"Kalshi {path}: HTTP {r.status_code} {failures} times in a row")
                self.sleep(min(60, 2 ** failures))
                continue
            r.raise_for_status()
            return r.json()

    def paged(self, path, key, params=None, limit=1000):
        params = dict(params or {}, limit=limit)
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            page = self.get(path, params)
            self._count("pages")
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


def in_day(created_time, day_iso):
    """True if a UTC ISO timestamp falls on day_iso. Exact, no second-boundary assumptions."""
    t = str(created_time)
    if t.endswith("Z") or t.endswith("+00:00"):
        return t[:10] == day_iso
    return dt.datetime.fromtimestamp(parse_ts(t), dt.timezone.utc).date().isoformat() == day_iso


REPORT_URL = "https://kalshi-public-docs.s3.amazonaws.com/reporting/trade_data_{day}{ext}"
# One row per fill: ticker, series, report day (Eastern), create_ts, contracts, Yes price in cents.
# Older files are compact JSON with real fill times; newer ones pretty-printed with create_ts at midnight.
REPORT_ROW = re.compile(rb'"ticker_name":\s*"((?:[^"\\]|\\.)*)",\s*"report_ticker":\s*"([^"]*)",\s*"date":\s*"([^"]*)",'
                        rb'\s*"create_ts":\s*"([^"]*)",\s*"contracts_traded":\s*"?([0-9.]+)"?,\s*"price":\s*"?([0-9.]+)"?')
SCHEMA = 3
FIELDS = ["contracts", "fills", "yes_premium", "taker_low", "taker_point", "taker_high", "maker_low", "maker_point", "maker_high"]


class ReportMissing(Exception):
    """Kalshi has not published the trade report for this day (yet)."""


def report_chunks(day, session=None, chunk=1 << 24):
    """Stream Kalshi's daily trade report (the file behind kalshi.com/market-data). Newer days are
    gzipped (.json.gz), older ones plain .json. Yields decompressed bytes."""
    s = session or requests.Session()
    for ext in (".json.gz", ".json"):
        url = REPORT_URL.format(day=day.isoformat(), ext=ext)
        for attempt in range(6):
            try:
                r = s.get(url, stream=True, timeout=120)
            except requests.RequestException:
                time.sleep(min(60, 2 ** attempt))
                continue
            if r.status_code in (403, 404):
                r.close()
                break
            if r.status_code >= 500 or r.status_code == 429:
                r.close()
                time.sleep(min(60, 2 ** attempt))
                continue
            r.raise_for_status()
            r.raw.decode_content = True
            src = gzip.GzipFile(fileobj=r.raw) if ext == ".json.gz" else r.raw
            while True:
                b = src.read(chunk)
                if not b:
                    return
                yield b
        else:
            raise RuntimeError(f"trade report {day}: gave up after retries")
    raise ReportMissing(f"trade report for {day} not published")


def report_rows(chunks):
    """(ticker, series, date, create_ts, contracts, price_cents) per fill, streamed in constant memory."""
    carry = b""
    for chunk in chunks:
        buf = carry + chunk
        last = 0
        for m in REPORT_ROW.finditer(buf):
            yield tuple(x.decode() for x in m.groups())
            last = m.end()
        carry = buf[last:]
        if len(carry) > 1 << 20:
            raise ValueError("trade report: unrecognised row format")
    if carry.strip(b" \n\r\t]}"):
        raise ValueError(f"trade report: trailing data not parsed: {carry[:120]!r}")


def fill_fees(terms, c_h, p_u, pc_regime):
    """Fees for one fill under (taker M, maker kind, maker M), as (taker low, point, high, maker low, point, high).
    low = unrounded (the least any order-level rounding can charge). Before Jul 7, 2026 the fee is rounded up
    to the cent on its own, which doesn't depend on the side, so point = high is exact per fill. From then,
    fee + positionCost is rounded to the centicent; the report omits the taker side, so point takes the
    larger of the two sides (at most $0.0001 above the other) and high rounds to the whole cent."""
    tm, mk, mm = terms
    q = c_h * p_u * (10 ** 6 - p_u)
    out = []
    for coef in (TAKER_COEF * as_fraction(tm) if tm else None,
                 (MAKER_QUAD_COEF * as_fraction(mm)) if mk == "quad" and mm else None):
        if coef is None:
            out += [0.0, 0.0, 0.0]
            continue
        low = coef.numerator * q / (coef.denominator * 1e14)
        if pc_regime:
            n_u = 10 ** 6 - p_u
            point = max(kalshi_fee(coef, c_h, p_u, p_u, 1), kalshi_fee(coef, c_h, p_u, n_u, 1))
            high = max(kalshi_fee(coef, c_h, p_u, p_u, 100), kalshi_fee(coef, c_h, p_u, n_u, 100))
        else:
            point = high = kalshi_fee(coef, c_h, p_u, p_u, 100, with_position_cost=False)
        out += [low, point, high]
    if mk == "flat":
        f = flat_maker_fee(c_h)
        out[3:6] = [0.0025 * c_h / 100, f, f]
    return out


def aggregate_report_day(book, day, rows):
    """Every fill on one Kalshi reporting day (Eastern time), per series:
    contracts, fills, Yes-side premium, and taker / maker fees as low, point and high.
    Fee terms are those in force at the fill's create_ts. Newer reports stamp every fill at midnight;
    on a day when a series' terms change, those fills are priced under both terms (low = lower, high =
    higher, point = terms at the day's start)."""
    d0 = et_midnight(day.isoformat())
    d1 = et_midnight((day + dt.timedelta(days=1)).isoformat())
    pc_regime = d0 >= POSITION_COST_ROUNDING_FROM
    rec, examples, cache, other_days = {}, {}, {}, 0
    term_cache = {}
    for ticker, series, date, cts, cnt, price in rows:
        if date != day.isoformat():
            other_days += 1
            continue
        s = series or book.series_of(ticker)
        t = parse_ts(cts)
        timed = t is not None and t != d0
        tk = (s, ticker.startswith("PRES-2024"), t if timed else None)
        terms = term_cache.get(tk)
        if terms is None:
            if timed:
                terms = (book.terms(ticker, t, s),)
            else:
                a, b = book.terms(ticker, d0, s), book.terms(ticker, d1 - 1, s)
                terms = (a,) if a == b else (a, b)
            if len(term_cache) > 200000:
                term_cache.clear()
            term_cache[tk] = terms
        c_h = units(cnt, 2)
        p_u = units(price, 4)                          # cents with up to 4 decimals -> dollars x 1e6
        key = (terms, c_h, p_u)
        f = cache.get(key)
        if f is None:
            fs = [fill_fees(x, c_h, p_u, pc_regime) for x in terms]
            f = fs[0] if len(fs) == 1 else [min(v[0] for v in fs), fs[0][1], max(v[2] for v in fs),
                                              min(v[3] for v in fs), fs[0][4], max(v[5] for v in fs)]
            if len(cache) > 2000000:
                cache.clear()
            cache[key] = f
        row = rec.get(s)
        if row is None:
            rec[s] = row = [0.0] * len(FIELDS)
            if s not in book.series:
                examples[s] = ticker
        row[0] += c_h / 100; row[1] += 1; row[2] += c_h * p_u / 1e8
        for i in range(6):
            row[3 + i] += f[i]
    if not rec:
        raise ValueError(f"trade report {day}: no fills parsed")
    return {"day": day.isoformat(), "schema": SCHEMA, "source": "Kalshi daily trade report",
            "series": {s: [round(x, 6) for x in v] for s, v in rec.items()},
            "unknown_examples": examples, "rows_other_days": other_days}


class InlineExecutor:
    """--workers 0: process report days in this process (debugging, tests)."""

    def submit(self, fn, *args):
        f = Future()
        try:
            f.set_result(fn(*args))
        except Exception as e:
            f.set_exception(e)
        return f

    def shutdown(self, wait=True, cancel_futures=False):
        pass


def process_report_day(book, day, out_path):
    """Download, parse and save one day. Runs in a worker process."""
    t0 = time.time()
    rec = aggregate_report_day(book, day, report_rows(report_chunks(day)))
    with open(out_path + ".tmp", "w") as f:
        json.dump(rec, f, separators=(",", ":"))
    os.replace(out_path + ".tmp", out_path)
    fills = int(sum(v[1] for v in rec["series"].values()))
    return day, fills, time.time() - t0


def category_of(series, native):
    if series.startswith(COMBO_PREFIX):
        return "Combos"
    return NATIVE.get(str(native or "").strip().lower(), "Other")


def resolve_series(api, examples, cache_dir, known):
    """Native category for series missing from GET /series, looked up through Kalshi's own endpoints:
    GET /series/{s}; else the example fill's market -> its event -> the event's category or series.
    Live endpoints first, then historical. Results are cached in series_extra.json; misses are retried."""
    path = os.path.join(cache_dir, "series_extra.json")
    extra = json.load(open(path)) if os.path.exists(path) else {}
    todo = {s: t for s, t in examples.items() if s not in known and not (extra.get(s) or {}).get("category")
            and (extra.get(s) or {}).get("tries", 0) < 3}

    def get(p, key):
        try:
            return api.get(p, max_failures=3, max_429=20).get(key) or {}
        except Exception:
            return {}

    def series_category(st):
        return (known.get(st) or {}).get("category") or get(f"/series/{st}", "series").get("category")

    for s, ticker in sorted(todo.items()):
        found, via = series_category(s), f"/series/{s}"
        if not found:
            for mp in (f"/markets/{ticker}", f"/historical/markets/{ticker}"):
                ev = get(mp, "market").get("event_ticker")
                for ep in ((f"/events/{ev}", f"/historical/events/{ev}") if ev else ()):
                    e = get(ep, "event")
                    found = e.get("category") or (series_category(e["series_ticker"]) if e.get("series_ticker") else None)
                    if found:
                        via = f"{mp} -> {ep}"
                        break
                if found:
                    break
        extra[s] = {"category": found, "via": via if found else None, "example": ticker,
                    "tries": (extra.get(s) or {}).get("tries", 0) + 1}
        log(f"  series {s} ({ticker}): {found or 'NOT RESOLVED'}{' via ' + via if found else ''}")
    if todo:
        with open(path, "w") as f:
            json.dump(extra, f, indent=0, sort_keys=True)
    return {s: v["category"] for s, v in extra.items() if v.get("category")}


METRICS_OUT = METRICS + ["fees_low", "fees_high"]


def day_categories(rec, natives):
    """Per-series day record -> category totals. usd is the Yes-side premium (contracts x Yes price):
    Kalshi's report has no taker side. Fees: fees_low = unrounded, point = per-fill rounding under the
    schedule in force, fees_high = per-fill whole-cent rounding."""
    agg = {c: {m: 0.0 for m in METRICS_OUT} for c in CATS}
    unresolved = {}
    for s, by in rec["series"].items():
        native = natives.get(s)
        if native is None and not s.startswith(COMBO_PREFIX):
            unresolved[s] = 0.0
        row = agg[category_of(s, native)]
        c, n, u, tl, tp, th, ml, mp, mh = by
        row["contracts"] += c; row["usd"] += u; row["trades"] += n
        row["taker_fees"] += tp; row["maker_fees"] += mp
        row["fees_low"] += tl + ml; row["fees_high"] += th + mh
        if s in unresolved:
            unresolved[s] += c
    return {"day": rec["day"], "categories": agg, "unresolved": unresolved}


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

    def __init__(self, session=None, weight_per_min=1000, sleep=time.sleep):
        self.s = session or requests.Session()
        self.budget, self.window, self.sleep = weight_per_min, [], sleep

    def post(self, body, weight=20):
        while True:
            now = time.time()
            self.window = [(t, w) for t, w in self.window if now - t < 60]
            if sum(w for _, w in self.window) + weight <= self.budget:
                break
            time.sleep(1)
        for attempt in range(8):
            try:
                r = self.s.post(HYPERLIQUID, json=body, timeout=60)
            except requests.RequestException:
                self.sleep(min(60, 2 ** attempt))
                continue
            if r.status_code == 429 or r.status_code >= 500:
                self.sleep(min(60, 2 ** attempt))
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
    blocks = {m: {c: [0.0] * len(keys) for c in CATS} for m in METRICS_OUT}
    idx = {k: i for i, k in enumerate(keys)}
    for d in days:
        i = idx.get(keyfn(d["day"]))
        if i is None:
            continue
        for c in CATS:
            for m in METRICS_OUT:
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
           "feeMethod": FEE_METHOD}
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
                        "fees": {c: [round(a + b, 4) for a, b in zip(dd["taker_fees"][c], dd["maker_fees"][c])] for c in dd["taker_fees"]},
                        "feesLow": dd["fees_low"], "feesHigh": dd["fees_high"], "makerFees": dd["maker_fees"],
                        "fills": dd["trades"]}
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
                   monthlyFeesLow=m["fees_low"], monthlyFeesHigh=m["fees_high"], monthlyFills=m["trades"],
                   weeks=weeks, weekly=w["contracts"], weeklyUsd=w["usd"], weeklyFees=fees_w,
                   weeklyFeesLow=w["fees_low"], weeklyFeesHigh=w["fees_high"])
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
                     "fees": fees, "fees_low": out["monthlyFeesLow"]["total"][i], "fees_high": out["monthlyFeesHigh"]["total"][i],
                     "deck_trading_fees": tf, "fees_diff": fees / tf - 1 if tf else None})
    return rows


def main(argv=None, session=None, hl_session=None, today=None, now_ts=None, sleep=time.sleep):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="first event-contract day to (back)fill, YYYY-MM-DD; remembered in the cache")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop starting new work after this many minutes (0 = no limit)")
    ap.add_argument("--end", help="last day, default yesterday (UTC)")
    ap.add_argument("--cache", default="cache")
    ap.add_argument("--rps", type=float, default=4.0, help="Kalshi requests per second, shared by all workers "
                    "(Kalshi's public limit measured ~4.3/s per IP)")
    ap.add_argument("--workers", type=int, default=3, help="trade-report days processed in parallel (processes)")
    ap.add_argument("--refetch", action="store_true", help="recompute days already in the cache")
    ap.add_argument("--skip-trades", action="store_true", help="skip fetching event contracts (still rebuilds output)")
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

    def have(kind, d):
        p = os.path.join(dirs[kind], f"{d.isoformat()}.json")
        if not os.path.exists(p):
            return False
        if kind == "days":                       # day files in an older format are rebuilt
            try:
                with open(p) as f:
                    return json.load(f).get("schema") == SCHEMA
            except ValueError:
                return False
        return True

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

    api = Kalshi(session, a.rps, sleep=sleep)
    errors, progress = [], {"events_days": 0, "perps_days": 0, "hyperliquid_days": 0}
    stopped_early, notional_rule, perps_error = False, None, None
    book = markets = rule = None
    not_published = []
    try:
        book = load_feebook(api, a.cache)
    except Exception as e:
        errors.append(f"fee terms: {e}")
        log(f"FEE TERMS UNAVAILABLE: {e}")
    if not a.skip_perps:
        try:
            markets = perps_markets(api)
            notional_rule = verify_perps_notional(api, markets, now_ts, a.cache)
            rule = notional_rule["rule"]
            hl = Hyperliquid(hl_session, sleep=sleep)
        except Exception as e:                      # never let perps block event contracts
            perps_error = str(e)
            errors.append(f"perps: {e}")
            log(f"PERPS SKIPPED THIS RUN: {perps_error}")

    def do_perps(d):
        rec = aggregate_perps_day(api, d, markets, rule)
        with open(os.path.join(dirs["perps_days"], f"{d.isoformat()}.json"), "w") as f:
            json.dump(rec, f)
        progress["perps_days"] += 1
        log(f"{d} perps: ${sum(v['notional'] for v in rec['classes'].values())/1e9:.2f}B over "
            f"{sum(v['fills'] for v in rec['classes'].values()):,} fills")

    # Event contracts: Kalshi's daily trade reports, newest first, in worker processes. Perps and
    # Hyperliquid go through the APIs in this process meanwhile.
    can_events = not a.skip_trades and book is not None
    pool = (ProcessPoolExecutor(max_workers=a.workers) if a.workers > 0 else InlineExecutor()) if can_events and ev_todo else None
    futures = {pool.submit(process_report_day, book, d, os.path.join(dirs["days"], f"{d.isoformat()}.json")): d
               for d in ev_todo} if pool else {}
    plan = []
    if not a.skip_perps and perps_error is None:
        plan += [("perps", d) for d in pk_todo]
        plan.append(("hyperliquid", hl_todo))
    for kind, arg in plan:
        if perps_error:
            break
        if out_of_time():
            stopped_early = True
            log("Time budget reached; remaining perps days go to the next run.")
            break
        try:
            if kind == "perps":
                do_perps(arg)
            else:
                before = sum(1 for d in arg if have("hl_days", d))
                hl_fill_days(hl, arg, now_ts, dirs["hl_days"], out_of_time)
                progress["hyperliquid_days"] += sum(1 for d in arg if have("hl_days", d)) - before
        except Exception as e:
            perps_error = f"{kind}: {e}"
            errors.append(perps_error)
            log(f"PERPS STOPPED THIS RUN: {perps_error}")
    pending = set(futures)
    while pending:
        if out_of_time():
            stopped_early = True
            for f in pending:
                f.cancel()
            log("Time budget reached; remaining report days go to the next run.")
        done = [f for f in pending if f.done()]
        for f in done:
            pending.discard(f)
            d = futures[f]
            if f.cancelled():
                continue
            try:
                _, fills, secs = f.result()
                progress["events_days"] += 1
                log(f"{d}: {fills:,} fills from Kalshi's trade report in {secs:.0f}s")
            except ReportMissing:
                not_published.append(d.isoformat())
                if d < last - dt.timedelta(days=2):
                    errors.append(f"events {d}: trade report not published")
            except Exception as e:
                errors.append(f"events {d}: {e}")
                log(f"  {d}: failed, retried next run: {e}")
        if pending and not done:
            time.sleep(0.5)
        if stopped_early:
            pending = {f for f in pending if f.running()}
    if pool:
        pool.shutdown(wait=True, cancel_futures=True)
    if notional_rule is None:
        rp = os.path.join(a.cache, "perps_notional_rule.json")
        if os.path.exists(rp):
            notional_rule = {k: v for k, v in json.load(open(rp)).items() if k != "per_market"}

    def read_json(p):
        with open(p) as f:
            return json.load(f)
    load = lambda kind: [read_json(os.path.join(dirs[kind], n)) for n in sorted(os.listdir(dirs[kind])) if n.endswith(".json")]
    raw_days = [r for r in load("days") if r.get("schema") == SCHEMA]
    known = book.series if book else {}
    examples = {}
    for r in raw_days:
        examples.update(r.get("unknown_examples") or {})
    natives = {s: v.get("category") for s, v in known.items()}
    try:
        natives.update(resolve_series(api, examples, a.cache, known))
    except Exception as e:
        errors.append(f"series lookup: {e}")
    days = [day_categories(r, natives) for r in raw_days]
    unresolved = defaultdict(float)
    for d in days:
        for s, c in d["unresolved"].items():
            unresolved[s] += c
    out = build_output(days, load("perps_days"), load("hl_days"), last, first, notional_rule)
    if perps_error:
        out["perpsError"] = perps_error
    missing = {k: sum(1 for d in v if not have(n, d)) for k, v, n in
               (("events", ev_todo, "days"), ("perps", pk_todo, "perps_days"), ("hyperliquid", hl_todo, "hl_days"))}
    if a.skip_trades:
        missing["events"] = sum(1 for d in newest_first(first) if not have("days", d))
    stopped_early = stopped_early or any(missing.values())
    elapsed = time.time() - t_start
    run = {"started": dt.datetime.fromtimestamp(t_start, dt.timezone.utc).isoformat(timespec="seconds"),
           "finished": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
           "minutes": round(elapsed / 60, 1), "rps": a.rps, "workers": a.workers,
           **api.stats, "pages_per_s": round(api.stats["pages"] / elapsed, 2) if elapsed else None,
           "progress": progress, "progress_made": any(progress.values()), "not_yet_published": not_published,
           "remaining_days": missing, "stopped_early": stopped_early,
           "errors": errors[:50], "error_count": len(errors),
           "unresolved_series": dict(sorted(unresolved.items(), key=lambda x: -x[1])[:20])}
    out["run"] = run
    with open(a.out, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    with open(os.path.join(a.cache, "status.json"), "w") as f:
        json.dump({"complete": not any(missing.values()), "missing_days": missing, "first": first.isoformat(),
                   "last": last.isoformat(), "stopped_early": stopped_early, "progress_made": run["progress_made"],
                   "run": run}, f, indent=1)
    log(f"Wrote {a.out}. Still to fetch: {missing}. {api.stats['pages']:,} pages at {run['pages_per_s']}/s, "
        f"{api.stats['http_429']} x 429, {len(errors)} errors.")

    if a.deck:
        log("Tie-out (local only, never written to disk):")
        log(f"  {'month':8} {'volume':>10} {'deck-impl':>10} {'diff':>7}   {'fees':>9} {'[low':>9} {'high]':>9} {'deck':>9} {'diff':>7}")
        for r in tieout(out, json.load(open(a.deck))):
            vd = "n/a" if r["volume_diff"] is None else f"{r['volume_diff']*100:+.1f}%"
            iv = "n/a" if r["deck_implied_volume"] is None else f"{r['deck_implied_volume']/1e9:.2f}B"
            fd = "n/a" if r["fees_diff"] is None else f"{r['fees_diff']*100:+.1f}%"
            log(f"  {r['month']:8} {r['volume']/1e9:9.2f}B {iv:>10} {vd:>7}   {r['fees']/1e6:8.2f}M {r['fees_low']/1e6:8.2f}M "
                f"{r['fees_high']/1e6:8.2f}M {r['deck_trading_fees']/1e6:8.2f}M {fd:>7}")
    return out


if __name__ == "__main__":
    main()
