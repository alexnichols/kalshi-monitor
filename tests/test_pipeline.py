"""Tests for kalshi_pipeline. Run: python -m unittest discover -s tests

Each test encodes a rule from Kalshi's published fee schedule or from how the public API behaves,
so a change that breaks the method fails here rather than silently shifting the numbers."""
import datetime as dt
import json
import os
import sys
import tempfile
import unittest
from fractions import Fraction

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import kalshi_pipeline as kp  # noqa: E402

UTC = dt.timezone.utc


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def iso(t):
    return dt.datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# --------------------------------------------------------------------------- fakes
class Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class FakeKalshi:
    """Serves trades like Kalshi: min_ts inclusive, max_ts exclusive, whole seconds, cursor paging.
    The live endpoint also serves fills from the hour before the historical cutoff (as observed)."""

    def __init__(self, trades, cutoff, page=3, series=None, fail=None, margin=None):
        self.trades = sorted(trades, key=lambda t: t["created_time"])
        self.cutoff, self.page, self.series = cutoff, page, series or {}
        self.fail = fail or {}            # path -> list of things to raise/return before succeeding
        self.margin = margin or {"markets": [], "trades": []}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        path = url.split("/trade-api/v2", 1)[1]
        params = dict(params or {})
        self.calls.append((path, params))
        queue = self.fail.get(path)
        if queue:
            x = queue.pop(0)
            if isinstance(x, Exception):
                raise x
            return Resp(x, {})
        if path == "/series":
            return Resp(200, {"series": [{"ticker": k, **v} for k, v in self.series.items()]})
        if path == "/series/fee_changes":
            return Resp(200, {"series_fee_change_arr": []})
        if path == "/events/fee_changes":
            return Resp(200, {"event_fee_changes": [], "cursor": ""})
        if path == "/historical/cutoff":
            return Resp(200, {"trades_created_ts": iso(self.cutoff)})
        if path in ("/markets/trades", "/historical/trades", "/margin/trades"):
            lo, hi = params.get("min_ts", 0), params.get("max_ts", 1e12)
            src = self.margin["trades"] if path == "/margin/trades" else self.trades
            rows = [t for t in src if lo <= int(kp.parse_ts(t["created_time"])) < hi]
            if path == "/historical/trades":
                rows = [t for t in rows if kp.parse_ts(t["created_time"]) < self.cutoff]
            if path == "/markets/trades":
                rows = [t for t in rows if kp.parse_ts(t["created_time"]) >= self.cutoff - 3600]
            start = int(params.get("cursor") or 0)
            chunk = rows[start:start + self.page]
            nxt = str(start + self.page) if start + self.page < len(rows) else ""
            return Resp(200, {"trades": chunk, "cursor": nxt})
        if path == "/margin/markets":
            return Resp(200, {"markets": self.margin["markets"] if params.get("status") == "active" else []})
        return Resp(404, {})


def fill(t, ticker="KXTEST-1", price="0.5000", count="1.00", side="yes", tid=None):
    p = Fraction(price)
    return {"trade_id": tid or f"{ticker}-{t}-{price}-{count}", "ticker": ticker, "created_time": iso(t),
            "count_fp": count, "yes_price_dollars": price, "no_price_dollars": f"{1 - p:.4f}",
            "taker_outcome_side": side, "taker_side": side}


def book(series=None, changes=None):
    return kp.FeeBook(series or {}, changes or [], [])


# --------------------------------------------------------------------------- fee arithmetic
# Kalshi's General Trading Fees Table (fee schedule effective Jul 7, 2026; identical figures in every
# 2025-26 version): price -> (fee for 1 contract, fee for 100 contracts).
GENERAL_TABLE = {
    "0.01": ("0.01", "0.07"), "0.05": ("0.01", "0.34"), "0.10": ("0.01", "0.63"), "0.15": ("0.01", "0.90"),
    "0.20": ("0.02", "1.12"), "0.25": ("0.02", "1.32"), "0.30": ("0.02", "1.47"), "0.35": ("0.02", "1.60"),
    "0.40": ("0.02", "1.68"), "0.45": ("0.02", "1.74"), "0.50": ("0.02", "1.75"), "0.55": ("0.02", "1.74"),
    "0.60": ("0.02", "1.68"), "0.65": ("0.02", "1.60"), "0.70": ("0.02", "1.47"), "0.75": ("0.02", "1.32"),
    "0.80": ("0.02", "1.12"), "0.85": ("0.01", "0.90"), "0.90": ("0.01", "0.63"), "0.95": ("0.01", "0.34"),
    "0.99": ("0.01", "0.07"),
}
# Specific table for S&P 500 / Nasdaq-100 (0.035) from the same schedules, 100 contracts.
INX_TABLE_100 = {"0.01": "0.04", "0.10": "0.32", "0.25": "0.66", "0.50": "0.88", "0.75": "0.66", "0.90": "0.32", "0.99": "0.04"}


class FeeArithmetic(unittest.TestCase):
    def test_general_table_42_rows_whole_cent(self):
        """The table is printed in whole cents: it matches both the pre-Jul-2026 rule (fee rounded up
        to the cent) and the Jul-2026 rule with a whole-cent unit (fee + positionCost to the cent)."""
        n = 0
        for price, (one, hundred) in GENERAL_TABLE.items():
            p = kp.units(price, 6)
            for c, want in ((1, one), (100, hundred)):
                self.assertAlmostEqual(kp.kalshi_fee(kp.TAKER_COEF, c * 100, p, p, 100, with_position_cost=False), float(want), 9, (price, c))
                self.assertAlmostEqual(kp.kalshi_fee(kp.TAKER_COEF, c * 100, p, p, 100), float(want), 9, (price, c))
                n += 1
        self.assertEqual(n, 42)

    def test_inx_table_half_multiplier(self):
        for price, want in INX_TABLE_100.items():
            p = kp.units(price, 6)
            self.assertAlmostEqual(kp.kalshi_fee(kp.TAKER_COEF * Fraction(1, 2), 10000, p, p, 100, with_position_cost=False), float(want), 9)

    def test_centicent_with_position_cost(self):
        """Jul 2026 rule: fee + positionCost rounded up to a centicent, checked against exact rationals."""
        cases = [("0.01", "1.00"), ("0.50", "1.00"), ("0.9650", "22.50"), ("0.0350", "0.25"), ("0.4321", "7.77")]
        for price, count in cases:
            P, C = Fraction(price), Fraction(count)
            raw = Fraction(7, 100) * C * P * (1 - P)
            pc = C * P
            want = -(-(raw + pc) * 10000 // 1) / Fraction(10000) - pc
            got = kp.kalshi_fee(kp.TAKER_COEF, kp.units(count, 2), kp.units(price, 6), kp.units(price, 6), 1)
            self.assertAlmostEqual(got, float(want), 12, (price, count))
            self.assertGreaterEqual(got + 1e-12, float(raw))
        # One contract at $0.01: 0.000693 raw -> 0.0007 with centicent, 0.01 with whole cents.
        self.assertAlmostEqual(kp.kalshi_fee(kp.TAKER_COEF, 100, 10000, 10000, 1), 0.0007, 12)

    def test_position_cost_uses_the_side_paid(self):
        """positionCost is the price that side paid, so a fractional position cost changes the rounding."""
        y, n = kp.units("0.3333", 6), kp.units("0.6667", 6)
        a = kp.kalshi_fee(kp.TAKER_COEF, 300, y, y, 1)
        b = kp.kalshi_fee(kp.TAKER_COEF, 300, y, n, 1)
        raw = 0.07 * 3 * 0.3333 * 0.6667
        for v in (a, b):
            self.assertGreaterEqual(v, raw)
            self.assertLess(v, raw + 0.0001)

    def test_flat_maker_2025(self):
        """May-Jul 2025 maker fee: round up(0.0025 x C); Kalshi's own example is 5 contracts -> $0.02."""
        self.assertAlmostEqual(kp.flat_maker_fee(500), 0.02, 12)
        self.assertAlmostEqual(kp.flat_maker_fee(400), 0.01, 12)
        self.assertAlmostEqual(kp.flat_maker_fee(10000), 0.25, 12)


# --------------------------------------------------------------------------- fee terms over time
class FeeTerms(unittest.TestCase):
    CHANGES = [
        {"series_ticker": "KXMLBGAME", "scheduled_ts": "2025-10-04T07:00:00Z", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
        {"series_ticker": "KXMLBGAME", "scheduled_ts": "2026-08-07T04:59:45.131Z", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 0.5},
        {"series_ticker": "KXINX", "scheduled_ts": "2026-07-03T17:00:00Z", "fee_type": "quadratic", "fee_multiplier": 1},
        {"series_ticker": "KXTRUMPOUT", "scheduled_ts": "2025-10-21T07:00:00Z", "fee_type": "quadratic", "fee_multiplier": 0},
        {"series_ticker": "KXMVECROSSCATEGORY", "scheduled_ts": "2026-08-20T09:01:00Z", "fee_type": "quadratic_with_combo_maker_fees", "fee_multiplier": 1},
    ]
    SERIES = {
        "KXMLBGAME": {"category": "Sports", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 0.5},
        "KXNBAGAME": {"category": "Sports", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
        "KXINX": {"category": "Financials", "fee_type": "quadratic", "fee_multiplier": 1},
        "KXINXY": {"category": "Financials", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
        "KXATPMATCH": {"category": "Sports", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
        "KXNEWTHING": {"category": "Politics", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
    }

    def setUp(self):
        self.b = book(self.SERIES, self.CHANGES)

    def t(self, ticker, when):
        return self.b.terms(ticker, ts(when))

    def test_mlb_game_follows_the_change_log(self):
        self.assertEqual(self.t("KXMLBGAME-25SEP30NYYBOS", "2025-09-30T20:00:00Z"), (1.0, None, 0.0))   # not on any PDF list
        self.assertEqual(self.t("KXMLBGAME-25OCT05NYYBOS", "2025-10-05T20:00:00Z"), (1.0, "quad", 1.0))
        self.assertEqual(self.t("KXMLBGAME-26AUG08NYYBOS", "2026-08-08T20:00:00Z"), (0.5, "quad", 0.5))

    def test_nba_game_from_published_lists(self):
        """No log entries: the published schedule decides before the log, current terms after."""
        self.assertEqual(self.t("KXNBAGAME-25MAY01", "2025-05-01T20:00:00Z"), (1.0, None, 0.0))       # not on May list
        self.assertEqual(self.t("KXNBAGAME-25JUN20", "2025-06-20T20:00:00Z"), (1.0, "flat", 1.0))     # June 18 list, flat
        self.assertEqual(self.t("KXNBAGAME-25AUG01", "2025-08-01T20:00:00Z"), (1.0, "quad", 1.0))     # July 8: quadratic
        self.assertEqual(self.t("KXNBAGAME-26JAN01", "2026-01-01T20:00:00Z"), (1.0, "quad", 1.0))     # current terms

    def test_tennis_gap(self):
        self.assertEqual(self.t("KXATPMATCH-X", "2025-07-10T12:00:00Z")[1], "quad")
        self.assertIsNone(self.t("KXATPMATCH-X", "2025-08-01T12:00:00Z")[1])        # "until July 12"
        self.assertEqual(self.t("KXATPMATCH-X", "2025-09-03T12:00:00Z")[1], "quad")  # back on Sept 2 list

    def test_inx_half_until_logged_change(self):
        self.assertEqual(self.t("KXINX-25JAN02", "2025-01-02T15:00:00Z")[0], 0.5)
        self.assertEqual(self.t("INXD-25JAN02", "2025-01-02T15:00:00Z")[0], 0.5)
        self.assertEqual(self.t("KXINX-26JUL01", "2026-07-01T15:00:00Z")[0], 0.5)
        self.assertEqual(self.t("KXINX-26JUL06", "2026-07-06T15:00:00Z")[0], 1.0)
        self.assertEqual(self.t("KXINXY-26JAN01", "2026-01-01T15:00:00Z"), (0.5, "quad", 1.0))   # no own entry: until KXINX's switch
        self.assertEqual(self.t("KXINXY-26AUG01", "2026-08-01T15:00:00Z"), (1.0, "quad", 1.0))

    def test_zero_fee_series(self):
        self.assertEqual(self.t("KXTRUMPOUT-X", "2025-10-01T00:00:00Z")[0], 1.0)
        self.assertEqual(self.t("KXTRUMPOUT-X", "2025-11-01T00:00:00Z"), (0.0, None, 0.0))

    def test_combo_maker_is_double(self):
        self.assertEqual(self.t("KXMVECROSSCATEGORY-X", "2026-09-01T00:00:00Z"), (1.0, "quad", 2.0))
        self.assertEqual(self.t("KXMVECROSSCATEGORY-X", "2026-08-01T00:00:00Z"), (1.0, None, 0.0))

    def test_unlogged_series_uses_current_terms_after_log_start(self):
        self.assertEqual(self.t("KXNEWTHING-X", "2026-01-01T00:00:00Z"), (1.0, "quad", 1.0))
        self.assertEqual(self.t("KXNEWTHING-X", "2025-03-01T00:00:00Z"), (1.0, None, 0.0))

    def test_2024_election_free_in_january_2025(self):
        self.assertEqual(self.t("PRES-2024-DJT", "2025-01-10T00:00:00Z"), (0.0, None, 0.0))
        self.assertEqual(self.t("PRES-2024-DJT", "2025-02-10T00:00:00Z")[0], 1.0)

    def test_event_override(self):
        b = kp.FeeBook(self.SERIES, self.CHANGES, [{"event_ticker": "KXNBAGAME-26JAN01LALBOS", "scheduled_ts": "2025-12-01T00:00:00Z",
                                                     "fee_type_override": "quadratic", "fee_multiplier_override": 0}])
        self.assertEqual(b.terms("KXNBAGAME-26JAN01LALBOS-LAL", ts("2026-01-01T00:00:00Z")), (0.0, None, 0.0))
        self.assertEqual(b.terms("KXNBAGAME-26JAN02LALBOS-LAL", ts("2026-01-01T00:00:00Z")), (1.0, "quad", 1.0))

    def test_hyphenated_series(self):
        b = kp.FeeBook({"KXMVECROSSCATEGORY": {}, "KXMVECROSSCATEGORY-SHARD1": {}}, [], [])
        self.assertEqual(b.series_of("KXMVECROSSCATEGORY-SHARD1-S2026ABC-XYZ"), "KXMVECROSSCATEGORY-SHARD1")
        self.assertEqual(b.series_of("KXMVECROSSCATEGORY-S2026ABC-XYZ"), "KXMVECROSSCATEGORY")

# --------------------------------------------------------------------------- Kalshi's daily trade report
def report_new(day, rows):
    """Newer format: pretty-printed, contracts as strings, price in cents as a float, create_ts at midnight ET."""
    body = ",\n".join("{\n    \"ticker_name\": \"%s\",\n    \"report_ticker\": \"%s\",\n    \"date\": \"%s\",\n"
                      "    \"create_ts\": \"%sT00:00:00-05:00\",\n    \"contracts_traded\": \"%s\",\n    \"price\": %s\n}"
                      % (t, t.split("-")[0], d, day, c, p) for t, d, c, p in rows)
    return ("[\n" + body + "\n]\n").encode()


def report_old(rows):
    """Older format: one line, contracts as numbers, real fill times."""
    return json.dumps([{"ticker_name": t, "report_ticker": t.split("-")[0], "date": d, "create_ts": ct,
                        "contracts_traded": c, "price": p} for t, d, ct, c, p in rows]).encode()


def chunked(b, n=37):
    return [b[k:k + n] for k in range(0, len(b), n)]


class TradeReport(unittest.TestCase):
    SERIES = {"KXNBAGAME": {"category": "Sports", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1},
              "KXBTC15M": {"category": "Crypto", "fee_type": "quadratic", "fee_multiplier": 1}}

    def test_parses_both_formats_across_chunk_boundaries(self):
        new = report_new("2026-11-14", [("KXBTC15M-1", "2026-11-14", "14.76", "68.0"), ("KXBTC15M-1", "2026-11-14", "1.00", "99.95")])
        rows = list(kp.report_rows(chunked(new)))
        self.assertEqual([r[4:] for r in rows], [("14.76", "68.0"), ("1.00", "99.95")])
        old = report_old([("KXHIGHLAX-25JAN16-B66.5", "2025-01-15", "2025-01-15T23:59:57-05:00", 11, 41)])
        self.assertEqual(list(kp.report_rows(chunked(old, 11)))[0], ("KXHIGHLAX-25JAN16-B66.5", "KXHIGHLAX", "2025-01-15",
                                                                 "2025-01-15T23:59:57-05:00", "11", "41"))

    def test_unrecognised_format_fails_loudly(self):
        with self.assertRaises(ValueError):
            list(kp.report_rows([b'[{"ticker": "X", "qty": 3}]']))

    def test_exact_fees_before_jul_2026(self):
        """Before Jul 7, 2026 each fill's fee is rounded up to the cent on its own: exact, side-free."""
        b = book(self.SERIES)
        day = dt.date(2026, 1, 10)
        rows = [("KXNBAGAME-26JAN10LALBOS-LAL", "KXNBAGAME", "2026-01-10", "2026-01-10T00:00:00-05:00", "1.00", "1"),
                ("KXNBAGAME-26JAN10LALBOS-LAL", "KXNBAGAME", "2026-01-10", "2026-01-10T00:00:00-05:00", "100.00", "50"),
                ("KXNBAGAME-26JAN09X-Y", "KXNBAGAME", "2026-01-09", "2026-01-09T00:00:00-05:00", "999.00", "50")]   # other day
        rec = kp.aggregate_report_day(b, day, iter(rows))
        c, n, u, tl, tp, th, ml, mp, mh = rec["series"]["KXNBAGAME"]
        self.assertEqual((c, n), (101.0, 2))
        self.assertAlmostEqual(u, 0.01 + 50.0, 9)                     # Yes-side premium
        self.assertAlmostEqual(tp, 0.01 + 1.75, 9)                     # Kalshi's table: $0.01 and $1.75
        self.assertEqual(tp, th)
        self.assertAlmostEqual(tl, 0.07 * 0.0099 + 1.75, 9)
        self.assertAlmostEqual(mp, 0.01 + 0.44, 9)                     # maker 0.0175: $0.0044 -> $0.01, $0.4375 -> $0.44
        self.assertEqual(rec["rows_other_days"], 1)

    def test_bounds_after_jul_2026(self):
        """From Jul 7, 2026: centicent with positionCost. Without the taker side, point is the larger side,
        never more than $0.0001 a fill above the unrounded fee; high is whole-cent rounding."""
        b = book(self.SERIES)
        day = dt.date(2026, 11, 14)
        rows = [("KXBTC15M-1", "KXBTC15M", "2026-11-14", "2026-11-14T00:00:00-05:00", c, p)
                for c, p in (("14.76", "68.0"), ("1.00", "1"), ("0.25", "96.5"), ("7.77", "43.21"))]
        rec = kp.aggregate_report_day(b, day, iter(rows))
        c, n, u, tl, tp, th, ml, mp, mh = rec["series"]["KXBTC15M"]
        self.assertLessEqual(tl, tp)
        self.assertLessEqual(tp, th)
        self.assertLess(tp - tl, 0.0001 * n * 2)
        self.assertEqual((ml, mp, mh), (0.0, 0.0, 0.0))                 # no maker fees on this series

    def test_terms_change_during_untimed_day_spans_both(self):
        changes = [{"series_ticker": "KXNBAGAME", "scheduled_ts": "2026-01-10T18:00:00Z", "fee_type": "quadratic", "fee_multiplier": 0}]
        b = book(self.SERIES, changes)
        rows = [("KXNBAGAME-X-Y", "KXNBAGAME", "2026-01-10", "2026-01-10T00:00:00-05:00", "100.00", "50")]
        c, n, u, tl, tp, th, ml, mp, mh = kp.aggregate_report_day(b, dt.date(2026, 1, 10), iter(rows))["series"]["KXNBAGAME"]
        self.assertEqual(tl, 0.0)                                        # zero-fee terms after the change
        self.assertAlmostEqual(th, 1.75, 9)                             # full terms before it
        self.assertAlmostEqual(tp, 1.75, 9)                             # point: terms at the day's start

    def test_timed_rows_use_their_own_terms(self):
        changes = [{"series_ticker": "KXNBAGAME", "scheduled_ts": "2025-10-10T18:00:00Z", "fee_type": "quadratic", "fee_multiplier": 0}]
        b = book(self.SERIES, changes)
        rows = [("KXNBAGAME-X-Y", "KXNBAGAME", "2025-10-10", "2025-10-10T13:00:00-04:00", "100.00", "50"),
                ("KXNBAGAME-X-Y", "KXNBAGAME", "2025-10-10", "2025-10-10T15:00:00-04:00", "100.00", "50")]
        c, n, u, tl, tp, th, ml, mp, mh = kp.aggregate_report_day(b, dt.date(2025, 10, 10), iter(rows))["series"]["KXNBAGAME"]
        self.assertAlmostEqual(tp, 1.75, 9)                              # only the 1 pm fill pays
        self.assertAlmostEqual(th, 1.75, 9)

    def test_categories_and_unknown_series(self):
        series = {"KXBTC": {"category": "Crypto"}, "KXGOLDD": {"category": "Commodities"}, "KXHIGHNY": {"category": "Climate and Weather"},
                  "KXOSCAR": {"category": "Entertainment"}}
        b = book(series)
        rows = [(t, t.split("-")[0], "2026-11-10", "2026-11-10T00:00:00-05:00", "1.00", "50")
                for t in ("KXBTC-1", "KXGOLDD-1", "KXHIGHNY-1", "KXMVECROSSCATEGORY-1", "KXOSCAR-1", "OLDSERIES-1")]
        rec = kp.aggregate_report_day(b, dt.date(2026, 11, 10), iter(rows))
        self.assertEqual(rec["unknown_examples"], {"OLDSERIES": "OLDSERIES-1", "KXMVECROSSCATEGORY": "KXMVECROSSCATEGORY-1"})
        out = kp.day_categories(rec, {s: v["category"] for s, v in series.items()})
        cats = {c: v["contracts"] for c, v in out["categories"].items() if v["contracts"]}
        self.assertEqual(cats, {"Crypto": 1, "Commodities": 1, "Weather": 1, "Combos": 1, "Other": 2})
        self.assertEqual(set(out["unresolved"]), {"OLDSERIES"})


class FakeS3:
    """Serves trade reports by URL; anything missing is 404. Raises queued errors first."""

    def __init__(self, files, fail=None):
        self.files, self.fail, self.urls = files, list(fail or []), []

    def get(self, url, stream=False, timeout=None):
        self.urls.append(url)
        if self.fail:
            x = self.fail.pop(0)
            if isinstance(x, Exception):
                raise x
            return StreamResp(x, b"")
        name = url.rsplit("/", 1)[1]
        return StreamResp(200, self.files[name]) if name in self.files else StreamResp(404, b"")


class StreamResp:
    def __init__(self, status, body):
        import io
        self.status_code, self.raw = status, io.BytesIO(body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def close(self):
        pass


class ReportDownload(unittest.TestCase):
    def setUp(self):
        kp_sleep = kp.time.sleep
        kp.time.sleep = lambda s: None
        self.addCleanup(setattr, kp.time, "sleep", kp_sleep)

    def test_gz_then_plain_json_then_missing(self):
        import gzip as gz
        body = report_new("2026-11-14", [("KXBTC15M-1", "2026-11-14", "1.00", "50")])
        s3 = FakeS3({"trade_data_2026-11-14.json.gz": gz.compress(body), "trade_data_2025-01-15.json": body})
        self.assertEqual(b"".join(kp.report_chunks(dt.date(2026, 11, 14), s3)), body)
        self.assertEqual(b"".join(kp.report_chunks(dt.date(2025, 1, 15), s3)), body)
        with self.assertRaises(kp.ReportMissing):
            b"".join(kp.report_chunks(dt.date(2026, 11, 16), s3))

    def test_retries_dropped_connections_and_5xx(self):
        body = report_old([("KXA-1", "2025-01-15", "2025-01-15T12:00:00-05:00", 1, 50)])
        s3 = FakeS3({"trade_data_2025-01-15.json": body}, fail=[503, requests.ConnectionError("reset")])
        self.assertEqual(b"".join(kp.report_chunks(dt.date(2025, 1, 15), s3)), body)


class KalshiClient(unittest.TestCase):
    def test_retries_dropped_connections_and_429(self):
        fk = FakeKalshi([], 0, fail={"/series": [requests.ConnectionError("reset"), 429, 503, requests.ReadTimeout("slow")]},
                        series={"KXA": {"category": "Sports"}})
        api = kp.Kalshi(fk, rps=1e6, sleep=lambda s: None)
        self.assertEqual(api.get("/series")["series"][0]["ticker"], "KXA")
        self.assertEqual(api.stats["http_429"], 1)
        self.assertEqual(api.stats["network_errors"], 2)

    def test_gives_up_after_repeated_failures(self):
        fk = FakeKalshi([], 0, fail={"/series": [requests.ConnectionError("down")] * 8})
        api = kp.Kalshi(fk, rps=1e6, sleep=lambda s: None)
        with self.assertRaises(RuntimeError):
            api.get("/series")


# --------------------------------------------------------------------------- perps
class Perps(unittest.TestCase):
    def markets(self, size="0.01"):
        return [{"ticker": "KXBTCPERP", "asset_class": "Crypto", "contract_size": size, "underlying_multiplier": "1",
                 "volume_24h": "100.00", "volume_24h_notional_value_dollars": "950.00"}]

    def trades(self, now):
        return [{"trade_id": "a", "ticker": "KXBTCPERP", "count": "60.00", "price": "9.0000", "created_time": iso(now - 100), "taker_side": "bid"},
                {"trade_id": "b", "ticker": "KXBTCPERP", "count": "40.00", "price": "10.2500", "created_time": iso(now - 50), "taker_side": "ask"}]

    def test_rule_detected_from_kalshi_notional(self):
        now = ts("2026-11-15T00:00:00Z")
        tmp = tempfile.mkdtemp()
        fk = FakeKalshi([], 0, margin={"markets": self.markets(), "trades": self.trades(now)})
        api = kp.Kalshi(fk, rps=1e6, sleep=lambda s: None)
        r = kp.verify_perps_notional(api, kp.perps_markets(api), now, tmp)
        self.assertEqual(r["rule"], "count_x_price")       # 60*9 + 40*10.25 = 950 = Kalshi's 24h notional
        self.assertTrue(os.path.exists(os.path.join(tmp, "perps_notional_rule.json")))

    def test_mismatch_stops_perps_not_events(self):
        now = ts("2026-11-15T00:30:00Z")
        bad = self.markets()
        bad[0]["volume_24h_notional_value_dollars"] = "5000.00"
        fk = FakeKalshi([], 0, margin={"markets": bad, "trades": self.trades(now)})
        body = report_new("2026-11-14", [("KXBTC15M-1", "2026-11-14", "1.00", "50")])
        orig = kp.report_chunks
        kp.report_chunks = lambda day, session=None: iter([body]) if day == dt.date(2026, 11, 14) else (_ for _ in ()).throw(kp.ReportMissing())
        self.addCleanup(setattr, kp, "report_chunks", orig)
        tmp = tempfile.mkdtemp()
        out = kp.main(["--cache", tmp, "--out", os.path.join(tmp, "o.json"), "--end", "2026-11-14", "--start", "2026-11-14",
                       "--rps", "1000000", "--workers", "0"], session=fk, hl_session=None, today=dt.date(2026, 11, 15), now_ts=now,
                      sleep=lambda s: None)
        self.assertIn("Perps notional check failed", out["perpsError"])
        self.assertEqual(out["days"], ["2026-11-14"])
        self.assertEqual(out["daily"]["contracts"]["total"], [1.0])
        self.assertFalse(out["run"]["stopped_early"] and out["run"]["remaining_days"]["events"])
        self.assertTrue(out["run"]["progress_made"])


# --------------------------------------------------------------------------- Hyperliquid
class FakeHL:
    def __init__(self, candles):
        self.candles = candles

    def post(self, url, json=None, timeout=None):
        t = json["type"]
        if t == "meta":
            return Resp(200, {"universe": [{"name": "BTC"}]} if "dex" not in json else {"universe": [{"name": "xyz:TSLA"}]})
        if t == "perpDexs":
            return Resp(200, [None, {"name": "xyz"}])
        if t == "candleSnapshot":
            return Resp(200, self.candles.get(json["req"]["coin"], []))
        return Resp(404, {})


class HyperliquidBounds(unittest.TestCase):
    def test_bounds_and_unavailable(self):
        now = ts("2026-11-15T00:10:00Z")
        d = dt.date(2026, 11, 14)
        t0 = ts("2026-11-14T00:00:00Z") * 1000
        c = lambda t, o, h, l, cl, v: {"t": t, "o": str(o), "h": str(h), "l": str(l), "c": str(cl), "v": str(v), "n": 3}
        candles = {"BTC": [c(t0, 100, 110, 90, 105, 2), c(t0 + 60000, 105, 106, 104, 105, 1)],
                   "xyz:TSLA": [c(t0 + 120000, 10, 12, 8, 11, 5)]}
        hl = kp.Hyperliquid(FakeHL(candles), sleep=lambda s: None)
        self.assertEqual(hl.coins(), ["BTC", "xyz:TSLA"])
        tmp = tempfile.mkdtemp()
        old = dt.date(2025, 1, 1)
        kp.hl_fill_days(hl, [d, old], now, tmp, lambda: False)
        r = json.load(open(os.path.join(tmp, "2026-11-14.json")))
        self.assertEqual(r["method"], "1m")
        self.assertAlmostEqual(r["low"], 2 * 90 + 1 * 104 + 5 * 8)
        self.assertAlmostEqual(r["high"], 2 * 110 + 1 * 106 + 5 * 12)
        self.assertLessEqual(r["low"], r["mid"])
        self.assertLessEqual(r["mid"], r["high"])
        self.assertEqual(r["coins"], 2)
        u = json.load(open(os.path.join(tmp, "2025-01-01.json")))
        self.assertEqual(u["method"], "unavailable")
        self.assertIsNone(u["low"])


# --------------------------------------------------------------------------- output periods
class Output(unittest.TestCase):
    def day(self, iso_day, v):
        cats = {c: {m: 0.0 for m in kp.METRICS_OUT} for c in kp.CATS}
        cats["Sports"].update(contracts=v, usd=v / 2, taker_fees=v / 100, fees_low=v / 120, fees_high=v / 90, trades=1)
        return {"day": iso_day, "categories": cats}

    def test_only_complete_periods(self):
        first, last = dt.date(2026, 9, 25), dt.date(2026, 11, 4)
        days = [self.day((first + dt.timedelta(k)).isoformat(), 1.0) for k in range((last - first).days + 1)]
        days = [d for d in days if d["day"] != "2026-10-20"]        # one gap
        out = kp.build_output(days, [], [], last, first)
        self.assertEqual(out["months"], ["2026-11"])                # Sept starts mid-month, Oct has a gap
        self.assertEqual(out["partialMonths"], {"2026-11": "Nov 1–4"})
        self.assertNotIn("2026-10-19", out["weeks"])                # week with the gap
        self.assertIn("2026-10-26", out["weeks"])
        self.assertNotIn("2026-11-02", out["weeks"])                # not finished
        self.assertEqual(out["monthlyFeesLow"]["total"], [round(4 / 120, 4)])
        self.assertFalse(out["coverage"]["complete"])


if __name__ == "__main__":
    unittest.main()
