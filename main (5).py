# -*- coding: utf-8 -*-
"""
BC + ATF Karşılaştırma Paneli
============================================================
TEK DOSYA / TEK BAŞINA ÇALIŞIR

EKLENENLER
- ATF yöntem raporu çekme (/reports/index?report_date=...)
- Günlük analiz ekranı
- Yöntem bazlı BC komisyon oranı girişi
- ATF rapordan yöntem komisyonu ve gün sonu alma
- Manuel yöntem alanları + açıklama / not alanı
- Eksik yatırım ve eksik çekim için ayrı sayfalar
"""

import os
import re
import json
import time
import html
import base64
import sqlite3
import signal
import logging
import asyncio
import threading
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta, date
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from aiohttp import web

try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None

try:
    import pyotp
except Exception:
    pyotp = None

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bc-atf-panel")


def _req_env(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        raise RuntimeError(f"Environment variable missing: {key}")
    return v


def _opt_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _env_bool(key: str, default: bool) -> bool:
    v = os.environ.get(key, "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


BC_EMAIL = _req_env("BC_EMAIL")
BC_PASSWORD = _req_env("BC_PASSWORD")
BC_TOTP = _req_env("BC_TOTP")
ATF_USERNAME = _req_env("ATF_USERNAME")
ATF_PASSWORD = _req_env("ATF_PASSWORD")

ATF_BASE_URL = _opt_env("ATF_BASE_URL", "https://at.777fin.xyz").rstrip("/")
PORT = int(_opt_env("PORT", "8000"))
TZ_NAME = _opt_env("TZ", "Europe/Istanbul")
TZ = ZoneInfo(TZ_NAME)

RECON_DB_PATH = _opt_env("RECON_DB_PATH", "/data/reconciliation.db")

PANEL_USER = _opt_env("PANEL_USER", "admin")
PANEL_PASS = _opt_env("PANEL_PASS", "admin123")

RECON_ENABLED = _env_bool("RECON_ENABLED", True)
RECON_POLL_SECONDS = int(_opt_env("RECON_POLL_SECONDS", "60"))
RECON_DAYS = int(_opt_env("RECON_DAYS", "2"))
RECON_DEPOSIT_TYPE_ID = int(_opt_env("RECON_DEPOSIT_TYPE_ID", "3"))

TIMEOUT = (10, 60)

ACCOUNTS_HOST = "https://api.accounts-bc.com"
ACCOUNTS_WWW = "https://www.accounts-bc.com"
BACKOFFICE_WEBADMIN = "https://backofficewebadmin.betconstruct.com"
BACKOFFICE_UI = "https://backoffice.betconstruct.com/"
LANG = "tr"
LOGIN_DOMAIN_CANDIDATES = ["accounts-bc.com", "www.accounts-bc.com"]

HDR_BASE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
}

DEFAULT_METHOD_ALIASES = {
    # =========================
    # ANINDA
    # =========================
    "aninda banka": "aninda_banka",
    "aninda havale": "aninda_havale",
    "anindahavale": "aninda_havale",
    "aninda kripto": "aninda_kripto",
    "anindakripto": "aninda_kripto",
    "aninda mefete": "aninda_mefete",
    "aninda paratim": "aninda_mefete",
    "aninda papara": "aninda_papara",
    "aninda papel": "aninda_papel",
    "aninda parola": "aninda_parola",
    "aninda qr": "aninda_qr",
    "anindaqr": "aninda_qr",

    # =========================
    # BIGPAY / BIGPAYS
    # =========================
    "bigpay": "turbo_papara",
    "turbo papara": "turbo_papara",

    "bigpaypaybol": "turbo_papel",
    "turbo papel": "turbo_papel",

    "bigpayss bank": "bigpays_havale",
    "bigpays havale": "bigpays_havale",

    # =========================
    # BROPAY / REDWALLET
    # =========================
    "bropayhavale": "redwallet",
    "bro pay havale": "redwallet",
    "redwallet": "redwallet",

    # =========================
    # HEMEN / VIP
    # =========================
    "hemen": "hemen_havale",
    "hemen havale": "hemen_havale",

    "hemenmefete": "hemen_paratim",
    "hemen paratim": "hemen_paratim",

    "vippapara": "hemen_papara",
    "hemen papara": "hemen_papara",

    "vipparola": "hemen_parolapara",
    "hemen parolapara": "hemen_parolapara",

    # =========================
    # HILLPAYS
    # =========================
    "hillpayscreditcard": "hillpays_kredi_karti",
    "hillpays kredi karti": "hillpays_kredi_karti",

    "hillpayshavale": "hillpays_havale",
    "hillpays havale": "hillpays_havale",

    # =========================
    # DİĞER
    # =========================
    "hizlikripto": "hizli_kripto",
    "hizli kripto": "hizli_kripto",

    "kolayhavale": "kolay_havale",
    "kolay havale": "kolay_havale",

    "lipaynew": "lipay_havale",
    "lipay havale": "lipay_havale",

    "milanohavale": "milano_havale",
    "milano havale": "milano_havale",

    "northhavale": "north_havale",
    "north havale": "north_havale",

    "parampaycard": "param_kredi_karti",
    "param kredi karti": "param_kredi_karti",

    "paytopayzhavale": "paytopayz_havale",
    "paytopayz havale": "paytopayz_havale",

    "payzen": "payzen_kredi_karti",
    "payzen kredi karti": "payzen_kredi_karti",

    "payzenv2transfer": "payzen_havale",
    "payzen havale": "payzen_havale",

    "pluspayhavale": "pluspay_havale",
    "pluspay havale": "pluspay_havale",
    "plus pay havale": "pluspay_havale",

    "tikkopay": "tikkopay_havale",
    "tikkopay havale": "tikkopay_havale",

    "turbohavale": "turbo_havale",
    "turbo havale": "turbo_havale",

    # =========================
    # ULTRAPAY
    # =========================
    "ultrapayv1ahlpay": "ultrapay_ahlpay",
    "ultrapay ahlpay": "ultrapay_ahlpay",

    "ultrapayv1card": "ultrapay_card",
    "ultrapay card": "ultrapay_card",

    "ultrapayv1fups": "ultrapay_fups",
    "ultrapay fups": "ultrapay_fups",

    "ultrapayv1havale": "ultrapay_havale",
    "ultrapay havale": "ultrapay_havale",

    "ultrapayv1haydipay": "ultrapay_haydipay",
    "ultrapay haydipay": "ultrapay_haydipay",

    "ultrapayv1hayhay": "ultrapay_hayhay",
    "ultrapay hayhay": "ultrapay_hayhay",

    "ultrapayv1papel": "ultrapay_papel",
    "ultrapay papel": "ultrapay_papel",

    "ultrapayv1paratim": "ultrapay_paratim",
    "ultrapay paratim": "ultrapay_paratim",

    "ultrapayv1parolapara": "ultrapay_parolapara",
    "ultrapay parolapara": "ultrapay_parolapara",

    "ultrapayv1paycell": "ultrapay_paycell",
    "ultrapay paycell": "ultrapay_paycell",

    "ultrapayv1tosla": "ultrapay_tosla",
    "ultrapay tosla": "ultrapay_tosla",

    # =========================
    # VEVO
    # =========================
    "vevopaycrypto": "vevo_kripto",
    "vevo kripto": "vevo_kripto",

    "vevopayparazula": "vevo_dinamikpay",
    "vevo dinamikpay": "vevo_dinamikpay",

    # =========================
    # WINPAY
    # =========================
    "winfinanshavale": "winpay_havale",
    "winpay havale": "winpay_havale",
}

METHOD_LABELS = {
    "aninda_banka": "Aninda - Banka",
    "aninda_havale": "Aninda - Havale",
    "aninda_kripto": "Aninda - Kripto",
    "aninda_mefete": "Anında - Paratim",
    "aninda_papara": "Aninda - Papara",
    "aninda_papel": "Aninda - Papel",
    "aninda_parola": "Aninda - Parola",
    "aninda_qr": "Aninda - QR",

    "turbo_papara": "Turbo - Papara",
    "turbo_papel": "Turbo - Papel",
    "bigpays_havale": "Bigpays Havale",
    "redwallet": "RedWallet",

    "hemen_havale": "Hemen - Havale",
    "hemen_paratim": "Hemen - Paratim",
    "hemen_papara": "Hemen - Papara",
    "hemen_parolapara": "Hemen - Parolapara",

    "hillpays_kredi_karti": "HillPays - Kredi Kartı",
    "hillpays_havale": "HillPaysHavale",
    "hizli_kripto": "HizliKripto",
    "kolay_havale": "Kolay Havale",
    "lipay_havale": "Lipay Havale",
    "milano_havale": "MilanoHavale",
    "north_havale": "North Havale",
    "param_kredi_karti": "Param Kredi Kartı",
    "paytopayz_havale": "Paytopayz Havale",
    "payzen_kredi_karti": "Payzen - Kredi Kartı",
    "payzen_havale": "Payzen - Havale",
    "pluspay_havale": "PlusPay Havale",
    "tikkopay_havale": "Tikkopay Havale",
    "turbo_havale": "Turbo - Havale",

    "ultrapay_ahlpay": "UltraPay - Ahlpay",
    "ultrapay_card": "UltraPay - Card",
    "ultrapay_fups": "UltraPay - Fups",
    "ultrapay_havale": "UltraPay - Havale",
    "ultrapay_haydipay": "UltraPay - HaydiPay",
    "ultrapay_hayhay": "UltraPay - HayHay",
    "ultrapay_papel": "UltraPay - Papel",
    "ultrapay_paratim": "UltraPay - Paratim",
    "ultrapay_parolapara": "UltraPay - ParolaPara",
    "ultrapay_paycell": "UltraPay - Paycell",
    "ultrapay_tosla": "UltraPay - Tosla",

    "vevo_kripto": "Vevo Kripto",
    "vevo_dinamikpay": "Vevo Dinamikpay",
    "winpay_havale": "Winpay Havale",
}

def now_iso() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def normalize_text_base(v: str | None) -> str:
    s = (v or "").strip().lower()
    s = s.translate(str.maketrans("çğıöşü", "cgiosu"))
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_login(v: str | None) -> str:
    return (v or "").strip().lower()


def norm_money(v) -> float:
    if v is None:
        return 0.0
    s = str(v).strip().replace("₺", "").replace("TRY", "").replace("TL", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def row_to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()}


def parse_bc_created_local(v: str | None) -> tuple[str, str]:
    if not v:
        return "", ""
    txt = str(v).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(txt, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(txt)
        return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%Y-%m-%d")
    except Exception:
        return "", ""


def parse_atf_created_local(v: str | None) -> tuple[str, str]:
    if not v:
        return "", ""
    try:
        dt = datetime.strptime(v.strip(), "%d.%m.%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%Y-%m-%d")
    except Exception:
        return "", ""


def format_date_for_atf(d: date) -> str:
    return d.strftime("%d-%m-%Y")


def norm_type_name(v: str | None) -> str:
    s = normalize_text_base(v)
    if "yat" in s or "deposit" in s or "depoz" in s:
        return "yatirim"
    if "cek" in s or "withdraw" in s:
        return "cekim"
    return s or "diger"


def build_retry_session() -> requests.Session:
    s = requests.Session()
    if Retry is None:
        return s
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=None,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _rand_token(n: int = 6) -> str:
    import uuid
    return uuid.uuid4().hex[:n]


def get_db():
    os.makedirs(os.path.dirname(RECON_DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(RECON_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _add_column_if_missing(cur, table: str, column: str, definition: str):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_recon_db():
    with closing(get_db()) as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mismatch_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_table TEXT NOT NULL,
                result_id INTEGER NOT NULL,
                business_date TEXT,
                match_key TEXT,
                reason_type TEXT DEFAULT '',
                note_text TEXT DEFAULT '',
                status_text TEXT DEFAULT 'Yeni',
                updated_at TEXT,
                UNIQUE(result_table, result_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_name TEXT UNIQUE,
                canonical_key TEXT,
                display_name TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_pair_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bc_raw_name TEXT UNIQUE,
                atf_raw_name TEXT,
                canonical_key TEXT,
                display_name TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bc_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bc_source_id TEXT UNIQUE,
                source_kind TEXT,
                created_at_raw TEXT,
                created_at_norm TEXT,
                client_login_raw TEXT,
                client_login_norm TEXT,
                client_name TEXT,
                amount_raw TEXT,
                amount_norm REAL,
                payment_system_raw TEXT,
                payment_system_norm TEXT,
                payment_system_display TEXT,
                type_name TEXT,
                business_date TEXT,
                fetched_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS atf_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                atf_source_id TEXT UNIQUE,
                created_at_raw TEXT,
                created_at_norm TEXT,
                client_login_raw TEXT,
                client_login_norm TEXT,
                amount_raw TEXT,
                amount_norm REAL,
                commission_raw TEXT,
                commission_norm REAL,
                payment_method_raw TEXT,
                payment_method_norm TEXT,
                payment_method_display TEXT,
                description_raw TEXT,
                type_name TEXT,
                business_date TEXT,
                fetched_at TEXT,
                record_status TEXT DEFAULT 'aktif',
                modified_at TEXT,
                deleted_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bc_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bc_source_id TEXT UNIQUE,
                request_id INTEGER,
                created_at_raw TEXT,
                created_at_norm TEXT,
                request_time_raw TEXT,
                request_time_norm TEXT,
                client_login_raw TEXT,
                client_login_norm TEXT,
                client_name TEXT,
                amount_raw TEXT,
                amount_norm REAL,
                payment_system_raw TEXT,
                payment_system_norm TEXT,
                payment_system_display TEXT,
                state_id INTEGER,
                state_name TEXT,
                business_date TEXT,
                fetched_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS atf_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                atf_source_id TEXT UNIQUE,
                created_at_raw TEXT,
                created_at_norm TEXT,
                client_login_raw TEXT,
                client_login_norm TEXT,
                amount_raw TEXT,
                amount_norm REAL,
                commission_raw TEXT,
                commission_norm REAL,
                payment_method_raw TEXT,
                payment_method_norm TEXT,
                payment_method_display TEXT,
                description_raw TEXT,
                business_date TEXT,
                fetched_at TEXT,
                record_status TEXT DEFAULT 'aktif',
                modified_at TEXT,
                deleted_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT,
                match_key TEXT UNIQUE,
                bc_transaction_id INTEGER,
                atf_transaction_id INTEGER,
                status TEXT,
                mismatch_type TEXT,
                bc_created_at TEXT,
                atf_created_at TEXT,
                bc_login TEXT,
                atf_login TEXT,
                bc_amount REAL,
                atf_amount REAL,
                bc_payment_method TEXT,
                atf_payment_method TEXT,
                type_name TEXT,
                first_detected_at TEXT,
                last_checked_at TEXT,
                panel_status TEXT DEFAULT 'open',
                review_note TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_reconciliation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT,
                match_key TEXT UNIQUE,
                bc_withdrawal_id INTEGER,
                atf_withdrawal_id INTEGER,
                status TEXT,
                mismatch_type TEXT,
                bc_created_at TEXT,
                atf_created_at TEXT,
                bc_login TEXT,
                atf_login TEXT,
                bc_amount REAL,
                atf_amount REAL,
                bc_payment_method TEXT,
                atf_payment_method TEXT,
                type_name TEXT,
                first_detected_at TEXT,
                last_checked_at TEXT,
                panel_status TEXT DEFAULT 'open',
                review_note TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT,
                business_date TEXT,
                started_at TEXT,
                finished_at TEXT,
                total_fetched INTEGER,
                total_inserted INTEGER,
                total_updated INTEGER,
                status TEXT,
                error_text TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_totals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT UNIQUE,
                bc_total REAL DEFAULT 0,
                atf_total REAL DEFAULT 0,
                diff_total REAL DEFAULT 0,
                bc_count INTEGER DEFAULT 0,
                atf_count INTEGER DEFAULT 0,
                last_checked_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_totals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT,
                source_name TEXT,
                flow_type TEXT,
                method_key TEXT,
                method_display TEXT,
                total_amount REAL DEFAULT 0,
                tx_count INTEGER DEFAULT 0,
                UNIQUE(business_date, source_name, flow_type, method_key)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_commission_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                method_key TEXT UNIQUE,
                method_display TEXT,
                bc_commission_rate REAL DEFAULT 0,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS atf_method_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT,
                method_key TEXT,
                method_display TEXT,
                deposit_total REAL DEFAULT 0,
                withdraw_total REAL DEFAULT 0,
                commission_total REAL DEFAULT 0,
                net_total REAL DEFAULT 0,
                raw_last_total REAL DEFAULT 0,
                fetched_at TEXT,
                UNIQUE(business_date, method_key)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_analysis_manual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT,
                method_key TEXT,
                method_display TEXT,
                manual_deposit REAL DEFAULT 0,
                manual_withdraw REAL DEFAULT 0,
                manual_commission REAL DEFAULT 0,
                manual_net REAL DEFAULT 0,
                note_text TEXT DEFAULT '',
                updated_at TEXT,
                UNIQUE(business_date, method_key)
            )
        """)

        _add_column_if_missing(cur, "bc_transactions", "payment_system_display", "TEXT")
        _add_column_if_missing(cur, "atf_transactions", "payment_method_display", "TEXT")
        _add_column_if_missing(cur, "atf_transactions", "record_status", "TEXT DEFAULT 'aktif'")
        _add_column_if_missing(cur, "atf_transactions", "modified_at", "TEXT")
        _add_column_if_missing(cur, "atf_transactions", "deleted_at", "TEXT")
        _add_column_if_missing(cur, "bc_withdrawals", "request_id", "INTEGER")
        _add_column_if_missing(cur, "bc_withdrawals", "request_time_raw", "TEXT")
        _add_column_if_missing(cur, "bc_withdrawals", "request_time_norm", "TEXT")
        _add_column_if_missing(cur, "bc_withdrawals", "payment_system_display", "TEXT")
        _add_column_if_missing(cur, "bc_withdrawals", "state_id", "INTEGER")
        _add_column_if_missing(cur, "bc_withdrawals", "state_name", "TEXT")
        _add_column_if_missing(cur, "atf_withdrawals", "payment_method_display", "TEXT")
        _add_column_if_missing(cur, "atf_withdrawals", "record_status", "TEXT DEFAULT 'aktif'")
        _add_column_if_missing(cur, "atf_withdrawals", "modified_at", "TEXT")
        _add_column_if_missing(cur, "atf_withdrawals", "deleted_at", "TEXT")

        for raw, key in DEFAULT_METHOD_ALIASES.items():
            disp = METHOD_LABELS.get(key, raw)
            cur.execute(
                "INSERT OR IGNORE INTO method_aliases (raw_name, canonical_key, display_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (raw, key, disp, now_iso(), now_iso()),
            )

        conn.commit()


def get_method_alias_map() -> dict:
    data = {}
    with closing(get_db()) as conn:
        rows = conn.execute("SELECT raw_name, canonical_key FROM method_aliases").fetchall()
        for r in rows:
            data[r["raw_name"]] = r["canonical_key"]
        rows2 = conn.execute("SELECT bc_raw_name, atf_raw_name, canonical_key FROM method_pair_map").fetchall()
        for r in rows2:
            if r["bc_raw_name"]:
                data[normalize_text_base(r["bc_raw_name"])] = r["canonical_key"]
            if r["atf_raw_name"]:
                data[normalize_text_base(r["atf_raw_name"])] = r["canonical_key"]
    return data


def get_method_display_by_key(canonical_key: str, fallback: str = "-") -> str:
    with closing(get_db()) as conn:
        row = conn.execute("SELECT display_name FROM method_aliases WHERE canonical_key=? LIMIT 1", (canonical_key,)).fetchone()
        if row and row[0]:
            return row[0]
        row2 = conn.execute("SELECT display_name FROM method_pair_map WHERE canonical_key=? LIMIT 1", (canonical_key,)).fetchone()
        if row2 and row2[0]:
            return row2[0]
    return METHOD_LABELS.get(canonical_key, fallback)


def upsert_method_pair_map(bc_raw_name: str, atf_raw_name: str):
    bc_raw_name = (bc_raw_name or "").strip()
    atf_raw_name = (atf_raw_name or "").strip()
    bc_norm = normalize_text_base(bc_raw_name)
    atf_norm = normalize_text_base(atf_raw_name)
    canonical_key = re.sub(r"\s+", "_", bc_norm.replace(" ", "_") or atf_norm.replace(" ", "_"))
    display_name = atf_raw_name or bc_raw_name or canonical_key
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO method_pair_map (bc_raw_name, atf_raw_name, canonical_key, display_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bc_raw_name) DO UPDATE SET
                atf_raw_name=excluded.atf_raw_name,
                canonical_key=excluded.canonical_key,
                display_name=excluded.display_name,
                updated_at=excluded.updated_at
            """,
            (bc_raw_name, atf_raw_name, canonical_key, display_name, now_iso(), now_iso()),
        )
        cur.execute(
            """
            INSERT INTO method_aliases (raw_name, canonical_key, display_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(raw_name) DO UPDATE SET
                canonical_key=excluded.canonical_key,
                display_name=excluded.display_name,
                updated_at=excluded.updated_at
            """,
            (bc_norm, canonical_key, display_name, now_iso(), now_iso()),
        )
        cur.execute(
            """
            INSERT INTO method_aliases (raw_name, canonical_key, display_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(raw_name) DO UPDATE SET
                canonical_key=excluded.canonical_key,
                display_name=excluded.display_name,
                updated_at=excluded.updated_at
            """,
            (atf_norm, canonical_key, display_name, now_iso(), now_iso()),
        )
        conn.commit()


def normalize_method_key(v: str | None) -> str:
    s = normalize_text_base(v)
    compact = s.replace(" ", "")
    alias_map = get_method_alias_map()
    for cand in (s, compact):
        if cand in alias_map:
            return alias_map[cand]
    return compact


def display_method(v: str | None) -> str:
    key = normalize_method_key(v)
    return get_method_display_by_key(key, (v or "-").strip() or "-")


def insert_sync_run(source_name: str, business_date: str) -> int:
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sync_runs (source_name, business_date, started_at, status) VALUES (?, ?, ?, ?)",
            (source_name, business_date, now_iso(), "running"),
        )
        conn.commit()
        return cur.lastrowid


def finish_sync_run(run_id: int, total_fetched: int, total_inserted: int, total_updated: int, status: str, error_text: str = ""):
    with closing(get_db()) as conn:
        conn.execute(
            "UPDATE sync_runs SET finished_at=?, total_fetched=?, total_inserted=?, total_updated=?, status=?, error_text=? WHERE id=?",
            (now_iso(), total_fetched, total_inserted, total_updated, status, error_text, run_id),
        )
        conn.commit()


def build_authorize_url(response_mode: str = "form_post") -> str:
    params = {
        "client_id": "BackOfficeSSO",
        "response_type": "code token id_token",
        "scope": "openid profile email offline_access introspect.full.access real_ip",
        "redirect_uri": f"{BACKOFFICE_WEBADMIN}/api/en/account/ssocallback",
        "state": f"{BACKOFFICE_UI}?s={_rand_token()}",
        "nonce": f"{BACKOFFICE_WEBADMIN}?n={_rand_token()}",
        "response_mode": response_mode,
    }
    return f"{ACCOUNTS_HOST}/connect/authorize?{urlencode(params)}"


def _signin_prewarm(sess: requests.Session, return_url: str):
    sess.get(
        f"{ACCOUNTS_WWW}/signin",
        params={"returnUrl": return_url},
        headers={**HDR_BASE, "Referer": ACCOUNTS_WWW},
        timeout=TIMEOUT,
    )


def _post_login(sess: requests.Session, email: str, password: str, return_url: str, domain_value: str | None):
    headers = {
        **HDR_BASE,
        "Origin": ACCOUNTS_WWW,
        "Referer": f"{ACCOUNTS_WWW}/signin",
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    payload = {"email": email, "password": password, "returnUrl": return_url}
    if domain_value:
        payload["domain"] = domain_value
    r = sess.post(f"{ACCOUNTS_HOST}/v1/auth/login", headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    try:
        js = r.json()
    except Exception:
        js = {}
    return r, js


def do_login_and_2fa(sess: requests.Session, email: str, password: str, return_url: str, totp_secret: str = ""):
    _signin_prewarm(sess, return_url)
    last_status = None
    last_body = None
    already_logged_in = False
    for dom in LOGIN_DOMAIN_CANDIDATES + [None]:
        r1, data = _post_login(sess, email, password, return_url, dom)
        last_status = r1.status_code
        last_body = data or r1.text
        if last_status == 400 and isinstance(last_body, dict) and last_body.get("Reason") == "InvalidOperation" and "already logged in" in (last_body.get("Message", "").lower()):
            already_logged_in = True
            break
        if last_status == 200:
            break
    if not (last_status == 200 or already_logged_in):
        raise RuntimeError(f"BC login failed: {last_status} | {str(last_body)[:300]}")
    if (last_status == 200) and isinstance(last_body, dict) and last_body.get("requestTwoFactor"):
        if not (totp_secret and pyotp):
            raise RuntimeError("BC 2FA requested but pyotp / BC_TOTP missing.")
        code = pyotp.TOTP(totp_secret).now()
        r2 = sess.post(
            f"{ACCOUNTS_HOST}/v1/twoFaAuth/verifications/codes",
            json={"twoFactorCode": code, "rememberMachine": False},
            headers={**HDR_BASE, "Origin": ACCOUNTS_WWW, "Referer": f"{ACCOUNTS_WWW}/signin"},
            timeout=TIMEOUT,
        )
        if r2.status_code != 200:
            raise RuntimeError(f"BC 2FA failed: {r2.status_code} | {r2.text[:300]}")


class AutoFormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_form = False
        self.form_depth = 0
        self.action = None
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        ad = dict(attrs)
        if tag.lower() == "form" and not self.in_form:
            self.in_form = True
            self.form_depth = 1
            self.action = ad.get("action")
        elif self.in_form and tag.lower() == "form":
            self.form_depth += 1
        if self.in_form and tag.lower() == "input":
            name = ad.get("name")
            if name is not None:
                self.fields[name] = ad.get("value", "")

    def handle_endtag(self, tag):
        if self.in_form and tag.lower() == "form":
            self.form_depth -= 1
            if self.form_depth <= 0:
                self.in_form = False


def _parse_auto_form(html_text: str):
    p = AutoFormParser()
    p.feed(html_text)
    if not p.action:
        return None
    return p.action, p.fields


def check_authentication(sess: requests.Session) -> tuple[bool, str | None]:
    url = f"{BACKOFFICE_WEBADMIN}/api/{LANG}/account/checkauthentication"
    r = sess.get(
        url,
        headers={
            **HDR_BASE,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": BACKOFFICE_UI,
            "Origin": BACKOFFICE_UI.rstrip('/'),
        },
        timeout=TIMEOUT,
    )
    token = r.headers.get("Authentication")
    try:
        js = r.json()
        ok = r.status_code == 200 and isinstance(js, dict) and not js.get("HasError") and js.get("Data", {}).get("AuthenticationStatus", 1) == 0
    except Exception:
        ok = False
    return ok, token


def try_authorize(sess: requests.Session) -> tuple[bool, str | None]:
    for mode in ("form_post", "query"):
        authorize_url = build_authorize_url(response_mode=mode)
        sess.get(
            f"{ACCOUNTS_WWW}/signin",
            params={"returnUrl": authorize_url},
            headers={**HDR_BASE, "Referer": ACCOUNTS_WWW},
            timeout=TIMEOUT,
        )
        r = sess.get(
            authorize_url,
            headers={**HDR_BASE, "Origin": ACCOUNTS_WWW, "Referer": f"{ACCOUNTS_WWW}/signin"},
            allow_redirects=True,
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            parsed = _parse_auto_form(r.text)
            if parsed:
                action, fields = parsed
                sess.post(
                    urljoin(authorize_url, action),
                    data=fields,
                    headers={**HDR_BASE, "Origin": BACKOFFICE_WEBADMIN, "Referer": authorize_url},
                    allow_redirects=True,
                    timeout=TIMEOUT,
                )
        ok, token = check_authentication(sess)
        if ok and token:
            return ok, token
    return False, None


def _bo_headers(token: str) -> dict:
    return {
        **HDR_BASE,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": BACKOFFICE_UI,
        "Origin": BACKOFFICE_UI.rstrip('/'),
        "Authentication": token,
        "Content-Type": "application/json;charset=UTF-8",
    }


class BCClient:
    def __init__(self):
        self.sess = build_retry_session()
        self.token = ""

    def ensure_token(self):
        ok, t = check_authentication(self.sess)
        if ok and t:
            self.token = t
            return
        ok2, t2 = try_authorize(self.sess)
        if ok2 and t2:
            self.token = t2
            return
        ret_url = build_authorize_url("form_post")
        do_login_and_2fa(self.sess, BC_EMAIL, BC_PASSWORD, ret_url, BC_TOTP)
        ok3, t3 = try_authorize(self.sess)
        if ok3 and t3:
            self.token = t3
            return
        raise RuntimeError("BC token alınamadı.")

    def get_documents_with_paging(self, from_created_local: str, to_created_local: str, type_id: int = 3, max_rows: int = 1000, skeep_rows: int = 0) -> dict:
        url = f"{BACKOFFICE_WEBADMIN}/api/{LANG}/Financial/GetDocumentsWithPaging"
        body = {
            "FromCreatedDateLocal": from_created_local,
            "ToCreatedDateLocal": to_created_local,
            "ClientId": "",
            "ClientLogin": "",
            "Id": "",
            "ExternalId": "",
            "AmountFrom": "",
            "AmountTo": "",
            "CashDeskId": None,
            "CurrencyId": "",
            "DefaultCurrencyId": "ADA",
            "IsTest": None,
            "PaymentSystemId": None,
            "SkeepRows": skeep_rows,
            "MaxRows": max_rows,
            "TypeId": type_id,
            "UserName": "",
        }
        r = self.sess.post(url, headers=_bo_headers(self.token), data=json.dumps(body), timeout=TIMEOUT)
        if r.status_code == 200:
            try:
                js = r.json()
            except Exception:
                js = {}
            if isinstance(js, dict) and not js.get("HasError"):
                return js.get("Data") or {"Count": 0, "Objects": []}
        return {"Count": 0, "Objects": []}

    def get_withdrawals_with_totals(self, from_created_local: str, to_created_local: str, max_rows: int = 1000, skeep_rows: int = 0) -> dict:
        url = f"{BACKOFFICE_WEBADMIN}/api/{LANG}/Client/GetClientWithdrawalRequestsWithTotals"
        body = {
            "BetShopId": "",
            "ByAllowDate": False,
            "ClientId": "",
            "ClientLogin": "",
            "CurrencyId": None,
            "Email": "",
            "FromDateLocal": from_created_local,
            "Id": None,
            "IsTest": None,
            "MaxAmount": None,
            "MinAmount": None,
            "PartnerClientCategoryId": "",
            "PaymentTypeIds": [],
            "RegionId": None,
            "StateList": [],
            "ToDateLocal": to_created_local,
            "SkeepRows": skeep_rows,
            "MaxRows": max_rows,
        }
        r = self.sess.post(url, headers=_bo_headers(self.token), data=json.dumps(body), timeout=TIMEOUT)
        if r.status_code == 200:
            try:
                js = r.json()
            except Exception:
                js = {}
            if isinstance(js, dict) and not js.get("HasError"):
                raw_data = js.get("Data") or {}
                client_requests = raw_data.get("ClientRequests") or []
                return {
                    "Count": raw_data.get("Count", len(client_requests)),
                    "TotalAmount": raw_data.get("TotalAmount", 0),
                    "Objects": client_requests,
                }
        return {"Count": 0, "TotalAmount": 0, "Objects": []}

    def fetch_deposits_for_day(self, day: date) -> list[dict]:
        from_s = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=TZ).strftime("%d-%m-%y - %H:%M:%S")
        to_s = (datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=TZ) + timedelta(days=1)).strftime("%d-%m-%y - %H:%M:%S")
        out = []
        skip = 0
        page = 1000
        while True:
            data = self.get_documents_with_paging(from_s, to_s, type_id=RECON_DEPOSIT_TYPE_ID, max_rows=page, skeep_rows=skip)
            objs = data.get("Objects") or []
            out.extend(objs)
            if len(objs) < page:
                break
            skip += page
        return out

    def fetch_withdrawals_for_day(self, day: date) -> list[dict]:
        from_s = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=TZ).strftime("%d-%m-%y - %H:%M:%S")
        to_s = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=TZ).strftime("%d-%m-%y - %H:%M:%S")
        out = []
        skip = 0
        page = 1000
        while True:
            data = self.get_withdrawals_with_totals(from_s, to_s, max_rows=page, skeep_rows=skip)
            objs = data.get("Objects") or []
            out.extend(objs)
            if len(objs) < page:
                break
            skip += page
        return out


class ATFTransactionTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_tr = False
        self.in_td = False
        self.current_td = []
        self.current_row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        ad = dict(attrs)
        if tag == "tr" and "transaction" in ad.get("class", ""):
            self.in_tr = True
            self.current_row = []
        elif self.in_tr and tag == "td":
            self.in_td = True
            self.current_td = []

    def handle_data(self, data):
        if self.in_tr and self.in_td:
            self.current_td.append(data)

    def handle_endtag(self, tag):
        if self.in_tr and tag == "td":
            self.in_td = False
            self.current_row.append(" ".join(" ".join(self.current_td).split()))
        elif self.in_tr and tag == "tr":
            self.in_tr = False
            if self.current_row:
                self.rows.append(self.current_row)


class ATFReportTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_tr = False
        self.in_td = False
        self.current_td = []
        self.current_row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "tr":
            self.in_tr = True
            self.current_row = []
        elif self.in_tr and tag.lower() == "td":
            self.in_td = True
            self.current_td = []

    def handle_data(self, data):
        if self.in_tr and self.in_td:
            self.current_td.append(data)

    def handle_endtag(self, tag):
        if self.in_tr and tag.lower() == "td":
            self.in_td = False
            txt = " ".join(" ".join(self.current_td).split()).strip()
            self.current_row.append(txt)
        elif self.in_tr and tag.lower() == "tr":
            self.in_tr = False
            if self.current_row:
                self.rows.append(self.current_row)


def parse_atf_description(desc: str) -> tuple[str, str]:
    parts = [x.strip() for x in (desc or "").split(" - ")]
    login_raw = parts[1] if len(parts) >= 2 else ""
    short_method = parts[2] if len(parts) >= 3 else ""
    return login_raw, short_method


def parse_atf_rows(html_text: str) -> list[dict]:
    parser = ATFTransactionTableParser()
    parser.feed(html_text)
    out = []
    for row in parser.rows:
        if len(row) < 8:
            continue
        source_id = row[0]
        created_raw = row[1]
        type_name = row[2]
        payment_method = row[4]
        description = row[5]
        commission_raw = row[6]
        amount_raw = row[7]
        login_raw, short_method = parse_atf_description(description)
        created_norm, business_date = parse_atf_created_local(created_raw)
        out.append({
            "atf_source_id": source_id,
            "created_at_raw": created_raw,
            "created_at_norm": created_norm,
            "business_date": business_date,
            "type_name": type_name,
            "norm_type": norm_type_name(type_name),
            "payment_method_raw": payment_method,
            "payment_method_norm": normalize_method_key(payment_method or short_method),
            "payment_method_display": display_method(payment_method or short_method),
            "description_raw": description,
            "commission_raw": commission_raw,
            "commission_norm": norm_money(commission_raw),
            "amount_raw": amount_raw,
            "amount_norm": norm_money(amount_raw),
            "client_login_raw": login_raw,
            "client_login_norm": norm_login(login_raw),
        })
    return out


def parse_atf_method_report_rows(html_text: str) -> list[dict]:
    parser = ATFReportTableParser()
    parser.feed(html_text)
    out = []
    for row in parser.rows:
        if not row or len(row) < 7:
            continue
        method_raw = (row[0] or "").strip()
        if not method_raw:
            continue
        if normalize_text_base(method_raw) in ("toplam", "total"):
            continue
        nums = [norm_money(x) for x in row[1:]]
        if len(nums) < 6:
            continue
        deposit_total = float(nums[0] or 0)
        withdraw_total = float(nums[1] or 0)
        commission_total = float(nums[5] or 0)
        net_total = float(nums[-1] or 0)
        raw_last_total = float(nums[-1] or 0)
        out.append({
            "method_display": method_raw,
            "method_key": normalize_method_key(method_raw),
            "deposit_total": deposit_total,
            "withdraw_total": withdraw_total,
            "commission_total": commission_total,
            "net_total": net_total,
            "raw_last_total": raw_last_total,
        })
    return out


class ATFClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.sess = build_retry_session()
        self.sess.headers.update({
            "User-Agent": HDR_BASE["User-Agent"],
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
        })

    def login(self):
        self.sess.get(f"{self.base_url}/login", timeout=TIMEOUT)
        r = self.sess.post(
            f"{self.base_url}/home/ajax/login",
            data={"username": self.username, "password": self.password},
            headers={
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/login",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            raise RuntimeError(f"ATF login failed: HTTP {r.status_code}")
        if "ci_session" not in self.sess.cookies.get_dict():
            raise RuntimeError(f"ATF login failed: session cookie missing | {r.text[:200]}")

    def search_transactions(self, query: str, start_date: str, end_date: str, page: int = 1) -> str:
        params = {"query": query, "startDate": start_date, "endDate": end_date, "page": page}
        url = f"{self.base_url}/transactions/index?{urlencode(params)}"
        r = self.sess.get(url, headers={"Referer": url, "X-Requested-With": "XMLHttpRequest"}, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"ATF search failed: HTTP {r.status_code}")
        return r.text

    def get_method_report_html(self, day: date) -> str:
        report_date = day.strftime("%Y-%m-%d")
        url = f"{self.base_url}/reports/index?report_date={report_date}"
        r = self.sess.get(url, headers={"Referer": url, "X-Requested-With": "XMLHttpRequest"}, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"ATF report failed: HTTP {r.status_code}")
        return r.text

    def fetch_method_report_for_day(self, day: date) -> list[dict]:
        html_text = self.get_method_report_html(day)
        return parse_atf_method_report_rows(html_text)

    def fetch_all_transactions_for_day(self, day: date) -> list[dict]:
        start_date = format_date_for_atf(day)
        end_date = format_date_for_atf(day)
        all_rows = []
        seen_ids = set()
        empty_streak = 0
        for page in range(1, 1000):
            html_text = self.search_transactions("", start_date, end_date, page=page)
            rows = parse_atf_rows(html_text)
            if not rows:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            empty_streak = 0
            new_count = 0
            for r in rows:
                sid = str(r.get("atf_source_id") or "")
                if not sid or sid in seen_ids:
                    continue
                seen_ids.add(sid)
                all_rows.append(r)
                new_count += 1
            if new_count == 0:
                break
            if len(rows) < 20:
                break
            time.sleep(0.10)
        return all_rows


def upsert_bc_deposits(rows: list[dict]) -> int:
    inserted = 0
    with closing(get_db()) as conn:
        cur = conn.cursor()
        for o in rows:
            source_id = f"dep:{o.get('Id')}"
            created_raw = o.get("CreatedLocal") or ""
            created_norm, business_date = parse_bc_created_local(created_raw)
            login_raw = o.get("ClientLogin") or ""
            client_name = o.get("ClientName") or ""
            amount_val = o.get("Amount")
            payment_raw = o.get("PaymentSystemName") or ""
            cur.execute(
                """
                INSERT OR IGNORE INTO bc_transactions
                (bc_source_id, source_kind, created_at_raw, created_at_norm, client_login_raw, client_login_norm,
                 client_name, amount_raw, amount_norm, payment_system_raw, payment_system_norm, payment_system_display,
                 type_name, business_date, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id, "deposit", created_raw, created_norm, login_raw, norm_login(login_raw), client_name,
                    str(amount_val or ""), norm_money(amount_val), payment_raw, normalize_method_key(payment_raw),
                    display_method(payment_raw), "Yatırım", business_date, now_iso(),
                ),
            )
            if cur.rowcount:
                inserted += 1
        conn.commit()
    return inserted


def upsert_bc_withdrawals(rows: list[dict]) -> tuple[int, int]:
    inserted = 0
    paid_count = 0
    with closing(get_db()) as conn:
        cur = conn.cursor()
        for o in rows:
            state_id = int(o.get("State") or 0)
            if state_id != 3:
                continue
            paid_count += 1
            source_id = f"wdr:{o.get('Id')}"
            created_raw = o.get("PaymentCreatedLocal") or ""
            created_norm, business_date = parse_bc_created_local(created_raw)
            request_raw = o.get("RequestTimeLocal") or ""
            request_norm, _ = parse_bc_created_local(request_raw)
            login_raw = o.get("ClientLogin") or ""
            client_name = o.get("ClientName") or ""
            amount_val = o.get("Amount")
            payment_raw = o.get("PaymentSystemName") or o.get("Notes") or ""
            cur.execute(
                """
                INSERT INTO bc_withdrawals
                (bc_source_id, request_id, created_at_raw, created_at_norm, request_time_raw, request_time_norm,
                 client_login_raw, client_login_norm, client_name, amount_raw, amount_norm,
                 payment_system_raw, payment_system_norm, payment_system_display,
                 state_id, state_name, business_date, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bc_source_id) DO UPDATE SET
                    request_id=excluded.request_id,
                    created_at_raw=excluded.created_at_raw,
                    created_at_norm=excluded.created_at_norm,
                    request_time_raw=excluded.request_time_raw,
                    request_time_norm=excluded.request_time_norm,
                    client_login_raw=excluded.client_login_raw,
                    client_login_norm=excluded.client_login_norm,
                    client_name=excluded.client_name,
                    amount_raw=excluded.amount_raw,
                    amount_norm=excluded.amount_norm,
                    payment_system_raw=excluded.payment_system_raw,
                    payment_system_norm=excluded.payment_system_norm,
                    payment_system_display=excluded.payment_system_display,
                    state_id=excluded.state_id,
                    state_name=excluded.state_name,
                    business_date=excluded.business_date,
                    fetched_at=excluded.fetched_at
                """,
                (
                    source_id, int(o.get("Id") or 0), created_raw, created_norm, request_raw, request_norm,
                    login_raw, norm_login(login_raw), client_name, str(amount_val or ""), norm_money(amount_val),
                    payment_raw, normalize_method_key(payment_raw), display_method(payment_raw), state_id,
                    o.get("StateName") or "", business_date, now_iso(),
                ),
            )
            inserted += 1
        conn.commit()
    return inserted, paid_count


def upsert_atf_deposits(rows: list[dict], business_date: str) -> int:
    inserted = 0
    with closing(get_db()) as conn:
        cur = conn.cursor()
        current_ids = set()
        for o in rows:
            if o.get("norm_type") != "yatirim":
                continue
            source_id = str(o.get("atf_source_id") or "")
            if not source_id:
                continue
            current_ids.add(source_id)
            existing = cur.execute(
                "SELECT id, record_status, amount_norm, commission_norm, payment_method_raw, description_raw FROM atf_transactions WHERE atf_source_id=?",
                (source_id,),
            ).fetchone()
            if existing:
                changed = (
                    float(existing["amount_norm"] or 0) != float(o.get("amount_norm") or 0)
                    or float(existing["commission_norm"] or 0) != float(o.get("commission_norm") or 0)
                    or (existing["payment_method_raw"] or "") != (o.get("payment_method_raw") or "")
                    or (existing["description_raw"] or "") != (o.get("description_raw") or "")
                )
                if changed:
                    cur.execute(
                        """
                        UPDATE atf_transactions
                        SET created_at_raw=?, created_at_norm=?, client_login_raw=?, client_login_norm=?, amount_raw=?,
                            amount_norm=?, commission_raw=?, commission_norm=?, payment_method_raw=?, payment_method_norm=?,
                            payment_method_display=?, description_raw=?, type_name=?, business_date=?, fetched_at=?,
                            record_status='duzenlendi', modified_at=?
                        WHERE atf_source_id=?
                        """,
                        (
                            o.get("created_at_raw") or "", o.get("created_at_norm") or "", o.get("client_login_raw") or "",
                            o.get("client_login_norm") or "", o.get("amount_raw") or "", float(o.get("amount_norm") or 0),
                            o.get("commission_raw") or "", float(o.get("commission_norm") or 0), o.get("payment_method_raw") or "",
                            o.get("payment_method_norm") or "", o.get("payment_method_display") or "", o.get("description_raw") or "",
                            "Yatırım", o.get("business_date") or business_date, now_iso(), now_iso(), source_id,
                        ),
                    )
                else:
                    cur.execute(
                        "UPDATE atf_transactions SET fetched_at=?, record_status=CASE WHEN record_status='silindi' THEN 'aktif' ELSE record_status END, deleted_at=CASE WHEN record_status='silindi' THEN NULL ELSE deleted_at END WHERE atf_source_id=?",
                        (now_iso(), source_id),
                    )
            else:
                cur.execute(
                    """
                    INSERT INTO atf_transactions
                    (atf_source_id, created_at_raw, created_at_norm, client_login_raw, client_login_norm,
                     amount_raw, amount_norm, commission_raw, commission_norm, payment_method_raw, payment_method_norm,
                     payment_method_display, description_raw, type_name, business_date, fetched_at, record_status, modified_at, deleted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'aktif', NULL, NULL)
                    """,
                    (
                        source_id, o.get("created_at_raw") or "", o.get("created_at_norm") or "", o.get("client_login_raw") or "",
                        o.get("client_login_norm") or "", o.get("amount_raw") or "", float(o.get("amount_norm") or 0),
                        o.get("commission_raw") or "", float(o.get("commission_norm") or 0), o.get("payment_method_raw") or "",
                        o.get("payment_method_norm") or "", o.get("payment_method_display") or "", o.get("description_raw") or "",
                        "Yatırım", o.get("business_date") or business_date, now_iso(),
                    ),
                )
                inserted += 1
        if current_ids:
            placeholders = ",".join(["?"] * len(current_ids))
            cur.execute(
                f"UPDATE atf_transactions SET record_status='silindi', deleted_at=?, fetched_at=? WHERE business_date=? AND record_status!='silindi' AND atf_source_id NOT IN ({placeholders})",
                (now_iso(), now_iso(), business_date, *current_ids),
            )
        conn.commit()
    return inserted


def upsert_atf_withdrawals(rows: list[dict], business_date: str) -> int:
    inserted = 0
    with closing(get_db()) as conn:
        cur = conn.cursor()
        current_ids = set()
        for o in rows:
            if o.get("norm_type") != "cekim":
                continue
            source_id = str(o.get("atf_source_id") or "")
            if not source_id:
                continue
            current_ids.add(source_id)
            existing = cur.execute(
                "SELECT id, record_status, amount_norm, commission_norm, payment_method_raw, description_raw FROM atf_withdrawals WHERE atf_source_id=?",
                (source_id,),
            ).fetchone()
            if existing:
                changed = (
                    float(existing["amount_norm"] or 0) != float(o.get("amount_norm") or 0)
                    or float(existing["commission_norm"] or 0) != float(o.get("commission_norm") or 0)
                    or (existing["payment_method_raw"] or "") != (o.get("payment_method_raw") or "")
                    or (existing["description_raw"] or "") != (o.get("description_raw") or "")
                )
                if changed:
                    cur.execute(
                        """
                        UPDATE atf_withdrawals
                        SET created_at_raw=?, created_at_norm=?, client_login_raw=?, client_login_norm=?, amount_raw=?,
                            amount_norm=?, commission_raw=?, commission_norm=?, payment_method_raw=?, payment_method_norm=?,
                            payment_method_display=?, description_raw=?, business_date=?, fetched_at=?,
                            record_status='duzenlendi', modified_at=?
                        WHERE atf_source_id=?
                        """,
                        (
                            o.get("created_at_raw") or "", o.get("created_at_norm") or "", o.get("client_login_raw") or "",
                            o.get("client_login_norm") or "", o.get("amount_raw") or "", float(o.get("amount_norm") or 0),
                            o.get("commission_raw") or "", float(o.get("commission_norm") or 0), o.get("payment_method_raw") or "",
                            o.get("payment_method_norm") or "", o.get("payment_method_display") or "", o.get("description_raw") or "",
                            o.get("business_date") or business_date, now_iso(), now_iso(), source_id,
                        ),
                    )
                else:
                    cur.execute(
                        "UPDATE atf_withdrawals SET fetched_at=?, record_status=CASE WHEN record_status='silindi' THEN 'aktif' ELSE record_status END, deleted_at=CASE WHEN record_status='silindi' THEN NULL ELSE deleted_at END WHERE atf_source_id=?",
                        (now_iso(), source_id),
                    )
            else:
                cur.execute(
                    """
                    INSERT INTO atf_withdrawals
                    (atf_source_id, created_at_raw, created_at_norm, client_login_raw, client_login_norm,
                     amount_raw, amount_norm, commission_raw, commission_norm, payment_method_raw, payment_method_norm,
                     payment_method_display, description_raw, business_date, fetched_at, record_status, modified_at, deleted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'aktif', NULL, NULL)
                    """,
                    (
                        source_id, o.get("created_at_raw") or "", o.get("created_at_norm") or "", o.get("client_login_raw") or "",
                        o.get("client_login_norm") or "", o.get("amount_raw") or "", float(o.get("amount_norm") or 0),
                        o.get("commission_raw") or "", float(o.get("commission_norm") or 0), o.get("payment_method_raw") or "",
                        o.get("payment_method_norm") or "", o.get("payment_method_display") or "", o.get("description_raw") or "",
                        o.get("business_date") or business_date, now_iso(),
                    ),
                )
                inserted += 1
        if current_ids:
            placeholders = ",".join(["?"] * len(current_ids))
            cur.execute(
                f"UPDATE atf_withdrawals SET record_status='silindi', deleted_at=?, fetched_at=? WHERE business_date=? AND record_status!='silindi' AND atf_source_id NOT IN ({placeholders})",
                (now_iso(), now_iso(), business_date, *current_ids),
            )
        conn.commit()
    return inserted


def aggregate_method_totals_from_rows(rows: list[dict], side: str) -> dict:
    out = {}
    for r in rows:
        if side == "BC":
            method_key = r.get("payment_system_norm") or normalize_method_key(r.get("payment_system_display"))
            method_display = r.get("payment_system_display") or "-"
        else:
            method_key = r.get("payment_method_norm") or normalize_method_key(r.get("payment_method_display"))
            method_display = r.get("payment_method_display") or "-"
        out.setdefault(method_key, {"display": method_display, "total": 0.0, "count": 0})
        out[method_key]["total"] += float(r.get("amount_norm") or 0)
        out[method_key]["count"] += 1
    return out


def upsert_method_totals(business_date: str, source_name: str, flow_type: str, totals: dict):
    with closing(get_db()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM method_totals WHERE business_date=? AND source_name=? AND flow_type=?", (business_date, source_name, flow_type))
        for method_key, info in totals.items():
            cur.execute(
                "INSERT OR REPLACE INTO method_totals (business_date, source_name, flow_type, method_key, method_display, total_amount, tx_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (business_date, source_name, flow_type, method_key, info["display"], float(info["total"]), int(info["count"])),
            )
        conn.commit()


def upsert_withdrawal_total(business_date: str, bc_total: float, atf_total: float, bc_count: int, atf_count: int):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO withdrawal_totals (business_date, bc_total, atf_total, diff_total, bc_count, atf_count, last_checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date) DO UPDATE SET
                bc_total=excluded.bc_total,
                atf_total=excluded.atf_total,
                diff_total=excluded.diff_total,
                bc_count=excluded.bc_count,
                atf_count=excluded.atf_count,
                last_checked_at=excluded.last_checked_at
            """,
            (business_date, bc_total, atf_total, bc_total - atf_total, bc_count, atf_count, now_iso()),
        )
        conn.commit()


def upsert_atf_method_report_rows(business_date: str, rows: list[dict]) -> int:
    inserted = 0
    with closing(get_db()) as conn:
        cur = conn.cursor()
        for r in rows:
            cur.execute(
                """
                INSERT INTO atf_method_reports
                (business_date, method_key, method_display, deposit_total, withdraw_total,
                 commission_total, net_total, raw_last_total, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(business_date, method_key) DO UPDATE SET
                    method_display=excluded.method_display,
                    deposit_total=excluded.deposit_total,
                    withdraw_total=excluded.withdraw_total,
                    commission_total=excluded.commission_total,
                    net_total=excluded.net_total,
                    raw_last_total=excluded.raw_last_total,
                    fetched_at=excluded.fetched_at
                """,
                (
                    business_date,
                    r["method_key"],
                    r["method_display"],
                    float(r["deposit_total"] or 0),
                    float(r["withdraw_total"] or 0),
                    float(r["commission_total"] or 0),
                    float(r["net_total"] or 0),
                    float(r["raw_last_total"] or 0),
                    now_iso(),
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


def upsert_method_commission_rate(method_key: str, method_display: str, rate: float):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO method_commission_rates (method_key, method_display, bc_commission_rate, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(method_key) DO UPDATE SET
                method_display=excluded.method_display,
                bc_commission_rate=excluded.bc_commission_rate,
                updated_at=excluded.updated_at
            """,
            (method_key, method_display, float(rate or 0), now_iso()),
        )
        conn.commit()


def upsert_method_analysis_manual(business_date: str, method_key: str, method_display: str, manual_deposit: float, manual_withdraw: float, manual_commission: float, manual_net: float, note_text: str):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO method_analysis_manual
            (business_date, method_key, method_display, manual_deposit, manual_withdraw,
             manual_commission, manual_net, note_text, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date, method_key) DO UPDATE SET
                method_display=excluded.method_display,
                manual_deposit=excluded.manual_deposit,
                manual_withdraw=excluded.manual_withdraw,
                manual_commission=excluded.manual_commission,
                manual_net=excluded.manual_net,
                note_text=excluded.note_text,
                updated_at=excluded.updated_at
            """,
            (
                business_date, method_key, method_display, float(manual_deposit or 0), float(manual_withdraw or 0),
                float(manual_commission or 0), float(manual_net or 0), note_text or "", now_iso(),
            ),
        )
        conn.commit()


def make_match_key(login_norm: str, created_norm: str, amount_norm: float, method_norm: str) -> str:
    return f"{login_norm}|{created_norm}|{amount_norm:.2f}|{method_norm}"


def close_old_open_mismatches(cur, table_name: str, business_date: str, login: str, amount: float, method: str, created_at: str):
    cur.execute(
        f"""
        UPDATE {table_name}
        SET panel_status='closed', review_note='Sonraki senkronda eşleşti', last_checked_at=?
        WHERE business_date=? AND panel_status='open' AND status!='matched'
          AND COALESCE(bc_login, atf_login)=?
          AND COALESCE(bc_amount, atf_amount)=?
          AND COALESCE(bc_payment_method, atf_payment_method)=?
          AND COALESCE(bc_created_at, atf_created_at)=?
        """,
        (now_iso(), business_date, login, amount, method, created_at),
    )


def reconcile_deposit_day(business_date: str) -> dict:
    with closing(get_db()) as conn:
        cur = conn.cursor()
        bc_rows = cur.execute("SELECT * FROM bc_transactions WHERE business_date=? AND source_kind='deposit'", (business_date,)).fetchall()
        atf_rows = cur.execute("SELECT * FROM atf_transactions WHERE business_date=? AND record_status!='silindi' AND LOWER(type_name) LIKE '%yat%'", (business_date,)).fetchall()
        bc_map = defaultdict(list)
        atf_map = defaultdict(list)
        for r in bc_rows:
            key = make_match_key(r["client_login_norm"], r["created_at_norm"], float(r["amount_norm"] or 0), r["payment_system_norm"] or "")
            bc_map[key].append(r)
        for r in atf_rows:
            key = make_match_key(r["client_login_norm"], r["created_at_norm"], float(r["amount_norm"] or 0), r["payment_method_norm"] or "")
            atf_map[key].append(r)
        all_keys = sorted(set(bc_map.keys()) | set(atf_map.keys()))
        inserted = matched = mismatched = 0
        for key in all_keys:
            bc_list = bc_map.get(key, [])
            atf_list = atf_map.get(key, [])
            max_len = max(len(bc_list), len(atf_list))
            for i in range(max_len):
                bc = bc_list[i] if i < len(bc_list) else None
                atf = atf_list[i] if i < len(atf_list) else None
                if bc and atf:
                    status = mismatch_type = "matched"
                    matched += 1
                elif bc and not atf:
                    status = mismatch_type = "missing_in_atf"
                    mismatched += 1
                else:
                    status = mismatch_type = "missing_in_bc"
                    mismatched += 1
                row_key = key if (bc and atf) else f"{key}|{i}|{status}"
                cur.execute(
                    """
                    INSERT OR IGNORE INTO reconciliation_results
                    (business_date, match_key, bc_transaction_id, atf_transaction_id, status, mismatch_type, bc_created_at,
                     atf_created_at, bc_login, atf_login, bc_amount, atf_amount, bc_payment_method, atf_payment_method,
                     type_name, first_detected_at, last_checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        business_date, row_key, bc["id"] if bc else None, atf["id"] if atf else None, status, mismatch_type,
                        bc["created_at_norm"] if bc else None, atf["created_at_norm"] if atf else None,
                        bc["client_login_raw"] if bc else None, atf["client_login_raw"] if atf else None,
                        float(bc["amount_norm"] or 0) if bc else None, float(atf["amount_norm"] or 0) if atf else None,
                        bc["payment_system_display"] if bc else None, atf["payment_method_display"] if atf else None,
                        "Yatırım", now_iso(), now_iso(),
                    ),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    cur.execute(
                        "UPDATE reconciliation_results SET last_checked_at=?, status=?, mismatch_type=?, bc_transaction_id=COALESCE(?, bc_transaction_id), atf_transaction_id=COALESCE(?, atf_transaction_id), panel_status='open' WHERE match_key=?",
                        (now_iso(), status, mismatch_type, bc["id"] if bc else None, atf["id"] if atf else None, row_key),
                    )
                if status == "matched":
                    login_val = bc["client_login_raw"] if bc else atf["client_login_raw"]
                    amount_val = float(bc["amount_norm"] if bc else atf["amount_norm"])
                    method_val = bc["payment_system_display"] if bc else atf["payment_method_display"]
                    created_val = bc["created_at_norm"] if bc else atf["created_at_norm"]
                    close_old_open_mismatches(cur, "reconciliation_results", business_date, login_val, amount_val, method_val, created_val)
        conn.commit()
    return {"deposit_inserted": inserted, "deposit_matched": matched, "deposit_mismatched": mismatched}


def reconcile_withdraw_day(business_date: str) -> dict:
    with closing(get_db()) as conn:
        cur = conn.cursor()
        bc_rows = cur.execute("SELECT * FROM bc_withdrawals WHERE business_date=?", (business_date,)).fetchall()
        atf_rows = cur.execute("SELECT * FROM atf_withdrawals WHERE business_date=? AND record_status!='silindi'", (business_date,)).fetchall()
        bc_map = defaultdict(list)
        atf_map = defaultdict(list)
        for r in bc_rows:
            key = make_match_key(r["client_login_norm"], r["created_at_norm"], float(r["amount_norm"] or 0), r["payment_system_norm"] or "")
            bc_map[key].append(r)
        for r in atf_rows:
            key = make_match_key(r["client_login_norm"], r["created_at_norm"], float(r["amount_norm"] or 0), r["payment_method_norm"] or "")
            atf_map[key].append(r)
        all_keys = sorted(set(bc_map.keys()) | set(atf_map.keys()))
        inserted = matched = mismatched = 0
        for key in all_keys:
            bc_list = bc_map.get(key, [])
            atf_list = atf_map.get(key, [])
            max_len = max(len(bc_list), len(atf_list))
            for i in range(max_len):
                bc = bc_list[i] if i < len(bc_list) else None
                atf = atf_list[i] if i < len(atf_list) else None
                if bc and atf:
                    status = mismatch_type = "matched"
                    matched += 1
                elif bc and not atf:
                    status = mismatch_type = "missing_in_atf"
                    mismatched += 1
                else:
                    status = mismatch_type = "missing_in_bc"
                    mismatched += 1
                row_key = key if (bc and atf) else f"{key}|{i}|{status}"
                cur.execute(
                    """
                    INSERT OR IGNORE INTO withdrawal_reconciliation_results
                    (business_date, match_key, bc_withdrawal_id, atf_withdrawal_id, status, mismatch_type, bc_created_at,
                     atf_created_at, bc_login, atf_login, bc_amount, atf_amount, bc_payment_method, atf_payment_method,
                     type_name, first_detected_at, last_checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        business_date, row_key, bc["id"] if bc else None, atf["id"] if atf else None, status, mismatch_type,
                        bc["created_at_norm"] if bc else None, atf["created_at_norm"] if atf else None,
                        bc["client_login_raw"] if bc else None, atf["client_login_raw"] if atf else None,
                        float(bc["amount_norm"] or 0) if bc else None, float(atf["amount_norm"] or 0) if atf else None,
                        bc["payment_system_display"] if bc else None, atf["payment_method_display"] if atf else None,
                        "Çekim", now_iso(), now_iso(),
                    ),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    cur.execute(
                        "UPDATE withdrawal_reconciliation_results SET last_checked_at=?, status=?, mismatch_type=?, bc_withdrawal_id=COALESCE(?, bc_withdrawal_id), atf_withdrawal_id=COALESCE(?, atf_withdrawal_id), panel_status='open' WHERE match_key=?",
                        (now_iso(), status, mismatch_type, bc["id"] if bc else None, atf["id"] if atf else None, row_key),
                    )
                if status == "matched":
                    login_val = bc["client_login_raw"] if bc else atf["client_login_raw"]
                    amount_val = float(bc["amount_norm"] if bc else atf["amount_norm"])
                    method_val = bc["payment_system_display"] if bc else atf["payment_method_display"]
                    created_val = bc["created_at_norm"] if bc else atf["created_at_norm"]
                    close_old_open_mismatches(cur, "withdrawal_reconciliation_results", business_date, login_val, amount_val, method_val, created_val)
        conn.commit()
    return {"withdraw_inserted": inserted, "withdraw_matched": matched, "withdraw_mismatched": mismatched}


def rebuild_method_normalizations():
    with closing(get_db()) as conn:
        cur = conn.cursor()
        for r in cur.execute("SELECT id, payment_system_raw FROM bc_transactions").fetchall():
            raw = r["payment_system_raw"] or ""
            cur.execute("UPDATE bc_transactions SET payment_system_norm=?, payment_system_display=? WHERE id=?", (normalize_method_key(raw), display_method(raw), r["id"]))
        for r in cur.execute("SELECT id, payment_method_raw FROM atf_transactions").fetchall():
            raw = r["payment_method_raw"] or ""
            cur.execute("UPDATE atf_transactions SET payment_method_norm=?, payment_method_display=? WHERE id=?", (normalize_method_key(raw), display_method(raw), r["id"]))
        for r in cur.execute("SELECT id, payment_system_raw FROM bc_withdrawals").fetchall():
            raw = r["payment_system_raw"] or ""
            cur.execute("UPDATE bc_withdrawals SET payment_system_norm=?, payment_system_display=? WHERE id=?", (normalize_method_key(raw), display_method(raw), r["id"]))
        for r in cur.execute("SELECT id, payment_method_raw FROM atf_withdrawals").fetchall():
            raw = r["payment_method_raw"] or ""
            cur.execute("UPDATE atf_withdrawals SET payment_method_norm=?, payment_method_display=? WHERE id=?", (normalize_method_key(raw), display_method(raw), r["id"]))
        conn.commit()


def build_analysis_rows(business_date: str) -> list[dict]:
    with closing(get_db()) as conn:
        bc_dep = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_totals WHERE business_date=? AND source_name='BC' AND flow_type='deposit'",
            (business_date,)
        ).fetchall()}

        bc_wdr = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_totals WHERE business_date=? AND source_name='BC' AND flow_type='withdraw'",
            (business_date,)
        ).fetchall()}

        atf_dep = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_totals WHERE business_date=? AND source_name='ATF' AND flow_type='deposit'",
            (business_date,)
        ).fetchall()}

        atf_wdr = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_totals WHERE business_date=? AND source_name='ATF' AND flow_type='withdraw'",
            (business_date,)
        ).fetchall()}

        rates = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_commission_rates"
        ).fetchall()}

        reports = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM atf_method_reports WHERE business_date=?",
            (business_date,)
        ).fetchall()}

        manuals = {r["method_key"]: row_to_dict(r) for r in conn.execute(
            "SELECT * FROM method_analysis_manual WHERE business_date=?",
            (business_date,)
        ).fetchall()}

    all_keys = sorted(set(bc_dep) | set(bc_wdr) | set(atf_dep) | set(atf_wdr) | set(rates) | set(reports) | set(manuals))

    rows = []
    for key in all_keys:
        display = (
            (bc_dep.get(key) or {}).get("method_display")
            or (bc_wdr.get(key) or {}).get("method_display")
            or (atf_dep.get(key) or {}).get("method_display")
            or (atf_wdr.get(key) or {}).get("method_display")
            or (reports.get(key) or {}).get("method_display")
            or (rates.get(key) or {}).get("method_display")
            or (manuals.get(key) or {}).get("method_display")
            or key
        )

        bc_dep_total = float((bc_dep.get(key) or {}).get("total_amount") or 0)
        bc_wdr_total = float((bc_wdr.get(key) or {}).get("total_amount") or 0)
        atf_dep_total = float((atf_dep.get(key) or {}).get("total_amount") or 0)
        atf_wdr_total = float((atf_wdr.get(key) or {}).get("total_amount") or 0)

        bc_rate = float((rates.get(key) or {}).get("bc_commission_rate") or 0)
        bc_commission = round((bc_dep_total + bc_wdr_total) * bc_rate / 100.0, 2)

        atf_commission = float((reports.get(key) or {}).get("commission_total") or 0)
        atf_net = float((reports.get(key) or {}).get("net_total") or 0)

        manual_deposit = float((manuals.get(key) or {}).get("manual_deposit") or 0)
        manual_withdraw = float((manuals.get(key) or {}).get("manual_withdraw") or 0)
        manual_commission = float((manuals.get(key) or {}).get("manual_commission") or 0)
        manual_net = float((manuals.get(key) or {}).get("manual_net") or 0)

        note_text = (manuals.get(key) or {}).get("note_text") or ""

        # SADECE GERÇEK İŞLEM VAR MI?
        has_transaction = any([
            abs(bc_dep_total) > 0.0001,
            abs(bc_wdr_total) > 0.0001,
            abs(atf_dep_total) > 0.0001,
            abs(atf_wdr_total) > 0.0001,
        ])

        # TOPLAM HACİM
        volume_total = bc_dep_total + bc_wdr_total + atf_dep_total + atf_wdr_total

        rows.append({
            "method_key": key,
            "method_display": display,
            "bc_deposit": bc_dep_total,
            "bc_withdraw": bc_wdr_total,
            "bc_rate": bc_rate,
            "bc_commission": bc_commission,
            "atf_deposit": atf_dep_total,
            "atf_withdraw": atf_wdr_total,
            "atf_commission": atf_commission,
            "atf_net": atf_net,
            "manual_deposit": manual_deposit,
            "manual_withdraw": manual_withdraw,
            "manual_commission": manual_commission,
            "manual_net": manual_net,
            "note_text": note_text,
            "has_transaction": has_transaction,
            "volume_total": volume_total,
        })

    # İŞLEM OLANLAR ÜSTTE, OLMAYANLAR ALTTA
    # Kendi içinde de büyük hacim üstte
    rows.sort(
        key=lambda r: (
            0 if r["has_transaction"] else 1,
            -float(r["volume_total"] or 0),
            (r["method_display"] or "").lower(),
        )
    )

    return rows


def build_analysis_status(row: dict) -> str:
    msgs = []

    bc_vs_atf_comm = round(float(row["bc_commission"]) - float(row["atf_commission"]), 2)
    if abs(bc_vs_atf_comm) > 0.009:
        msgs.append(f"KOM FARK {bc_vs_atf_comm:+,.2f}")

    manual_vs_atf_dep = round(float(row["manual_deposit"]) - float(row["atf_deposit"]), 2)
    if abs(manual_vs_atf_dep) > 0.009:
        msgs.append(f"MANUEL YAT {manual_vs_atf_dep:+,.2f}")

    manual_vs_atf_wdr = round(float(row["manual_withdraw"]) - float(row["atf_withdraw"]), 2)
    if abs(manual_vs_atf_wdr) > 0.009:
        msgs.append(f"MANUEL ÇEK {manual_vs_atf_wdr:+,.2f}")

    manual_vs_atf_comm = round(float(row["manual_commission"]) - float(row["atf_commission"]), 2)
    if abs(manual_vs_atf_comm) > 0.009:
        msgs.append(f"MANUEL KOM {manual_vs_atf_comm:+,.2f}")

    manual_vs_atf_net = round(float(row["manual_net"]) - float(row["atf_net"]), 2)
    if abs(manual_vs_atf_net) > 0.009:
        msgs.append(f"GÜN SONU {manual_vs_atf_net:+,.2f}")

    if not msgs:
        return "FARK YOK"

    return " | ".join(msgs)


class ReconciliationService:
    def __init__(self):
        self.bc = BCClient()
        self.atf = ATFClient(ATF_BASE_URL, ATF_USERNAME, ATF_PASSWORD)

    def sync_one_day(self, day: date) -> dict:
        business_date = day.strftime("%Y-%m-%d")
        result = {"business_date": business_date}
        bc_run = insert_sync_run("bc", business_date)
        try:
            self.bc.ensure_token()
            dep_rows = self.bc.fetch_deposits_for_day(day)
            bc_dep_inserted = upsert_bc_deposits(dep_rows)
            bc_dep_method_totals = aggregate_method_totals_from_rows([
                {
                    "payment_system_norm": normalize_method_key(x.get("PaymentSystemName")),
                    "payment_system_display": display_method(x.get("PaymentSystemName")),
                    "amount_norm": norm_money(x.get("Amount")),
                }
                for x in dep_rows
            ], "BC")
            upsert_method_totals(business_date, "BC", "deposit", bc_dep_method_totals)
            bc_wdr_rows = self.bc.fetch_withdrawals_for_day(day)
            bc_wdr_inserted, bc_paid_count = upsert_bc_withdrawals(bc_wdr_rows)
            bc_wdr_norm_rows = []
            for x in bc_wdr_rows:
                if int(x.get("State") or 0) != 3:
                    continue
                method_raw = x.get("PaymentSystemName") or x.get("Notes") or ""
                bc_wdr_norm_rows.append({
                    "payment_system_norm": normalize_method_key(method_raw),
                    "payment_system_display": display_method(method_raw),
                    "amount_norm": norm_money(x.get("Amount")),
                })
            bc_wdr_method_totals = aggregate_method_totals_from_rows(bc_wdr_norm_rows, "BC")
            upsert_method_totals(business_date, "BC", "withdraw", bc_wdr_method_totals)
            bc_wdr_total = sum(v["total"] for v in bc_wdr_method_totals.values())
            bc_wdr_count = sum(v["count"] for v in bc_wdr_method_totals.values())
            finish_sync_run(bc_run, len(dep_rows) + len(bc_wdr_rows), bc_dep_inserted + bc_wdr_inserted, 0, "ok")
            result.update({
                "bc_deposit_count": len(dep_rows),
                "bc_withdraw_total": bc_wdr_total,
                "bc_withdraw_count": bc_wdr_count,
                "bc_withdraw_saved_rows": bc_wdr_inserted,
                "bc_paid_count": bc_paid_count,
            })
        except Exception as e:
            finish_sync_run(bc_run, 0, 0, 0, "error", str(e))
            raise

        atf_run = insert_sync_run("atf", business_date)
        try:
            self.atf.login()
            atf_rows = self.atf.fetch_all_transactions_for_day(day)
            dep_rows = [r for r in atf_rows if r["norm_type"] == "yatirim"]
            wdr_rows = [r for r in atf_rows if r["norm_type"] == "cekim"]
            dep_rows = list({str(r["atf_source_id"]): r for r in dep_rows}.values())
            wdr_rows = list({str(r["atf_source_id"]): r for r in wdr_rows}.values())
            atf_dep_inserted = upsert_atf_deposits(dep_rows, business_date)
            atf_wdr_inserted = upsert_atf_withdrawals(wdr_rows, business_date)
            atf_dep_totals = aggregate_method_totals_from_rows(dep_rows, "ATF")
            upsert_method_totals(business_date, "ATF", "deposit", atf_dep_totals)
            atf_wdr_totals = aggregate_method_totals_from_rows(wdr_rows, "ATF")
            upsert_method_totals(business_date, "ATF", "withdraw", atf_wdr_totals)
            atf_wdr_total = sum(v["total"] for v in atf_wdr_totals.values())
            atf_wdr_count = sum(v["count"] for v in atf_wdr_totals.values())
            report_saved = 0
            try:
                report_rows = self.atf.fetch_method_report_for_day(day)
                report_saved = upsert_atf_method_report_rows(business_date, report_rows)
            except Exception:
                log.exception("ATF yöntem raporu alınamadı")
            finish_sync_run(atf_run, len(atf_rows), atf_dep_inserted + atf_wdr_inserted, 0, "ok")
            result.update({
                "atf_deposit_count": len(dep_rows),
                "atf_withdraw_total": atf_wdr_total,
                "atf_withdraw_count": atf_wdr_count,
                "atf_withdraw_saved_rows": atf_wdr_inserted,
                "atf_report_method_rows": report_saved,
            })
        except Exception as e:
            finish_sync_run(atf_run, 0, 0, 0, "error", str(e))
            raise

        upsert_withdrawal_total(
            business_date,
            float(result.get("bc_withdraw_total", 0)),
            float(result.get("atf_withdraw_total", 0)),
            int(result.get("bc_withdraw_count", 0)),
            int(result.get("atf_withdraw_count", 0)),
        )
        result.update(reconcile_deposit_day(business_date))
        result.update(reconcile_withdraw_day(business_date))
        return result

    def sync_recent_days(self, days: int) -> list[dict]:
        today = datetime.now(TZ).date()
        return [self.sync_one_day(today - timedelta(days=i)) for i in range(days)]

    def sync_date_range(self, start_date: date, end_date: date) -> list[dict]:
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        out = []
        cur = start_date
        while cur <= end_date:
            out.append(self.sync_one_day(cur))
            cur += timedelta(days=1)
        return out


service = ReconciliationService()


async def background_recon_loop():
    await asyncio.sleep(10)
    while True:
        try:
            if RECON_ENABLED:
                log.info("Arka plan senkron başladı...")
                await asyncio.to_thread(service.sync_recent_days, RECON_DAYS)
                log.info("Arka plan senkron tamamlandı.")
        except Exception:
            log.exception("background_recon_loop hata")
        await asyncio.sleep(RECON_POLL_SECONDS)


def parse_date_range(request: web.Request):
    start = request.query.get("start", "")
    end = request.query.get("end", "")
    today = datetime.now(TZ).date()
    if not start and not end:
        return today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    try:
        s = datetime.strptime(start, "%Y-%m-%d").date() if start else datetime.strptime(end, "%Y-%m-%d").date()
        e = datetime.strptime(end, "%Y-%m-%d").date() if end else s
        if e < s:
            s, e = e, s
        return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
    except Exception:
        return today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def check_basic_auth(request: web.Request) -> bool:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        user, pwd = raw.split(":", 1)
        return user == PANEL_USER and pwd == PANEL_PASS
    except Exception:
        return False


async def require_auth(request: web.Request):
    if not check_basic_auth(request):
        raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Panel"'})


def html_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang='tr'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{html.escape(title)}</title>
<style>
body{{font-family:Arial,sans-serif;margin:16px;background:#f5f7fb;color:#1f2937}}
a{{color:#2563eb;text-decoration:none}}
.nav{{display:flex;gap:8px;flex-wrap:wrap;align-items:center}}
.nav a{{margin-right:0;font-weight:700;padding:8px 12px;border-radius:10px;background:#f8fafc;border:1px solid #dbe3f0}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:12px 14px;margin-bottom:12px;box-shadow:0 2px 8px rgba(0,0,0,.04)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}}
.stat{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:10px 12px}}
.stat .n{{font-size:19px;font-weight:800;line-height:1.15;margin-top:4px}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{border:1px solid #e5e7eb;padding:8px;font-size:13px;text-align:left;vertical-align:top}}
th{{background:#eef2ff;position:sticky;top:0}}
.matched{{background:#dff3e7}}
.mismatch{{background:#fde2e2}}
.chips{{display:flex;flex-wrap:wrap;gap:6px}}
.chips a{{display:inline-block;margin:0;padding:5px 9px;border-radius:999px;background:#eef2ff;border:1px solid #c7d2fe;font-size:13px;line-height:1.1}}
form.inline{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:10px 0}}
textarea{{font-family:Arial,sans-serif}}
</style>
</head>
<body>
<div class='card'><div class='nav'>
<a href='/panel'>Özet</a>
<a href='/panel/results'>Yatırım Karşılaştırma</a>
<a href='/panel/withdraw-results'>Çekim Karşılaştırma</a>
<a href='/panel/deposit-missing'>Eksik Yatırımlar</a>
<a href='/panel/withdraw-missing'>Eksik Çekimler</a>
<a href='/panel/bc'>BC Yatırımlar</a>
<a href='/panel/atf'>ATF Yatırımlar</a>
<a href='/panel/atf-changed'>ATF Düzenlenenler</a>
<a href='/panel/atf-deleted'>ATF Silinenler</a>
<a href='/panel/withdrawals'>Çekim Özet</a>
<a href='/panel/bc-withdrawals'>BC Çekimler</a>
<a href='/panel/atf-withdrawals'>ATF Çekimler</a>
<a href='/panel/method-aliases'>Yöntem Eşleme</a>
<a href='/panel/analysis'>Günlük Analiz</a>
<a href='/panel/missing-deposit-methods'>Eksik Yatırım Yöntemleri</a>
<a href='/panel/missing-withdraw-methods'>Eksik Çekim Yöntemleri</a>
<a href='/panel/analysis'>Günlük Analiz</a>
</div></div>
{body}
</body>
</html>"""


def query_method_diff(start_date, end_date, flow_type: str):
    sql = """
    WITH bc AS (
      SELECT method_key, MAX(method_display) AS disp, SUM(total_amount) AS total_amount, SUM(tx_count) AS tx_count
      FROM method_totals
      WHERE business_date BETWEEN ? AND ? AND source_name='BC' AND flow_type=?
      GROUP BY method_key
    ),
    atf AS (
      SELECT method_key, MAX(method_display) AS disp, SUM(total_amount) AS total_amount, SUM(tx_count) AS tx_count
      FROM method_totals
      WHERE business_date BETWEEN ? AND ? AND source_name='ATF' AND flow_type=?
      GROUP BY method_key
    ),
    report_keys AS (
      SELECT method_key, MAX(method_display) AS disp
      FROM atf_method_reports
      WHERE business_date BETWEEN ? AND ?
      GROUP BY method_key
    ),
    manual_keys AS (
      SELECT method_key, MAX(method_display) AS disp
      FROM method_analysis_manual
      WHERE business_date BETWEEN ? AND ?
      GROUP BY method_key
    ),
    rate_keys AS (
      SELECT method_key, MAX(method_display) AS disp
      FROM method_commission_rates
      GROUP BY method_key
    ),
    keys AS (
      SELECT method_key FROM bc
      UNION
      SELECT method_key FROM atf
      UNION
      SELECT method_key FROM report_keys
      UNION
      SELECT method_key FROM manual_keys
      UNION
      SELECT method_key FROM rate_keys
    )
    SELECT keys.method_key AS method_key,
           COALESCE(bc.disp, atf.disp, report_keys.disp, manual_keys.disp, rate_keys.disp, keys.method_key) AS method_display,
           COALESCE(bc.total_amount, 0) AS bc_total,
           COALESCE(atf.total_amount, 0) AS atf_total,
           COALESCE(bc.total_amount, 0) - COALESCE(atf.total_amount, 0) AS diff_total,
           COALESCE(bc.tx_count, 0) AS bc_count,
           COALESCE(atf.tx_count, 0) AS atf_count
    FROM keys
    LEFT JOIN bc ON bc.method_key = keys.method_key
    LEFT JOIN atf ON atf.method_key = keys.method_key
    LEFT JOIN report_keys ON report_keys.method_key = keys.method_key
    LEFT JOIN manual_keys ON manual_keys.method_key = keys.method_key
    LEFT JOIN rate_keys ON rate_keys.method_key = keys.method_key
    WHERE COALESCE(bc.total_amount, 0) != 0
       OR COALESCE(atf.total_amount, 0) != 0
       OR COALESCE(bc.tx_count, 0) != 0
       OR COALESCE(atf.tx_count, 0) != 0
    ORDER BY method_display ASC
    """
    with closing(get_db()) as conn:
        return conn.execute(
            sql,
            [
                start_date, end_date, flow_type,
                start_date, end_date, flow_type,
                start_date, end_date,
                start_date, end_date,
            ],
        ).fetchall()


async def panel_home(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    sync_msg = ""
    if request.query.get("do_sync", "") == "1":
        try:
            s = datetime.strptime(start_date, "%Y-%m-%d").date()
            e = datetime.strptime(end_date, "%Y-%m-%d").date()
            res = await asyncio.to_thread(service.sync_date_range, s, e)
            sync_msg = f"<div class='card'><b>Senkron tamamlandı:</b> {html.escape(start_date)} → {html.escape(end_date)} | {html.escape(str(res))}</div>"
        except Exception as ex:
            sync_msg = f"<div class='card'><b>Senkron hatası:</b> {html.escape(str(ex))}</div>"
    params = [start_date, end_date]
    where = " WHERE business_date BETWEEN ? AND ? "
    with closing(get_db()) as conn:
        bc_count = conn.execute(f"SELECT COUNT(*) FROM bc_transactions {where} AND source_kind='deposit'", params).fetchone()[0]
        atf_count = conn.execute(f"SELECT COUNT(*) FROM atf_transactions {where} AND LOWER(type_name) LIKE '%yat%'", params).fetchone()[0]
        dep_matched = conn.execute(f"SELECT COUNT(*) FROM reconciliation_results {where} AND status='matched' AND panel_status='open'", params).fetchone()[0]
        dep_mismatch = conn.execute(f"SELECT COUNT(*) FROM reconciliation_results {where} AND status!='matched' AND panel_status='open'", params).fetchone()[0]
        wdr_matched = conn.execute(f"SELECT COUNT(*) FROM withdrawal_reconciliation_results {where} AND status='matched' AND panel_status='open'", params).fetchone()[0]
        wdr_mismatch = conn.execute(f"SELECT COUNT(*) FROM withdrawal_reconciliation_results {where} AND status!='matched' AND panel_status='open'", params).fetchone()[0]
        bc_w_rows = conn.execute("SELECT COUNT(*) FROM bc_withdrawals WHERE business_date BETWEEN ? AND ?", params).fetchone()[0]
        atf_w_rows = conn.execute("SELECT COUNT(*) FROM atf_withdrawals WHERE business_date BETWEEN ? AND ? AND record_status!='silindi'", params).fetchone()[0]
        recent = conn.execute("SELECT * FROM sync_runs ORDER BY id DESC LIMIT 12").fetchall()
    dep_method_diff = query_method_diff(start_date, end_date, "deposit")
    wdr_method_diff = query_method_diff(start_date, end_date, "withdraw")
    total_bc_dep = sum(float(r["bc_total"] or 0) for r in dep_method_diff)
    total_atf_dep = sum(float(r["atf_total"] or 0) for r in dep_method_diff)
    total_bc_wdr = sum(float(r["bc_total"] or 0) for r in wdr_method_diff)
    total_atf_wdr = sum(float(r["atf_total"] or 0) for r in wdr_method_diff)
    body = [
        sync_msg,
        f"""
        <div class='card'>
            <h2>Tarih Aralığı</h2>
            <form method='get' class='inline'>
                <label>Başlangıç</label>
                <input type='date' name='start' value='{html.escape(start_date)}'>
                <label>Bitiş</label>
                <input type='date' name='end' value='{html.escape(end_date)}'>
                <button type='submit'>Uygula</button>
                <button type='submit' name='do_sync' value='1'>Senkron + Uygula</button>
                <a href='/panel'>Bugün</a>
            </form>
        </div>
        """,
        "<div class='grid'>",
        f"<div class='stat'><div>BC Yatırım</div><div class='n'>{bc_count}</div></div>",
        f"<div class='stat'><div>ATF Yatırım</div><div class='n'>{atf_count}</div></div>",
        f"<div class='stat'><div>BC Yatırım Toplam</div><div class='n'>{total_bc_dep:,.2f}</div></div>",
        f"<div class='stat'><div>ATF Yatırım Toplam</div><div class='n'>{total_atf_dep:,.2f}</div></div>",
        f"<div class='stat'><div>Yatırım Eşleşen</div><div class='n'>{dep_matched}</div></div>",
        f"<div class='stat'><div>Yatırım Eşleşmeyen</div><div class='n'>{dep_mismatch}</div></div>",
        f"<div class='stat'><div>BC Çekim Satır</div><div class='n'>{bc_w_rows}</div></div>",
        f"<div class='stat'><div>ATF Çekim Satır</div><div class='n'>{atf_w_rows}</div></div>",
        f"<div class='stat'><div>Çekim Eşleşen</div><div class='n'>{wdr_matched}</div></div>",
        f"<div class='stat'><div>Çekim Eşleşmeyen</div><div class='n'>{wdr_mismatch}</div></div>",
        f"<div class='stat'><div>BC Çekim Toplam</div><div class='n'>{total_bc_wdr:,.2f}</div></div>",
        f"<div class='stat'><div>ATF Çekim Toplam</div><div class='n'>{total_atf_wdr:,.2f}</div></div>",
        "</div>",
        "<div class='card'><h2>Yöntem Bazlı Yatırım Karşılaştırma</h2><table><tr><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th></tr>",
    ]
    for r in dep_method_diff:
        cls = "mismatch" if abs(float(r['diff_total'] or 0)) > 0.0001 else ""
        method_name = r['method_display'] or ''
        method_href = f"/panel/results?start={html.escape(start_date)}&end={html.escape(end_date)}&method={html.escape(method_name)}"
        body.append(f"<tr class='{cls}'><td><a href='{method_href}'>{html.escape(method_name or '-')}</a></td><td>{(r['bc_total'] or 0):,.2f} ({r['bc_count'] or 0})</td><td>{(r['atf_total'] or 0):,.2f} ({r['atf_count'] or 0})</td><td>{(r['diff_total'] or 0):,.2f}</td></tr>")
    body.append("</table></div>")
    body.append("<div class='card'><h2>Yöntem Bazlı Çekim Karşılaştırma</h2><table><tr><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th></tr>")
    for r in wdr_method_diff:
        cls = "mismatch" if abs(float(r['diff_total'] or 0)) > 0.0001 else ""
        method_name = r['method_display'] or ''
        method_href = f"/panel/withdraw-results?start={html.escape(start_date)}&end={html.escape(end_date)}&method={html.escape(method_name)}"
        body.append(f"<tr class='{cls}'><td><a href='{method_href}'>{html.escape(method_name or '-')}</a></td><td>{(r['bc_total'] or 0):,.2f} ({r['bc_count'] or 0})</td><td>{(r['atf_total'] or 0):,.2f} ({r['atf_count'] or 0})</td><td>{(r['diff_total'] or 0):,.2f}</td></tr>")
    body.append("</table></div>")
    body.append("<div class='card'><h2>Son Senkronlar</h2><table><tr><th>ID</th><th>Kaynak</th><th>Tarih</th><th>Başladı</th><th>Bitti</th><th>Çekilen</th><th>Eklenen</th><th>Durum</th></tr>")
    for r in recent:
        body.append(f"<tr><td>{r['id']}</td><td>{html.escape(r['source_name'] or '')}</td><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(r['started_at'] or '')}</td><td>{html.escape(r['finished_at'] or '')}</td><td>{r['total_fetched'] or 0}</td><td>{r['total_inserted'] or 0}</td><td>{html.escape(r['status'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("Özet", "".join(body)), content_type="text/html")


def save_mismatch_note(result_id: int, note_text: str, table_name: str = "reconciliation_results"):
    with closing(get_db()) as conn:
        conn.execute(
            f"""
            UPDATE {table_name}
            SET review_note=?, last_checked_at=?
            WHERE id=?
            """,
            (note_text.strip(), now_iso(), int(result_id)),
        )
        conn.commit()


def get_mismatch_note_map(
    start_date: str,
    end_date: str,
    table_name: str = "reconciliation_results",
) -> dict:
    with closing(get_db()) as conn:
        rows = conn.execute(
            f"""
            SELECT id, review_note
            FROM {table_name}
            WHERE business_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()

    return {int(r["id"]): (r["review_note"] or "") for r in rows}

async def panel_results(request: web.Request):
    await require_auth(request)

    status_filter = request.query.get("status", "")
    method_filter = request.query.get("method", "")
    start_date, end_date = parse_date_range(request)

    msg = ""

    if request.method == "POST":
        data = await request.post()

        note_result_id = str(data.get("note_result_id", "")).strip()
        note_business_date = str(data.get("note_business_date", "")).strip()
        note_match_key = str(data.get("note_match_key", "")).strip()
        reason_type = str(data.get("reason_type", "")).strip()
        note_text = str(data.get("note_text", "")).strip()
        note_status_text = str(data.get("note_status_text", "Yeni")).strip() or "Yeni"

        try:
            save_mismatch_note(
                "reconciliation_results",
                int(note_result_id),
                note_business_date,
                note_match_key,
                reason_type,
                note_text,
                note_status_text,
            )
            msg = "<div class='card'><b>Not kaydedildi.</b></div>"
        except Exception as e:
            msg = f"<div class='card'><b>Not kaydedilemedi:</b> {html.escape(str(e))}</div>"

        status_filter = str(data.get("status", status_filter)).strip()
        method_filter = str(data.get("method", method_filter)).strip()
        start_date = str(data.get("start", start_date)).strip() or start_date
        end_date = str(data.get("end", end_date)).strip() or end_date

    q = """
        SELECT *
        FROM reconciliation_results
        WHERE type_name='Yatırım'
          AND panel_status='open'
    """
    params = []

    if status_filter:
        q += " AND status = ?"
        params.append(status_filter)

    if method_filter:
        q += " AND COALESCE(bc_payment_method, atf_payment_method, '') = ?"
        params.append(method_filter)

    q += " AND business_date BETWEEN ? AND ?"
    params.extend([start_date, end_date])

    q += " ORDER BY business_date DESC, COALESCE(atf_created_at, bc_created_at) DESC, id DESC"

    method_q = """
        SELECT COALESCE(bc_payment_method, atf_payment_method, '') AS method_name,
               COUNT(*) AS row_count
        FROM reconciliation_results
        WHERE type_name='Yatırım'
          AND panel_status='open'
          AND business_date BETWEEN ? AND ?
    """
    method_params = [start_date, end_date]

    if status_filter:
        method_q += " AND status = ?"
        method_params.append(status_filter)

    method_q += """
        AND COALESCE(bc_payment_method, atf_payment_method, '') != ''
        GROUP BY 1
        ORDER BY 1
    """

    with closing(get_db()) as conn:
        rows = conn.execute(q, params).fetchall()
        method_rows = conn.execute(method_q, method_params).fetchall()

    note_map = get_mismatch_note_map(
        "reconciliation_results",
        [int(r["id"]) for r in rows]
    )

    status_base = {"start": start_date, "end": end_date}
    method_base = {"start": start_date, "end": end_date}

    if status_filter:
        method_base["status"] = status_filter
    if method_filter:
        status_base["method"] = method_filter

    status_chips = [
        ("matched", "Eşleşen"),
        ("missing_in_atf", "Sadece BC"),
        ("missing_in_bc", "Sadece ATF"),
    ]

    body = [msg]
    body.append("<div class='card'><h2>Yatırım Karşılaştırma</h2>")

    body.append("<div class='chips'>")
    for status_value, label in status_chips:
        link_params = dict(status_base)
        link_params["status"] = status_value
        body.append(
            f"<a href='/panel/results?{html.escape(urlencode(link_params))}'>{html.escape(label)}</a>"
        )
    body.append(
        f"<a href='/panel/results?{html.escape(urlencode(status_base))}'>Tümü</a>"
    )
    body.append("</div>")

    body.append("<div class='card' style='margin-top:10px'><b>Yönteme göre incele</b><div class='chips' style='margin-top:8px'>")
    body.append(
        f"<a href='/panel/results?{html.escape(urlencode(method_base))}'>Tüm Yöntemler</a>"
    )

    for mr in method_rows:
        mname = mr["method_name"] or ""
        chip_params = dict(method_base)
        chip_params["method"] = mname
        label = f"{mname} ({mr['row_count'] or 0})"
        body.append(
            f"<a href='/panel/results?{html.escape(urlencode(chip_params))}'>{html.escape(label)}</a>"
        )

    body.append("</div></div>")

    body.append(
        f"<form method='get' class='inline'>"
        f"<input type='hidden' name='status' value='{html.escape(status_filter)}'>"
        f"<input type='hidden' name='method' value='{html.escape(method_filter)}'>"
        f"<label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'>"
        f"<label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'>"
        f"<button type='submit'>Filtrele</button>"
        f"<a href='/panel/results'>Bugün</a>"
        f"</form>"
    )

    if method_filter:
        body.append(
            f"<div class='card'><b>Seçili yöntem:</b> {html.escape(method_filter)}</div>"
        )

    body.append(
        "<table>"
        "<tr>"
        "<th>Tarih</th>"
        "<th>Durum</th>"
        "<th>Zaman</th>"
        "<th>BC Login</th>"
        "<th>ATF Login</th>"
        "<th>Tutar</th>"
        "<th>BC Yöntem</th>"
        "<th>ATF Yöntem</th>"
        "<th>Sebep</th>"
        "<th>Not</th>"
        "<th>Durum Notu</th>"
        "<th>Kaydet</th>"
        "</tr>"
    )

    for r in rows:
        cls = "matched" if r["status"] == "matched" else "mismatch"
        nice = {
            "matched": "Eşleşti",
            "missing_in_atf": "Sadece BC",
            "missing_in_bc": "Sadece ATF",
        }.get(r["status"], r["status"])

        dt = r["atf_created_at"] or r["bc_created_at"] or ""
        amt = r["bc_amount"] if r["bc_amount"] is not None else r["atf_amount"]

        bc_method = r["bc_payment_method"] or ""
        atf_method = r["atf_payment_method"] or ""

        bc_link = ""
        if bc_method:
            bc_params = {"start": start_date, "end": end_date, "method": bc_method}
            if status_filter:
                bc_params["status"] = status_filter
            bc_link = f"<a href='/panel/results?{html.escape(urlencode(bc_params))}'>{html.escape(bc_method)}</a>"

        atf_link = ""
        if atf_method:
            atf_params = {"start": start_date, "end": end_date, "method": atf_method}
            if status_filter:
                atf_params["status"] = status_filter
            atf_link = f"<a href='/panel/results?{html.escape(urlencode(atf_params))}'>{html.escape(atf_method)}</a>"

        note_info = note_map.get(int(r["id"]), {})
        reason_type = note_info.get("reason_type", "")
        note_text = note_info.get("note_text", "")
        note_status_text = note_info.get("status_text", "Yeni")

        body.append(f"<tr class='{cls}'>")
        body.append(f"<td>{html.escape(r['business_date'] or '')}</td>")
        body.append(f"<td>{html.escape(nice)}</td>")
        body.append(f"<td>{html.escape(dt)}</td>")
        body.append(f"<td>{html.escape(r['bc_login'] or '')}</td>")
        body.append(f"<td>{html.escape(r['atf_login'] or '')}</td>")
        body.append(f"<td>{amt if amt is not None else ''}</td>")
        body.append(f"<td>{bc_link}</td>")
        body.append(f"<td>{atf_link}</td>")

        if r["status"] == "matched":
            body.append(f"<td>{html.escape(reason_type)}</td>")
            body.append(f"<td>{html.escape(note_text)}</td>")
            body.append(f"<td>{html.escape(note_status_text)}</td>")
            body.append("<td>-</td>")
        else:
            form_id = f"note_form_{int(r['id'])}"

            body.append("<td>")
            body.append(
                f"<select name='reason_type' form='{form_id}'>"
                f"<option value='' {'selected' if reason_type == '' else ''}>Seç</option>"
                f"<option value='ATF silindi' {'selected' if reason_type == 'ATF silindi' else ''}>ATF silindi</option>"
                f"<option value='ATF tutar degisti' {'selected' if reason_type == 'ATF tutar degisti' else ''}>ATF tutar değişti</option>"
                f"<option value='ATF yontem degisti' {'selected' if reason_type == 'ATF yontem degisti' else ''}>ATF yöntem değişti</option>"
                f"<option value='ATF login farkli' {'selected' if reason_type == 'ATF login farkli' else ''}>ATF login farklı</option>"
                f"<option value='BC eksik' {'selected' if reason_type == 'BC eksik' else ''}>BC eksik</option>"
                f"<option value='Elle kontrol edildi' {'selected' if reason_type == 'Elle kontrol edildi' else ''}>Elle kontrol edildi</option>"
                f"</select>"
            )
            body.append("</td>")

            body.append(
                f"<td><textarea name='note_text' rows='2' style='width:180px' form='{form_id}'>{html.escape(note_text)}</textarea></td>"
            )

            body.append("<td>")
            body.append(
                f"<select name='note_status_text' form='{form_id}'>"
                f"<option value='Yeni' {'selected' if note_status_text == 'Yeni' else ''}>Yeni</option>"
                f"<option value='İncelendi' {'selected' if note_status_text == 'İncelendi' else ''}>İncelendi</option>"
                f"<option value='Beklemede' {'selected' if note_status_text == 'Beklemede' else ''}>Beklemede</option>"
                f"<option value='Tamamlandı' {'selected' if note_status_text == 'Tamamlandı' else ''}>Tamamlandı</option>"
                f"</select>"
            )
            body.append("</td>")

            body.append("<td>")
            body.append(
                f"<form id='{form_id}' method='post'>"
                f"<input type='hidden' name='note_result_id' value='{int(r['id'])}'>"
                f"<input type='hidden' name='note_business_date' value='{html.escape(r['business_date'] or '')}'>"
                f"<input type='hidden' name='note_match_key' value='{html.escape(r['match_key'] or '')}'>"
                f"<input type='hidden' name='status' value='{html.escape(status_filter)}'>"
                f"<input type='hidden' name='method' value='{html.escape(method_filter)}'>"
                f"<input type='hidden' name='start' value='{html.escape(start_date)}'>"
                f"<input type='hidden' name='end' value='{html.escape(end_date)}'>"
                f"<button type='submit'>Kaydet</button>"
                f"</form>"
            )
            body.append("</td>")

        body.append("</tr>")

    body.append("</table></div>")

    return web.Response(
        text=html_page("Yatırım Karşılaştırma", "".join(body)),
        content_type="text/html"
    )

async def panel_withdraw_results(request: web.Request):
    await require_auth(request)

    status_filter = request.query.get("status", "")
    method_filter = request.query.get("method", "")
    start_date, end_date = parse_date_range(request)

    msg = ""

    if request.method == "POST":
        data = await request.post()

        note_result_id = str(data.get("note_result_id", "")).strip()
        note_business_date = str(data.get("note_business_date", "")).strip()
        note_match_key = str(data.get("note_match_key", "")).strip()
        reason_type = str(data.get("reason_type", "")).strip()
        note_text = str(data.get("note_text", "")).strip()
        note_status_text = str(data.get("note_status_text", "Yeni")).strip() or "Yeni"

        try:
            save_mismatch_note(
                "withdrawal_reconciliation_results",
                int(note_result_id),
                note_business_date,
                note_match_key,
                reason_type,
                note_text,
                note_status_text,
            )
            msg = "<div class='card'><b>Not kaydedildi.</b></div>"
        except Exception as e:
            msg = f"<div class='card'><b>Not kaydedilemedi:</b> {html.escape(str(e))}</div>"

        status_filter = str(data.get("status", status_filter)).strip()
        method_filter = str(data.get("method", method_filter)).strip()
        start_date = str(data.get("start", start_date)).strip() or start_date
        end_date = str(data.get("end", end_date)).strip() or end_date

    q = """
        SELECT *
        FROM withdrawal_reconciliation_results
        WHERE type_name='Çekim'
          AND panel_status='open'
    """
    params = []

    if status_filter:
        q += " AND status = ?"
        params.append(status_filter)

    if method_filter:
        q += " AND COALESCE(bc_payment_method, atf_payment_method, '') = ?"
        params.append(method_filter)

    q += " AND business_date BETWEEN ? AND ?"
    params.extend([start_date, end_date])

    q += " ORDER BY business_date DESC, COALESCE(atf_created_at, bc_created_at) DESC, id DESC"

    method_q = """
        SELECT COALESCE(bc_payment_method, atf_payment_method, '') AS method_name,
               COUNT(*) AS row_count
        FROM withdrawal_reconciliation_results
        WHERE type_name='Çekim'
          AND panel_status='open'
          AND business_date BETWEEN ? AND ?
    """
    method_params = [start_date, end_date]

    if status_filter:
        method_q += " AND status = ?"
        method_params.append(status_filter)

    method_q += """
        AND COALESCE(bc_payment_method, atf_payment_method, '') != ''
        GROUP BY 1
        ORDER BY 1
    """

    with closing(get_db()) as conn:
        rows = conn.execute(q, params).fetchall()
        method_rows = conn.execute(method_q, method_params).fetchall()

    note_map = get_mismatch_note_map(
        "withdrawal_reconciliation_results",
        [int(r["id"]) for r in rows]
    )

    status_base = {"start": start_date, "end": end_date}
    method_base = {"start": start_date, "end": end_date}

    if status_filter:
        method_base["status"] = status_filter
    if method_filter:
        status_base["method"] = method_filter

    status_chips = [
        ("matched", "Eşleşen"),
        ("missing_in_atf", "Sadece BC"),
        ("missing_in_bc", "Sadece ATF"),
    ]

    body = [msg]
    body.append("<div class='card'><h2>Çekim Karşılaştırma</h2>")

    body.append("<div class='chips'>")
    for status_value, label in status_chips:
        link_params = dict(status_base)
        link_params["status"] = status_value
        body.append(
            f"<a href='/panel/withdraw-results?{html.escape(urlencode(link_params))}'>{html.escape(label)}</a>"
        )
    body.append(
        f"<a href='/panel/withdraw-results?{html.escape(urlencode(status_base))}'>Tümü</a>"
    )
    body.append("</div>")

    body.append("<div class='card' style='margin-top:10px'><b>Yönteme göre incele</b><div class='chips' style='margin-top:8px'>")
    body.append(
        f"<a href='/panel/withdraw-results?{html.escape(urlencode(method_base))}'>Tüm Yöntemler</a>"
    )

    for mr in method_rows:
        mname = mr["method_name"] or ""
        chip_params = dict(method_base)
        chip_params["method"] = mname
        label = f"{mname} ({mr['row_count'] or 0})"
        body.append(
            f"<a href='/panel/withdraw-results?{html.escape(urlencode(chip_params))}'>{html.escape(label)}</a>"
        )

    body.append("</div></div>")

    body.append(
        f"<form method='get' class='inline'>"
        f"<input type='hidden' name='status' value='{html.escape(status_filter)}'>"
        f"<input type='hidden' name='method' value='{html.escape(method_filter)}'>"
        f"<label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'>"
        f"<label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'>"
        f"<button type='submit'>Filtrele</button>"
        f"<a href='/panel/withdraw-results'>Bugün</a>"
        f"</form>"
    )

    if method_filter:
        body.append(
            f"<div class='card'><b>Seçili yöntem:</b> {html.escape(method_filter)}</div>"
        )

    body.append(
        "<table>"
        "<tr>"
        "<th>Tarih</th>"
        "<th>Durum</th>"
        "<th>Zaman</th>"
        "<th>BC Login</th>"
        "<th>ATF Login</th>"
        "<th>Tutar</th>"
        "<th>BC Yöntem</th>"
        "<th>ATF Yöntem</th>"
        "<th>Sebep</th>"
        "<th>Not</th>"
        "<th>Durum Notu</th>"
        "<th>Kaydet</th>"
        "</tr>"
    )

    for r in rows:
        cls = "matched" if r["status"] == "matched" else "mismatch"
        nice = {
            "matched": "Eşleşti",
            "missing_in_atf": "Sadece BC",
            "missing_in_bc": "Sadece ATF",
        }.get(r["status"], r["status"])

        dt = r["atf_created_at"] or r["bc_created_at"] or ""
        amt = r["bc_amount"] if r["bc_amount"] is not None else r["atf_amount"]

        bc_method = r["bc_payment_method"] or ""
        atf_method = r["atf_payment_method"] or ""

        bc_link = ""
        if bc_method:
            bc_params = {"start": start_date, "end": end_date, "method": bc_method}
            if status_filter:
                bc_params["status"] = status_filter
            bc_link = f"<a href='/panel/withdraw-results?{html.escape(urlencode(bc_params))}'>{html.escape(bc_method)}</a>"

        atf_link = ""
        if atf_method:
            atf_params = {"start": start_date, "end": end_date, "method": atf_method}
            if status_filter:
                atf_params["status"] = status_filter
            atf_link = f"<a href='/panel/withdraw-results?{html.escape(urlencode(atf_params))}'>{html.escape(atf_method)}</a>"

        note_info = note_map.get(int(r["id"]), {})
        reason_type = note_info.get("reason_type", "")
        note_text = note_info.get("note_text", "")
        note_status_text = note_info.get("status_text", "Yeni")

        body.append(f"<tr class='{cls}'>")
        body.append(f"<td>{html.escape(r['business_date'] or '')}</td>")
        body.append(f"<td>{html.escape(nice)}</td>")
        body.append(f"<td>{html.escape(dt)}</td>")
        body.append(f"<td>{html.escape(r['bc_login'] or '')}</td>")
        body.append(f"<td>{html.escape(r['atf_login'] or '')}</td>")
        body.append(f"<td>{amt if amt is not None else ''}</td>")
        body.append(f"<td>{bc_link}</td>")
        body.append(f"<td>{atf_link}</td>")

        if r["status"] == "matched":
            body.append(f"<td>{html.escape(reason_type)}</td>")
            body.append(f"<td>{html.escape(note_text)}</td>")
            body.append(f"<td>{html.escape(note_status_text)}</td>")
            body.append("<td>-</td>")
        else:
            form_id = f"withdraw_note_form_{int(r['id'])}"

            body.append("<td>")
            body.append(
                f"<select name='reason_type' form='{form_id}'>"
                f"<option value='' {'selected' if reason_type == '' else ''}>Seç</option>"
                f"<option value='ATF silindi' {'selected' if reason_type == 'ATF silindi' else ''}>ATF silindi</option>"
                f"<option value='ATF tutar degisti' {'selected' if reason_type == 'ATF tutar degisti' else ''}>ATF tutar değişti</option>"
                f"<option value='ATF yontem degisti' {'selected' if reason_type == 'ATF yontem degisti' else ''}>ATF yöntem değişti</option>"
                f"<option value='ATF login farkli' {'selected' if reason_type == 'ATF login farkli' else ''}>ATF login farklı</option>"
                f"<option value='BC eksik' {'selected' if reason_type == 'BC eksik' else ''}>BC eksik</option>"
                f"<option value='Elle kontrol edildi' {'selected' if reason_type == 'Elle kontrol edildi' else ''}>Elle kontrol edildi</option>"
                f"</select>"
            )
            body.append("</td>")

            body.append(
                f"<td><textarea name='note_text' rows='2' style='width:180px' form='{form_id}'>{html.escape(note_text)}</textarea></td>"
            )

            body.append("<td>")
            body.append(
                f"<select name='note_status_text' form='{form_id}'>"
                f"<option value='Yeni' {'selected' if note_status_text == 'Yeni' else ''}>Yeni</option>"
                f"<option value='İncelendi' {'selected' if note_status_text == 'İncelendi' else ''}>İncelendi</option>"
                f"<option value='Beklemede' {'selected' if note_status_text == 'Beklemede' else ''}>Beklemede</option>"
                f"<option value='Tamamlandı' {'selected' if note_status_text == 'Tamamlandı' else ''}>Tamamlandı</option>"
                f"</select>"
            )
            body.append("</td>")

            body.append("<td>")
            body.append(
                f"<form id='{form_id}' method='post'>"
                f"<input type='hidden' name='note_result_id' value='{int(r['id'])}'>"
                f"<input type='hidden' name='note_business_date' value='{html.escape(r['business_date'] or '')}'>"
                f"<input type='hidden' name='note_match_key' value='{html.escape(r['match_key'] or '')}'>"
                f"<input type='hidden' name='status' value='{html.escape(status_filter)}'>"
                f"<input type='hidden' name='method' value='{html.escape(method_filter)}'>"
                f"<input type='hidden' name='start' value='{html.escape(start_date)}'>"
                f"<input type='hidden' name='end' value='{html.escape(end_date)}'>"
                f"<button type='submit'>Kaydet</button>"
                f"</form>"
            )
            body.append("</td>")

        body.append("</tr>")

    body.append("</table></div>")

    return web.Response(
        text=html_page("Çekim Karşılaştırma", "".join(body)),
        content_type="text/html"
    )


async def panel_deposit_missing(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM reconciliation_results WHERE panel_status='open' AND status!='matched' AND business_date BETWEEN ? AND ? ORDER BY business_date DESC, COALESCE(atf_created_at, bc_created_at) DESC, id DESC",
            (start_date, end_date),
        ).fetchall()
    body = [f"<div class='card'><h2>Eksik Yatırımlar</h2><form method='get' class='inline'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/deposit-missing'>Bugün</a></form><table><tr><th>Tarih</th><th>Durum</th><th>Zaman</th><th>BC Login</th><th>ATF Login</th><th>Tutar</th><th>BC Yöntem</th><th>ATF Yöntem</th></tr>"]
    for r in rows:
        nice = {"missing_in_atf": "Sadece BC", "missing_in_bc": "Sadece ATF"}.get(r["status"], r["status"])
        dt = r["atf_created_at"] or r["bc_created_at"] or ""
        amt = r["bc_amount"] if r["bc_amount"] is not None else r["atf_amount"]
        body.append(f"<tr class='mismatch'><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(nice)}</td><td>{html.escape(dt)}</td><td>{html.escape(r['bc_login'] or '')}</td><td>{html.escape(r['atf_login'] or '')}</td><td>{amt if amt is not None else ''}</td><td>{html.escape(r['bc_payment_method'] or '')}</td><td>{html.escape(r['atf_payment_method'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("Eksik Yatırımlar", "".join(body)), content_type="text/html")


async def panel_withdraw_missing(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT * FROM withdrawal_reconciliation_results WHERE panel_status='open' AND status!='matched' AND business_date BETWEEN ? AND ? ORDER BY business_date DESC, COALESCE(atf_created_at, bc_created_at) DESC, id DESC",
            (start_date, end_date),
        ).fetchall()
    body = [f"<div class='card'><h2>Eksik Çekimler</h2><form method='get' class='inline'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/withdraw-missing'>Bugün</a></form><table><tr><th>Tarih</th><th>Durum</th><th>Zaman</th><th>BC Login</th><th>ATF Login</th><th>Tutar</th><th>BC Yöntem</th><th>ATF Yöntem</th></tr>"]
    for r in rows:
        nice = {"missing_in_atf": "Sadece BC", "missing_in_bc": "Sadece ATF"}.get(r["status"], r["status"])
        dt = r["atf_created_at"] or r["bc_created_at"] or ""
        amt = r["bc_amount"] if r["bc_amount"] is not None else r["atf_amount"]
        body.append(f"<tr class='mismatch'><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(nice)}</td><td>{html.escape(dt)}</td><td>{html.escape(r['bc_login'] or '')}</td><td>{html.escape(r['atf_login'] or '')}</td><td>{amt if amt is not None else ''}</td><td>{html.escape(r['bc_payment_method'] or '')}</td><td>{html.escape(r['atf_payment_method'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("Eksik Çekimler", "".join(body)), content_type="text/html")


async def panel_bc(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    with closing(get_db()) as conn:
        rows = conn.execute("SELECT * FROM bc_transactions WHERE source_kind='deposit' AND business_date BETWEEN ? AND ? ORDER BY business_date DESC, created_at_norm DESC, id DESC", [start_date, end_date]).fetchall()
    body = [f"<div class='card'><h2>BC Yatırımlar</h2><form method='get' class='inline'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/bc'>Bugün</a></form><table><tr><th>Tarih</th><th>Source ID</th><th>Zaman</th><th>Login</th><th>Ad Soyad</th><th>Tutar</th><th>Yöntem</th></tr>"]
    for r in rows:
        body.append(f"<tr><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(r['bc_source_id'] or '')}</td><td>{html.escape(r['created_at_norm'] or '')}</td><td>{html.escape(r['client_login_raw'] or '')}</td><td>{html.escape(r['client_name'] or '')}</td><td>{r['amount_norm']}</td><td>{html.escape(r['payment_system_display'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("BC Yatırımlar", "".join(body)), content_type="text/html")


async def panel_atf(request: web.Request):
    await require_auth(request)
    status_filter = request.query.get("record_status", "aktif")
    start_date, end_date = parse_date_range(request)
    q = "SELECT * FROM atf_transactions WHERE LOWER(type_name) LIKE '%yat%'"
    params = []
    if status_filter:
        q += " AND record_status = ?"
        params.append(status_filter)
    q += " AND business_date BETWEEN ? AND ?"
    params.extend([start_date, end_date])
    q += " ORDER BY business_date DESC, created_at_norm DESC, id DESC"
    with closing(get_db()) as conn:
        rows = conn.execute(q, params).fetchall()
    body = [
        f"<div class='card'><h2>ATF Yatırımlar</h2>"
        f"<div class='chips'>"
        f"<a href='/panel/atf?record_status=aktif&start={html.escape(start_date)}&end={html.escape(end_date)}'>Aktifler</a>"
        f"<a href='/panel/atf?record_status=duzenlendi&start={html.escape(start_date)}&end={html.escape(end_date)}'>Düzenlenenler</a>"
        f"<a href='/panel/atf?record_status=silindi&start={html.escape(start_date)}&end={html.escape(end_date)}'>Silinenler</a>"
        f"<a href='/panel/atf?record_status=&start={html.escape(start_date)}&end={html.escape(end_date)}'>Tümü</a>"
        f"</div>"
        f"<form method='get' class='inline'><input type='hidden' name='record_status' value='{html.escape(status_filter)}'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/atf'>Bugün</a></form>"
        f"<table><tr><th>Tarih</th><th>Source ID</th><th>Zaman</th><th>Login</th><th>Tutar</th><th>Komisyon</th><th>Yöntem</th><th>Kayıt Durumu</th><th>Düzenlenme</th><th>Silinme</th></tr>"
    ]
    for r in rows:
        body.append(f"<tr><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(r['atf_source_id'] or '')}</td><td>{html.escape(r['created_at_norm'] or '')}</td><td>{html.escape(r['client_login_raw'] or '')}</td><td>{r['amount_norm']}</td><td>{r['commission_norm']}</td><td>{html.escape(r['payment_method_display'] or '')}</td><td>{html.escape(r['record_status'] or '')}</td><td>{html.escape(r['modified_at'] or '')}</td><td>{html.escape(r['deleted_at'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("ATF Yatırımlar", "".join(body)), content_type="text/html")


async def panel_atf_changed(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    raise web.HTTPFound(f"/panel/atf?record_status=duzenlendi&start={start_date}&end={end_date}")


async def panel_atf_deleted(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    raise web.HTTPFound(f"/panel/atf?record_status=silindi&start={start_date}&end={end_date}")


async def panel_withdrawals(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    rows = query_method_diff(start_date, end_date, "withdraw")
    body = [f"<div class='card'><h2>Çekim Özet</h2><form method='get' class='inline'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/withdrawals'>Bugün</a></form><table><tr><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th></tr>"]
    for r in rows:
        cls = "mismatch" if abs(float(r["diff_total"] or 0)) > 0.0001 else ""
        body.append(f"<tr class='{cls}'><td>{html.escape(r['method_display'] or '-')}</td><td>{(r['bc_total'] or 0):,.2f} ({r['bc_count'] or 0})</td><td>{(r['atf_total'] or 0):,.2f} ({r['atf_count'] or 0})</td><td>{(r['diff_total'] or 0):,.2f}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("Çekim Özet", "".join(body)), content_type="text/html")


async def panel_bc_withdrawals(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    q = "SELECT * FROM bc_withdrawals WHERE business_date BETWEEN ? AND ? ORDER BY business_date DESC, created_at_norm DESC, id DESC"
    with closing(get_db()) as conn:
        rows = conn.execute(q, [start_date, end_date]).fetchall()
    body = [f"<div class='card'><h2>BC Çekimler (Ödenenler)</h2><form method='get' class='inline'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/bc-withdrawals'>Bugün</a></form><table><tr><th>Tarih</th><th>Request ID</th><th>Ödeme Zamanı</th><th>Talep Zamanı</th><th>Login</th><th>Ad Soyad</th><th>Tutar</th><th>Yöntem</th><th>Durum</th></tr>"]
    for r in rows:
        body.append(f"<tr><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(str(r['request_id'] or ''))}</td><td>{html.escape(r['created_at_norm'] or '')}</td><td>{html.escape(r['request_time_norm'] or '')}</td><td>{html.escape(r['client_login_raw'] or '')}</td><td>{html.escape(r['client_name'] or '')}</td><td>{(r['amount_norm'] or 0):,.2f}</td><td>{html.escape(r['payment_system_display'] or '')}</td><td>{html.escape(r['state_name'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("BC Çekimler", "".join(body)), content_type="text/html")


async def panel_atf_withdrawals(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    status_filter = request.query.get("record_status", "aktif")
    q = "SELECT * FROM atf_withdrawals WHERE business_date BETWEEN ? AND ?"
    params = [start_date, end_date]
    if status_filter:
        q += " AND record_status = ?"
        params.append(status_filter)
    q += " ORDER BY business_date DESC, created_at_norm DESC, id DESC"
    with closing(get_db()) as conn:
        rows = conn.execute(q, params).fetchall()
    body = [
        f"<div class='card'><h2>ATF Çekimler</h2><div class='chips'>"
        f"<a href='/panel/atf-withdrawals?record_status=aktif&start={html.escape(start_date)}&end={html.escape(end_date)}'>Aktifler</a>"
        f"<a href='/panel/atf-withdrawals?record_status=duzenlendi&start={html.escape(start_date)}&end={html.escape(end_date)}'>Düzenlenenler</a>"
        f"<a href='/panel/atf-withdrawals?record_status=silindi&start={html.escape(start_date)}&end={html.escape(end_date)}'>Silinenler</a>"
        f"<a href='/panel/atf-withdrawals?record_status=&start={html.escape(start_date)}&end={html.escape(end_date)}'>Tümü</a></div>"
        f"<form method='get' class='inline'><input type='hidden' name='record_status' value='{html.escape(status_filter)}'><label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'><label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'><button type='submit'>Filtrele</button><a href='/panel/atf-withdrawals'>Bugün</a></form>"
        f"<table><tr><th>Tarih</th><th>Source ID</th><th>Zaman</th><th>Login</th><th>Tutar</th><th>Komisyon</th><th>Yöntem</th><th>Kayıt Durumu</th><th>Düzenlenme</th><th>Silinme</th></tr>"
    ]
    for r in rows:
        body.append(f"<tr><td>{html.escape(r['business_date'] or '')}</td><td>{html.escape(r['atf_source_id'] or '')}</td><td>{html.escape(r['created_at_norm'] or '')}</td><td>{html.escape(r['client_login_raw'] or '')}</td><td>{(r['amount_norm'] or 0):,.2f}</td><td>{(r['commission_norm'] or 0):,.2f}</td><td>{html.escape(r['payment_method_display'] or '')}</td><td>{html.escape(r['record_status'] or '')}</td><td>{html.escape(r['modified_at'] or '')}</td><td>{html.escape(r['deleted_at'] or '')}</td></tr>")
    body.append("</table></div>")
    return web.Response(text=html_page("ATF Çekimler", "".join(body)), content_type="text/html")


async def panel_method_aliases(request: web.Request):
    await require_auth(request)
    msg = ""
    if request.method == "POST":
        data = await request.post()
        bc_name = str(data.get("bc_name", "")).strip()
        atf_name = str(data.get("atf_name", "")).strip()
        if bc_name and atf_name:
            upsert_method_pair_map(bc_name, atf_name)
            await asyncio.to_thread(rebuild_method_normalizations)
            msg = "<div class='card'><b>Yöntemler eşlendi ve eski kayıtlar yeniden normalize edildi.</b></div>"
        else:
            msg = "<div class='card'><b>BC adı ve ATF adı zorunlu.</b></div>"
    with closing(get_db()) as conn:
        pair_rows = conn.execute("SELECT * FROM method_pair_map ORDER BY updated_at DESC, id DESC").fetchall()
        bc_candidates = conn.execute("SELECT DISTINCT payment_system_raw AS name FROM bc_transactions WHERE payment_system_raw != '' ORDER BY 1 ASC").fetchall()
        atf_candidates = conn.execute("SELECT DISTINCT payment_method_raw AS name FROM atf_transactions WHERE payment_method_raw != '' ORDER BY 1 ASC").fetchall()
    body = [msg]
    body.append("<div class='card'><h2>Yöntem Eşleme</h2>")
    body.append("<p><b>Nasıl çalışır?</b> BC'deki adı ve ATF'deki adı yazarsın, sistem ikisini aynı anahtara bağlar.</p>")
    body.append("<form method='post' class='inline'>")
    body.append("<label>BC Adı</label><input type='text' name='bc_name' placeholder='Örn: PayToPayzHavale' style='min-width:260px'>")
    body.append("<label>ATF Adı</label><input type='text' name='atf_name' placeholder='Örn: Pay To Payz - Havale' style='min-width:260px'>")
    body.append("<button type='submit'>Eşle</button></form></div>")
    body.append("<div class='card'><h2>Kayıtlı Eşlemeler</h2><table><tr><th>ID</th><th>BC Adı</th><th>ATF Adı</th><th>Canonical Key</th><th>Görünen Ad</th><th>Güncellendi</th></tr>")
    for r in pair_rows:
        body.append(f"<tr><td>{r['id']}</td><td>{html.escape(r['bc_raw_name'] or '')}</td><td>{html.escape(r['atf_raw_name'] or '')}</td><td>{html.escape(r['canonical_key'] or '')}</td><td>{html.escape(r['display_name'] or '')}</td><td>{html.escape(r['updated_at'] or '')}</td></tr>")
    body.append("</table></div>")
    body.append("<div class='card'><h2>BC Aday Yöntemler</h2><div class='chips'>")
    for r in bc_candidates[:500]:
        body.append(f"<span style='padding:5px 9px;border:1px solid #e5e7eb;border-radius:999px;background:#fff'>{html.escape(r['name'])}</span>")
    body.append("</div></div>")
    body.append("<div class='card'><h2>ATF Aday Yöntemler</h2><div class='chips'>")
    for r in atf_candidates[:500]:
        body.append(f"<span style='padding:5px 9px;border:1px solid #e5e7eb;border-radius:999px;background:#fff'>{html.escape(r['name'])}</span>")
    body.append("</div></div>")
    return web.Response(text=html_page("Yöntem Eşleme", "".join(body)), content_type="text/html")


async def panel_analysis(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    business_date = end_date
    msg = ""
    if request.method == "POST":
        data = await request.post()
        keys = set()
        for field in data.keys():
            if "__" in field:
                prefix, method_key = field.split("__", 1)
                if prefix in {"display", "rate", "myat", "mcek", "mkom", "mnet", "note"}:
                    keys.add(method_key)
        for method_key in sorted(keys):
            method_display = str(data.get(f"display__{method_key}", method_key)).strip() or method_key
            bc_rate = norm_money(data.get(f"rate__{method_key}", "0"))
            manual_deposit = norm_money(data.get(f"myat__{method_key}", "0"))
            manual_withdraw = norm_money(data.get(f"mcek__{method_key}", "0"))
            manual_commission = norm_money(data.get(f"mkom__{method_key}", "0"))
            manual_net = norm_money(data.get(f"mnet__{method_key}", "0"))
            note_text = str(data.get(f"note__{method_key}", "")).strip()
            upsert_method_commission_rate(method_key, method_display, bc_rate)
            upsert_method_analysis_manual(business_date, method_key, method_display, manual_deposit, manual_withdraw, manual_commission, manual_net, note_text)
        msg = "<div class='card'><b>Analiz verileri kaydedildi.</b></div>"
    if request.query.get("sync_report") == "1":
        try:
            day = datetime.strptime(business_date, "%Y-%m-%d").date()
            await asyncio.to_thread(service.atf.login)
            report_rows = await asyncio.to_thread(service.atf.fetch_method_report_for_day, day)
            await asyncio.to_thread(upsert_atf_method_report_rows, business_date, report_rows)
            msg += f"<div class='card'><b>ATF yöntem raporu çekildi:</b> {len(report_rows)} satır</div>"
        except Exception as ex:
            msg += f"<div class='card'><b>ATF rapor senkron hatası:</b> {html.escape(str(ex))}</div>"
    rows = build_analysis_rows(business_date)
    body = [msg]
    body.append(f"<div class='card'><h2>Günlük Analiz</h2><form method='get' class='inline'><label>Tarih</label><input type='date' name='end' value='{html.escape(business_date)}'><button type='submit'>Göster</button><button type='submit' name='sync_report' value='1'>ATF Raporunu Çek</button></form></div>")
    body.append("<form method='post'>")
    body.append(f"<input type='hidden' name='start' value='{html.escape(business_date)}'><input type='hidden' name='end' value='{html.escape(business_date)}'>")
    body.append("<div class='card'><button type='submit'>Kaydet</button></div>")
    body.append("<div class='card'><table><tr><th>Yöntem</th><th>BC Yatırım</th><th>BC Çekim</th><th>BC Kom %</th><th>BC Kom</th><th>ATF Yatırım</th><th>ATF Çekim</th><th>ATF Kom</th><th>ATF Gün Sonu</th><th>Manuel Yat</th><th>Manuel Çek</th><th>Manuel Kom</th><th>Manuel Gün Sonu</th><th>Açıklama</th><th>Durum</th></tr>")
    for r in rows:
        status_txt = build_analysis_status(r)
        status_cls = "matched" if status_txt == "FARK YOK" else "mismatch"
        k = html.escape(r["method_key"])
        d = html.escape(r["method_display"])
        body.append(
            f"<tr class='{status_cls}'>"
            f"<td>{d}<input type='hidden' name='display__{k}' value='{d}'></td>"
            f"<td>{r['bc_deposit']:,.2f}</td>"
            f"<td>{r['bc_withdraw']:,.2f}</td>"
            f"<td><input type='text' name='rate__{k}' value='{r['bc_rate']:,.2f}' style='width:80px'></td>"
            f"<td>{r['bc_commission']:,.2f}</td>"
            f"<td>{r['atf_deposit']:,.2f}</td>"
            f"<td>{r['atf_withdraw']:,.2f}</td>"
            f"<td>{r['atf_commission']:,.2f}</td>"
            f"<td>{r['atf_net']:,.2f}</td>"
            f"<td><input type='text' name='myat__{k}' value='{r['manual_deposit']:,.2f}' style='width:100px'></td>"
            f"<td><input type='text' name='mcek__{k}' value='{r['manual_withdraw']:,.2f}' style='width:100px'></td>"
            f"<td><input type='text' name='mkom__{k}' value='{r['manual_commission']:,.2f}' style='width:100px'></td>"
            f"<td><input type='text' name='mnet__{k}' value='{r['manual_net']:,.2f}' style='width:100px'></td>"
            f"<td><textarea name='note__{k}' rows='2' style='width:220px'>{html.escape(r['note_text'])}</textarea></td>"
            f"<td>{html.escape(status_txt)}</td>"
            f"</tr>"
        )
    body.append("</table></div>")
    body.append("<div class='card'><button type='submit'>Kaydet</button></div>")
    body.append("</form>")
    return web.Response(text=html_page("Günlük Analiz", "".join(body)), content_type="text/html")


async def healthz(request: web.Request):
    return web.Response(text="ok")


async def create_web_app():
    app = web.Application()

    async def start_background_tasks(app):
        app["recon_task"] = asyncio.create_task(background_recon_loop())

    async def cleanup_background_tasks(app):
        task = app.get("recon_task")
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    app.add_routes([
        web.get("/", healthz),
        web.get("/healthz", healthz),

        web.get("/panel", panel_home),

        web.get("/panel/results", panel_results),
        web.post("/panel/results", panel_results),

        web.get("/panel/withdraw-results", panel_withdraw_results),
        web.post("/panel/withdraw-results", panel_withdraw_results),

        web.get("/panel/deposit-missing", panel_deposit_missing),
        web.get("/panel/withdraw-missing", panel_withdraw_missing),

        web.get("/panel/bc", panel_bc),
        web.get("/panel/atf", panel_atf),
        web.get("/panel/atf-changed", panel_atf_changed),
        web.get("/panel/atf-deleted", panel_atf_deleted),

        web.get("/panel/withdrawals", panel_withdrawals),
        web.get("/panel/bc-withdrawals", panel_bc_withdrawals),
        web.get("/panel/atf-withdrawals", panel_atf_withdrawals),

        web.get("/panel/method-aliases", panel_method_aliases),
        web.post("/panel/method-aliases", panel_method_aliases),

        web.get("/panel/analysis", panel_daily_analysis),
        web.post("/panel/analysis", panel_daily_analysis),

        web.get("/panel/daily-analysis", panel_daily_analysis),
        web.post("/panel/daily-analysis", panel_daily_analysis),

        web.get("/panel/method-commissions", panel_method_commissions),
        web.post("/panel/method-commissions", panel_method_commissions),

        web.get("/panel/missing-deposit-methods", panel_missing_deposit_methods),
        web.get("/panel/missing-withdraw-methods", panel_missing_withdraw_methods),
    ])

    return app


def run_web_server():
    async def _run():
        app = await create_web_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        log.info("Web panel listening on :%s", PORT)
        while True:
            await asyncio.sleep(3600)
    asyncio.run(_run())


    def _handle_signal(signum, frame):
        log.info("Signal received: %s", signum)
        stop_flag["stop"] = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            pass

    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()

    log.info("Panel + otomatik senkron çalışıyor. Telegram gerekli değil.")
    while not stop_flag["stop"]:
        time.sleep(2)


# ============================================================
# 21) GÜNLÜK ANALİZ + KOMİSYON + EKSİK YÖNTEM EKRANLARI (TEK AKTİF BLOK)
# ============================================================

def ensure_extended_panel_tables():
    with closing(get_db()) as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mismatch_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_table TEXT NOT NULL,
                result_id INTEGER NOT NULL,
                business_date TEXT,
                match_key TEXT,
                reason_type TEXT DEFAULT '',
                note_text TEXT DEFAULT '',
                status_text TEXT DEFAULT 'Yeni',
                updated_at TEXT,
                UNIQUE(result_table, result_id)
            )
        """)

        # method_commission_rates düzelt
        row = cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='method_commission_rates'
        """).fetchone()

        if not row:
            cur.execute("""
                CREATE TABLE method_commission_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    method_key TEXT NOT NULL,
                    method_display TEXT,
                    deposit_rate REAL DEFAULT 0,
                    withdraw_rate REAL DEFAULT 0,
                    effective_date TEXT NOT NULL,
                    created_at TEXT,
                    updated_at TEXT,
                    UNIQUE(method_key, effective_date)
                )
            """)
        else:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(method_commission_rates)").fetchall()]
            need_rebuild = (
                "effective_date" not in cols
                or "deposit_rate" not in cols
                or "withdraw_rate" not in cols
            )

            if need_rebuild:
                old_name = "method_commission_rates_old_backup"

                cur.execute(f"DROP TABLE IF EXISTS {old_name}")
                cur.execute(f"ALTER TABLE method_commission_rates RENAME TO {old_name}")

                cur.execute("""
                    CREATE TABLE method_commission_rates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        method_key TEXT NOT NULL,
                        method_display TEXT,
                        deposit_rate REAL DEFAULT 0,
                        withdraw_rate REAL DEFAULT 0,
                        effective_date TEXT NOT NULL,
                        created_at TEXT,
                        updated_at TEXT,
                        UNIQUE(method_key, effective_date)
                    )
                """)

                old_rows = cur.execute(f"SELECT * FROM {old_name}").fetchall()
                old_cols = [r[1] for r in cur.execute(f"PRAGMA table_info({old_name})").fetchall()]

                for r in old_rows:
                    rdict = {k: r[k] for k in r.keys()}

                    method_key = str(rdict.get("method_key") or "").strip()
                    method_display = str(rdict.get("method_display") or method_key).strip()

                    deposit_rate = 0.0
                    withdraw_rate = 0.0

                    if "deposit_rate" in old_cols:
                        deposit_rate = float(rdict.get("deposit_rate") or 0)
                    elif "bc_commission_rate" in old_cols:
                        deposit_rate = float(rdict.get("bc_commission_rate") or 0)

                    if "withdraw_rate" in old_cols:
                        withdraw_rate = float(rdict.get("withdraw_rate") or 0)

                    effective_date = str(
                        rdict.get("effective_date")
                        or (str(rdict.get("updated_at") or "")[:10])
                        or datetime.now(TZ).strftime("%Y-%m-%d")
                    ).strip()

                    if not effective_date:
                        effective_date = datetime.now(TZ).strftime("%Y-%m-%d")

                    if method_key:
                        cur.execute("""
                            INSERT OR IGNORE INTO method_commission_rates
                            (method_key, method_display, deposit_rate, withdraw_rate, effective_date, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            method_key,
                            method_display,
                            deposit_rate,
                            withdraw_rate,
                            effective_date,
                            now_iso(),
                            now_iso(),
                        ))

        cur.execute("""
            CREATE TABLE IF NOT EXISTS method_daily_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_date TEXT NOT NULL,
                method_key TEXT NOT NULL,
                method_display TEXT,
                note_text TEXT DEFAULT '',
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(business_date, method_key)
            )
        """)

        atf_cols = [r[1] for r in cur.execute("PRAGMA table_info(atf_method_reports)").fetchall()]
        if "day_end_total" not in atf_cols:
            cur.execute("ALTER TABLE atf_method_reports ADD COLUMN day_end_total REAL DEFAULT 0")

        try:
            cur.execute("""
                UPDATE atf_method_reports
                SET day_end_total = COALESCE(
                    NULLIF(day_end_total, 0),
                    raw_last_total,
                    net_total,
                    0
                )
                WHERE COALESCE(day_end_total, 0) = 0
            """)
        except Exception:
            pass

        conn.commit()


def save_mismatch_note(
    result_table: str,
    result_id: int,
    business_date: str,
    match_key: str,
    reason_type: str,
    note_text: str,
    status_text: str,
):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO mismatch_notes
            (result_table, result_id, business_date, match_key, reason_type, note_text, status_text, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(result_table, result_id) DO UPDATE SET
                business_date=excluded.business_date,
                match_key=excluded.match_key,
                reason_type=excluded.reason_type,
                note_text=excluded.note_text,
                status_text=excluded.status_text,
                updated_at=excluded.updated_at
            """,
            (
                result_table,
                int(result_id),
                business_date or "",
                match_key or "",
                reason_type or "",
                note_text or "",
                status_text or "Yeni",
                now_iso(),
            ),
        )
        conn.commit()


def get_mismatch_note_map(result_table: str, result_ids: list[int]) -> dict:
    if not result_ids:
        return {}

    placeholders = ",".join(["?"] * len(result_ids))

    with closing(get_db()) as conn:
        cur = conn.cursor()
        rows = cur.execute(
            f"""
            SELECT result_id, reason_type, note_text, status_text
            FROM mismatch_notes
            WHERE result_table=?
              AND result_id IN ({placeholders})
            """,
            [result_table, *result_ids],
        ).fetchall()

    out = {}
    for r in rows:
        out[int(r["result_id"])] = {
            "reason_type": r["reason_type"] or "",
            "note_text": r["note_text"] or "",
            "status_text": r["status_text"] or "Yeni",
        }

    return out


def save_method_commission_rate(
    method_key: str,
    method_display: str,
    deposit_rate: float,
    withdraw_rate: float,
    effective_date: str,
):
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        conn.execute("""
            INSERT INTO method_commission_rates
            (method_key, method_display, deposit_rate, withdraw_rate, effective_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(method_key, effective_date) DO UPDATE SET
                method_display=excluded.method_display,
                deposit_rate=excluded.deposit_rate,
                withdraw_rate=excluded.withdraw_rate,
                updated_at=excluded.updated_at
        """, (
            method_key,
            method_display,
            float(deposit_rate or 0),
            float(withdraw_rate or 0),
            effective_date,
            now_iso(),
            now_iso(),
        ))
        conn.commit()


def get_commission_rate_for_date(method_key: str, business_date: str) -> tuple[float, float]:
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        row = conn.execute("""
            SELECT deposit_rate, withdraw_rate
            FROM method_commission_rates
            WHERE method_key=? AND effective_date<=?
            ORDER BY effective_date DESC, id DESC
            LIMIT 1
        """, (method_key, business_date)).fetchone()

        if not row:
            return 0.0, 0.0

        return float(row["deposit_rate"] or 0), float(row["withdraw_rate"] or 0)


def save_method_note(business_date: str, method_key: str, method_display: str, note_text: str):
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        conn.execute("""
            INSERT INTO method_daily_notes
            (business_date, method_key, method_display, note_text, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date, method_key) DO UPDATE SET
                method_display=excluded.method_display,
                note_text=excluded.note_text,
                updated_at=excluded.updated_at
        """, (
            business_date,
            method_key,
            method_display,
            note_text or "",
            now_iso(),
            now_iso(),
        ))
        conn.commit()


def get_method_note(business_date: str, method_key: str) -> str:
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        row = conn.execute("""
            SELECT note_text
            FROM method_daily_notes
            WHERE business_date=? AND method_key=?
            LIMIT 1
        """, (business_date, method_key)).fetchone()

        if not row:
            return ""

        return row["note_text"] or ""


def save_manual_values(
    business_date: str,
    method_key: str,
    method_display: str,
    manual_deposit: float,
    manual_withdraw: float,
    manual_commission: float,
    manual_day_end: float,
):
    with closing(get_db()) as conn:
        conn.execute("""
            INSERT INTO method_analysis_manual
            (business_date, method_key, method_display, manual_deposit, manual_withdraw,
             manual_commission, manual_net, note_text, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date, method_key) DO UPDATE SET
                method_display=excluded.method_display,
                manual_deposit=excluded.manual_deposit,
                manual_withdraw=excluded.manual_withdraw,
                manual_commission=excluded.manual_commission,
                manual_net=excluded.manual_net,
                updated_at=excluded.updated_at
        """, (
            business_date,
            method_key,
            method_display,
            float(manual_deposit or 0),
            float(manual_withdraw or 0),
            float(manual_commission or 0),
            float(manual_day_end or 0),
            "",
            now_iso(),
        ))
        conn.commit()


def parse_rate_input(v) -> float:
    s = str(v or "").strip().replace("%", "").replace(" ", "")
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def fmt_rate_value(v: float) -> str:
    try:
        return f"{float(v or 0):.2f}".replace(".", ",")
    except Exception:
        return "0,00"


def save_manual_values(
    business_date: str,
    method_key: str,
    method_display: str,
    manual_deposit: float,
    manual_withdraw: float,
    manual_commission: float,
    manual_day_end: float,
):
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO method_daily_manual
            (business_date, method_key, method_display, manual_deposit, manual_withdraw, manual_commission, manual_day_end, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date, method_key) DO UPDATE SET
                method_display=excluded.method_display,
                manual_deposit=excluded.manual_deposit,
                manual_withdraw=excluded.manual_withdraw,
                manual_commission=excluded.manual_commission,
                manual_day_end=excluded.manual_day_end,
                updated_at=excluded.updated_at
            """,
            (
                business_date,
                method_key,
                method_display,
                float(manual_deposit or 0),
                float(manual_withdraw or 0),
                float(manual_commission or 0),
                float(manual_day_end or 0),
                now_iso(),
                now_iso(),
            ),
        )
        conn.commit()

def get_commission_rates(method_key: str) -> tuple[float, float]:
    ensure_extended_panel_tables()
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(deposit_rate, bc_commission_rate, 0) AS deposit_rate,
                COALESCE(withdraw_rate, 0) AS withdraw_rate
            FROM method_commission_rates
            WHERE method_key=?
            LIMIT 1
            """,
            (method_key,),
        ).fetchone()

        if not row:
            return 0.0, 0.0

        return float(row["deposit_rate"] or 0), float(row["withdraw_rate"] or 0)


def save_manual_values(
    business_date: str,
    method_key: str,
    method_display: str,
    manual_deposit: float,
    manual_withdraw: float,
    manual_commission: float,
    manual_day_end: float,
    note_text: str,
):
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO method_analysis_manual
            (business_date, method_key, method_display, manual_deposit, manual_withdraw,
             manual_commission, manual_net, note_text, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(business_date, method_key) DO UPDATE SET
                method_display=excluded.method_display,
                manual_deposit=excluded.manual_deposit,
                manual_withdraw=excluded.manual_withdraw,
                manual_commission=excluded.manual_commission,
                manual_net=excluded.manual_net,
                note_text=excluded.note_text,
                updated_at=excluded.updated_at
            """,
            (
                business_date,
                method_key,
                method_display,
                float(manual_deposit or 0),
                float(manual_withdraw or 0),
                float(manual_commission or 0),
                float(manual_day_end or 0),
                note_text or "",
                now_iso(),
            ),
        )
        conn.commit()


def get_manual_values(business_date: str, method_key: str) -> dict:
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT manual_deposit, manual_withdraw, manual_commission, manual_net, note_text
            FROM method_analysis_manual
            WHERE business_date=? AND method_key=?
            LIMIT 1
            """,
            (business_date, method_key),
        ).fetchone()

        if not row:
            return {
                "manual_deposit": 0.0,
                "manual_withdraw": 0.0,
                "manual_commission": 0.0,
                "manual_day_end": 0.0,
                "note_text": "",
            }

        return {
            "manual_deposit": float(row["manual_deposit"] or 0),
            "manual_withdraw": float(row["manual_withdraw"] or 0),
            "manual_commission": float(row["manual_commission"] or 0),
            "manual_day_end": float(row["manual_net"] or 0),
            "note_text": row["note_text"] or "",
        }





def get_all_known_method_pairs() -> list[tuple[str, str]]:
    seen = {}

    with closing(get_db()) as conn:
        for r in conn.execute("SELECT DISTINCT method_key, method_display FROM method_totals").fetchall():
            mk = (r["method_key"] or "").strip()
            md = (r["method_display"] or mk).strip() or mk
            if mk:
                seen[mk] = md

        for r in conn.execute("SELECT DISTINCT method_key, method_display FROM atf_method_reports").fetchall():
            mk = (r["method_key"] or "").strip()
            md = (r["method_display"] or mk).strip() or mk
            if mk and mk not in seen:
                seen[mk] = md

        for r in conn.execute("SELECT DISTINCT method_key, method_display FROM method_commission_rates").fetchall():
            mk = (r["method_key"] or "").strip()
            md = (r["method_display"] or mk).strip() or mk
            if mk and mk not in seen:
                seen[mk] = md

        for r in conn.execute("SELECT DISTINCT method_key, method_display FROM method_analysis_manual").fetchall():
            mk = (r["method_key"] or "").strip()
            md = (r["method_display"] or mk).strip() or mk
            if mk and mk not in seen:
                seen[mk] = md

        for key, label in METHOD_LABELS.items():
            if key not in seen:
                seen[key] = label

    return sorted(seen.items(), key=lambda x: (x[1] or x[0]).lower())


def build_daily_analysis_rows(business_date: str) -> list[dict]:
    ensure_extended_panel_tables()

    with closing(get_db()) as conn:
        bc_dep = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_totals WHERE business_date=? AND source_name='BC' AND flow_type='deposit'",
                (business_date,),
            ).fetchall()
        }
        bc_wdr = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_totals WHERE business_date=? AND source_name='BC' AND flow_type='withdraw'",
                (business_date,),
            ).fetchall()
        }
        atf_dep = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_totals WHERE business_date=? AND source_name='ATF' AND flow_type='deposit'",
                (business_date,),
            ).fetchall()
        }
        atf_wdr = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_totals WHERE business_date=? AND source_name='ATF' AND flow_type='withdraw'",
                (business_date,),
            ).fetchall()
        }
        reports = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM atf_method_reports WHERE business_date=?",
                (business_date,),
            ).fetchall()
        }
        manuals = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_analysis_manual WHERE business_date=?",
                (business_date,),
            ).fetchall()
        }
        rates = {
            r["method_key"]: row_to_dict(r)
            for r in conn.execute(
                "SELECT * FROM method_commission_rates"
            ).fetchall()
        }

    all_keys = sorted(set(bc_dep) | set(bc_wdr) | set(atf_dep) | set(atf_wdr) | set(reports) | set(manuals) | set(rates))

    if not all_keys:
        all_keys = [mk for mk, _ in get_all_known_method_pairs()]

    rows = []
    for key in all_keys:
        display = (
            (bc_dep.get(key) or {}).get("method_display")
            or (bc_wdr.get(key) or {}).get("method_display")
            or (atf_dep.get(key) or {}).get("method_display")
            or (atf_wdr.get(key) or {}).get("method_display")
            or (reports.get(key) or {}).get("method_display")
            or (manuals.get(key) or {}).get("method_display")
            or (rates.get(key) or {}).get("method_display")
            or get_method_display_by_key(key, key)
            or key
        )

        bc_deposit = float((bc_dep.get(key) or {}).get("total_amount") or 0)
        bc_withdraw = float((bc_wdr.get(key) or {}).get("total_amount") or 0)
        atf_deposit = float((atf_dep.get(key) or {}).get("total_amount") or 0)
        atf_withdraw = float((atf_wdr.get(key) or {}).get("total_amount") or 0)

        dep_rate, wdr_rate = get_commission_rates(key)
        bc_dep_comm = round((bc_deposit * dep_rate) / 100.0, 2)
        bc_wdr_comm = round((bc_withdraw * wdr_rate) / 100.0, 2)
        bc_total_comm = round(bc_dep_comm + bc_wdr_comm, 2)

        rep = reports.get(key) or {}
        atf_commission = float(rep.get("commission_total") or 0)
        atf_day_end = float(
            rep.get("day_end_total")
            or rep.get("raw_last_total")
            or rep.get("net_total")
            or 0
        )

        manual = get_manual_values(business_date, key)
        manual_deposit = float(manual["manual_deposit"] or 0)
        manual_withdraw = float(manual["manual_withdraw"] or 0)
        manual_commission = float(manual["manual_commission"] or 0)
        manual_day_end = float(manual["manual_day_end"] or 0)
        note_text = manual["note_text"] or ""

        is_active = any(
            abs(v) > 0.0001 for v in [
                bc_deposit,
                bc_withdraw,
                atf_deposit,
                atf_withdraw,
                atf_commission,
                atf_day_end,
                manual_deposit,
                manual_withdraw,
                manual_commission,
                manual_day_end,
            ]
        ) or bool(note_text.strip())

        rows.append({
            "method_key": key,
            "method_display": display,
            "bc_deposit": bc_deposit,
            "bc_withdraw": bc_withdraw,
            "deposit_rate": dep_rate,
            "withdraw_rate": wdr_rate,
            "bc_deposit_commission": bc_dep_comm,
            "bc_withdraw_commission": bc_wdr_comm,
            "bc_commission_total": bc_total_comm,
            "atf_deposit": atf_deposit,
            "atf_withdraw": atf_withdraw,
            "atf_commission": atf_commission,
            "atf_day_end": atf_day_end,
            "manual_deposit": manual_deposit,
            "manual_withdraw": manual_withdraw,
            "manual_commission": manual_commission,
            "manual_day_end": manual_day_end,
            "note_text": note_text,
            "is_active": is_active,
        })

    rows = [r for r in rows if r["is_active"]]
    rows.sort(
        key=lambda r: (
            -(
                abs(r["bc_deposit"])
                + abs(r["bc_withdraw"])
                + abs(r["atf_deposit"])
                + abs(r["atf_withdraw"])
                + abs(r["atf_commission"])
                + abs(r["atf_day_end"])
            ),
            r["method_display"].lower(),
        )
    )
    return rows


def build_daily_analysis_status(row: dict) -> str:
    msgs = []

    kom_fark = round(float(row["bc_commission_total"]) - float(row["atf_commission"]), 2)
    if abs(kom_fark) > 0.009:
        msgs.append(f"KOM FARK {kom_fark:+,.2f}")

    manual_yat_fark = round(float(row["manual_deposit"]) - float(row["atf_deposit"]), 2)
    if abs(manual_yat_fark) > 0.009:
        msgs.append(f"MANUEL YAT {manual_yat_fark:+,.2f}")

    manual_cek_fark = round(float(row["manual_withdraw"]) - float(row["atf_withdraw"]), 2)
    if abs(manual_cek_fark) > 0.009:
        msgs.append(f"MANUEL ÇEK {manual_cek_fark:+,.2f}")

    manual_kom_fark = round(float(row["manual_commission"]) - float(row["atf_commission"]), 2)
    if abs(manual_kom_fark) > 0.009:
        msgs.append(f"MANUEL KOM {manual_kom_fark:+,.2f}")

    gun_sonu_fark = round(float(row["manual_day_end"]) - float(row["atf_day_end"]), 2)
    if abs(gun_sonu_fark) > 0.009:
        msgs.append(f"GÜN SONU {gun_sonu_fark:+,.2f}")

    if not msgs:
        return "FARK YOK"

    return " | ".join(msgs)


def get_daily_analysis_rows(business_date: str) -> list[dict]:
    rows = []

    with closing(get_db()) as conn:
        bc_dep = {
            r["method_key"]: {
                "total": float(r["total_amount"] or 0),
                "count": int(r["tx_count"] or 0),
                "display": r["method_display"] or r["method_key"],
            }
            for r in conn.execute(
                """
                SELECT method_key, method_display, total_amount, tx_count
                FROM method_totals
                WHERE business_date=? AND source_name='BC' AND flow_type='deposit'
                """,
                (business_date,),
            ).fetchall()
        }

        bc_wdr = {
            r["method_key"]: {
                "total": float(r["total_amount"] or 0),
                "count": int(r["tx_count"] or 0),
                "display": r["method_display"] or r["method_key"],
            }
            for r in conn.execute(
                """
                SELECT method_key, method_display, total_amount, tx_count
                FROM method_totals
                WHERE business_date=? AND source_name='BC' AND flow_type='withdraw'
                """,
                (business_date,),
            ).fetchall()
        }

        atf_dep = {
            r["method_key"]: {
                "total": float(r["total_amount"] or 0),
                "count": int(r["tx_count"] or 0),
                "display": r["method_display"] or r["method_key"],
            }
            for r in conn.execute(
                """
                SELECT method_key, method_display, total_amount, tx_count
                FROM method_totals
                WHERE business_date=? AND source_name='ATF' AND flow_type='deposit'
                """,
                (business_date,),
            ).fetchall()
        }

        atf_wdr = {
            r["method_key"]: {
                "total": float(r["total_amount"] or 0),
                "count": int(r["tx_count"] or 0),
                "display": r["method_display"] or r["method_key"],
            }
            for r in conn.execute(
                """
                SELECT method_key, method_display, total_amount, tx_count
                FROM method_totals
                WHERE business_date=? AND source_name='ATF' AND flow_type='withdraw'
                """,
                (business_date,),
            ).fetchall()
        }

        atf_reports = {
            r["method_key"]: {
                "display": r["method_display"] or r["method_key"],
                "commission_total": float(r["commission_total"] or 0),
                "day_end_total": float(
                    r["day_end_total"]
                    if r["day_end_total"] is not None
                    else (
                        r["net_total"]
                        if r["net_total"] is not None
                        else (r["raw_last_total"] or 0)
                    )
                ),
            }
            for r in conn.execute(
                """
                SELECT
                    method_key,
                    method_display,
                    commission_total,
                    day_end_total,
                    net_total,
                    raw_last_total
                FROM atf_method_reports
                WHERE business_date=?
                """,
                (business_date,),
            ).fetchall()
        }

    all_methods = get_all_known_method_pairs()

    for method_key, method_display in all_methods:
        bc_dep_total = bc_dep.get(method_key, {}).get("total", 0.0)
        bc_wdr_total = bc_wdr.get(method_key, {}).get("total", 0.0)
        atf_dep_total = atf_dep.get(method_key, {}).get("total", 0.0)
        atf_wdr_total = atf_wdr.get(method_key, {}).get("total", 0.0)

        dep_rate, wdr_rate = get_commission_rate_for_date(method_key, business_date)

        bc_commission_total = round(
            (bc_dep_total * dep_rate / 100.0) +
            (bc_wdr_total * wdr_rate / 100.0),
            2
        )

        atf_commission = float(atf_reports.get(method_key, {}).get("commission_total", 0.0))
        atf_day_end = float(atf_reports.get(method_key, {}).get("day_end_total", 0.0))

        manual_vals = get_manual_values(business_date, method_key)
        manual_deposit = float(manual_vals.get("manual_deposit", 0))
        manual_withdraw = float(manual_vals.get("manual_withdraw", 0))
        manual_commission = float(manual_vals.get("manual_commission", 0))
        manual_day_end = float(manual_vals.get("manual_day_end", 0))

        note_text = get_method_note(business_date, method_key)

        has_transaction = any([
            abs(bc_dep_total) > 0.0001,
            abs(bc_wdr_total) > 0.0001,
            abs(atf_dep_total) > 0.0001,
            abs(atf_wdr_total) > 0.0001,
        ])

        has_bc_deposit = abs(bc_dep_total) > 0.0001
        total_volume = bc_dep_total + bc_wdr_total + atf_dep_total + atf_wdr_total

        msgs = []

        bc_vs_atf_comm = round(bc_commission_total - atf_commission, 2)
        if abs(bc_vs_atf_comm) > 0.009:
            msgs.append(f"KOM FARK {bc_vs_atf_comm:+,.2f}")

        manual_vs_atf_dep = round(manual_deposit - atf_dep_total, 2)
        if abs(manual_vs_atf_dep) > 0.009:
            msgs.append(f"MANUEL YAT {manual_vs_atf_dep:+,.2f}")

        manual_vs_atf_wdr = round(manual_withdraw - atf_wdr_total, 2)
        if abs(manual_vs_atf_wdr) > 0.009:
            msgs.append(f"MANUEL ÇEK {manual_vs_atf_wdr:+,.2f}")

        manual_vs_atf_comm = round(manual_commission - atf_commission, 2)
        if abs(manual_vs_atf_comm) > 0.009:
            msgs.append(f"MANUEL KOM {manual_vs_atf_comm:+,.2f}")

        manual_vs_atf_day_end = round(manual_day_end - atf_day_end, 2)
        if abs(manual_vs_atf_day_end) > 0.009:
            msgs.append(f"GÜN SONU {manual_vs_atf_day_end:+,.2f}")

        status_text = "FARK YOK" if not msgs else " | ".join(msgs)

        rows.append({
            "method_key": method_key,
            "method_display": method_display,
            "bc_deposit": bc_dep_total,
            "bc_withdraw": bc_wdr_total,
            "deposit_rate": dep_rate,
            "withdraw_rate": wdr_rate,
            "bc_commission_total": bc_commission_total,
            "atf_deposit": atf_dep_total,
            "atf_withdraw": atf_wdr_total,
            "atf_commission": atf_commission,
            "atf_day_end": atf_day_end,
            "manual_deposit": manual_deposit,
            "manual_withdraw": manual_withdraw,
            "manual_commission": manual_commission,
            "manual_day_end": manual_day_end,
            "note_text": note_text,
            "status_text": status_text,
            "is_active": has_transaction,
            "has_bc_deposit": has_bc_deposit,
            "total_volume": total_volume,
        })

    rows.sort(
        key=lambda r: (
            0 if r["has_bc_deposit"] else 1,
            -float(r["bc_deposit"] or 0),
            -float(r["total_volume"] or 0),
            (r["method_display"] or "").lower(),
        )
    )

    return rows


def get_missing_method_rows(start_date: str, end_date: str, flow_type: str) -> list[dict]:
    sql = """
    WITH bc AS (
      SELECT business_date, method_key, MAX(method_display) AS method_display, SUM(total_amount) AS total_amount, SUM(tx_count) AS tx_count
      FROM method_totals
      WHERE business_date BETWEEN ? AND ? AND source_name='BC' AND flow_type=?
      GROUP BY business_date, method_key
    ),
    atf AS (
      SELECT business_date, method_key, MAX(method_display) AS method_display, SUM(total_amount) AS total_amount, SUM(tx_count) AS tx_count
      FROM method_totals
      WHERE business_date BETWEEN ? AND ? AND source_name='ATF' AND flow_type=?
      GROUP BY business_date, method_key
    ),
    keys AS (
      SELECT business_date, method_key FROM bc
      UNION
      SELECT business_date, method_key FROM atf
    )
    SELECT keys.business_date,
           keys.method_key,
           COALESCE(bc.method_display, atf.method_display, keys.method_key) AS method_display,
           COALESCE(bc.total_amount, 0) AS bc_total,
           COALESCE(atf.total_amount, 0) AS atf_total,
           COALESCE(bc.tx_count, 0) AS bc_count,
           COALESCE(atf.tx_count, 0) AS atf_count,
           COALESCE(bc.total_amount, 0) - COALESCE(atf.total_amount, 0) AS diff_total
    FROM keys
    LEFT JOIN bc ON bc.business_date = keys.business_date AND bc.method_key = keys.method_key
    LEFT JOIN atf ON atf.business_date = keys.business_date AND atf.method_key = keys.method_key
    WHERE ABS(COALESCE(bc.total_amount, 0) - COALESCE(atf.total_amount, 0)) > 0.0001
    ORDER BY keys.business_date DESC, method_display ASC
    """
    with closing(get_db()) as conn:
        return conn.execute(sql, [start_date, end_date, flow_type, start_date, end_date, flow_type]).fetchall()


def html_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang='tr'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{html.escape(title)}</title>
<style>
body{{font-family:Arial,sans-serif;margin:16px;background:#f5f7fb;color:#1f2937}}
a{{color:#2563eb;text-decoration:none}}
.nav{{display:flex;gap:8px;flex-wrap:wrap;align-items:center}}
.nav a{{margin-right:0;font-weight:700;padding:8px 12px;border-radius:10px;background:#f8fafc;border:1px solid #dbe3f0}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:12px 14px;margin-bottom:12px;box-shadow:0 2px 8px rgba(0,0,0,.04)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}}
.stat{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:10px 12px}}
.stat .n{{font-size:19px;font-weight:800;line-height:1.15;margin-top:4px}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{border:1px solid #e5e7eb;padding:8px;font-size:13px;text-align:left;vertical-align:top}}
th{{background:#eef2ff;position:sticky;top:0}}
.matched{{background:#dff3e7}}
.mismatch{{background:#fde2e2}}
.chips{{display:flex;flex-wrap:wrap;gap:6px}}
.chips a{{display:inline-block;margin:0;padding:5px 9px;border-radius:999px;background:#eef2ff;border:1px solid #c7d2fe;font-size:13px;line-height:1.1}}
form.inline{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:10px 0}}
input[type='text'], input[type='date'], textarea{{padding:4px 6px}}
textarea{{font-family:Arial,sans-serif;min-width:180px;min-height:36px}}
</style>
</head>
<body>
<div class='card'><div class='nav'>
<a href='/panel'>Özet</a>
<a href='/panel/results'>Yatırım Karşılaştırma</a>
<a href='/panel/withdraw-results'>Çekim Karşılaştırma</a>
<a href='/panel/deposit-missing'>Eksik Yatırımlar</a>
<a href='/panel/withdraw-missing'>Eksik Çekimler</a>
<a href='/panel/bc'>BC Yatırımlar</a>
<a href='/panel/atf'>ATF Yatırımlar</a>
<a href='/panel/atf-changed'>ATF Düzenlenenler</a>
<a href='/panel/atf-deleted'>ATF Silinenler</a>
<a href='/panel/withdrawals'>Çekim Özet</a>
<a href='/panel/bc-withdrawals'>BC Çekimler</a>
<a href='/panel/atf-withdrawals'>ATF Çekimler</a>
<a href='/panel/method-aliases'>Yöntem Eşleme</a>
<a href='/panel/daily-analysis'>Günlük Analiz</a>
<a href='/panel/method-commissions'>Komisyon Oranları</a>
<a href='/panel/missing-deposit-methods'>Eksik Yatırım Yöntemleri</a>
<a href='/panel/missing-withdraw-methods'>Eksik Çekim Yöntemleri</a>
</div></div>
{body}
</body>
</html>"""


async def panel_daily_analysis(request: web.Request):
    await require_auth(request)
    ensure_extended_panel_tables()

    try:
        business_date = request.query.get("date", datetime.now(TZ).strftime("%Y-%m-%d"))
        msg = ""

        if request.method == "POST":
            data = await request.post()
            business_date = str(data.get("date", business_date)).strip() or business_date

            for method_key, method_display in get_all_known_method_pairs():
                manual_deposit = norm_money(data.get(f"manual_deposit__{method_key}", "0"))
                manual_withdraw = norm_money(data.get(f"manual_withdraw__{method_key}", "0"))
                manual_commission = norm_money(data.get(f"manual_commission__{method_key}", "0"))
                manual_day_end = norm_money(data.get(f"manual_day_end__{method_key}", "0"))
                note_text = str(data.get(f"note__{method_key}", "") or "").strip()

                save_manual_values(
                    business_date,
                    method_key,
                    method_display,
                    manual_deposit,
                    manual_withdraw,
                    manual_commission,
                    manual_day_end,
                    note_text,
                )

                save_method_note(
                    business_date,
                    method_key,
                    method_display,
                    note_text,
                )

            msg = "<div class='card'><b>Günlük analiz kayıtları kaydedildi.</b></div>"

        rows = get_daily_analysis_rows(business_date)

        body = [msg]
        body.append(
            f"<div class='card'><h2>Günlük Analiz</h2>"
            f"<form method='get' class='inline'>"
            f"<label>Tarih</label>"
            f"<input type='date' name='date' value='{html.escape(business_date)}'>"
            f"<button type='submit'>Göster</button>"
            f"<a href='/panel/daily-analysis'>Bugün</a>"
            f"</form></div>"
        )

        body.append(
            f"<form method='post'>"
            f"<input type='hidden' name='date' value='{html.escape(business_date)}'>"
        )

        body.append("<div class='card'><button type='submit'>Kaydet</button></div>")

        body.append(
            "<div class='card'><table>"
            "<tr>"
            "<th>Yöntem</th>"
            "<th>BC Yatırım</th>"
            "<th>BC Çekim</th>"
            "<th>BC Kom</th>"
            "<th>ATF Yatırım</th>"
            "<th>ATF Çekim</th>"
            "<th>ATF Kom</th>"
            "<th>ATF Gün Sonu</th>"
            "<th>Manuel Yat</th>"
            "<th>Manuel Çek</th>"
            "<th>Manuel Kom</th>"
            "<th>Manuel Gün Sonu</th>"
            "<th>Açıklama</th>"
            "<th>Durum</th>"
            "<th>Detay</th>"
            "</tr>"
        )

        for r in rows:
            row_cls = "matched" if r["status_text"] == "FARK YOK" else "mismatch"

            dep_detail_href = (
                f"/panel/results?start={html.escape(business_date)}"
                f"&end={html.escape(business_date)}"
                f"&method={html.escape(r['method_display'])}"
            )

            wdr_detail_href = (
                f"/panel/withdraw-results?start={html.escape(business_date)}"
                f"&end={html.escape(business_date)}"
                f"&method={html.escape(r['method_display'])}"
            )

            body.append(
                f"<tr class='{row_cls}'>"
                f"<td>{html.escape(r['method_display'])}</td>"
                f"<td>{r['bc_deposit']:,.2f}</td>"
                f"<td>{r['bc_withdraw']:,.2f}</td>"
                f"<td>{r['bc_commission_total']:,.2f}</td>"
                f"<td>{r['atf_deposit']:,.2f}</td>"
                f"<td>{r['atf_withdraw']:,.2f}</td>"
                f"<td>{r['atf_commission']:,.2f}</td>"
                f"<td>{r['atf_day_end']:,.2f}</td>"
                f"<td><input type='text' name='manual_deposit__{html.escape(r['method_key'])}' value='{r['manual_deposit']:,.2f}' style='width:80px'></td>"
                f"<td><input type='text' name='manual_withdraw__{html.escape(r['method_key'])}' value='{r['manual_withdraw']:,.2f}' style='width:80px'></td>"
                f"<td><input type='text' name='manual_commission__{html.escape(r['method_key'])}' value='{r['manual_commission']:,.2f}' style='width:80px'></td>"
                f"<td><input type='text' name='manual_day_end__{html.escape(r['method_key'])}' value='{r['manual_day_end']:,.2f}' style='width:80px'></td>"
                f"<td><textarea name='note__{html.escape(r['method_key'])}' rows='2' style='width:180px'>{html.escape(r['note_text'])}</textarea></td>"
                f"<td>{html.escape(r['status_text'])}</td>"
                f"<td><a href='{dep_detail_href}'>Yatırım Detay</a> | <a href='{wdr_detail_href}'>Çekim Detay</a></td>"
                f"</tr>"
            )

        body.append("</table></div>")
        body.append("<div class='card'><button type='submit'>Kaydet</button></div>")
        body.append("</form>")

        return web.Response(
            text=html_page("Günlük Analiz", "".join(body)),
            content_type="text/html"
        )

    except Exception as e:
        log.exception("panel_daily_analysis hata")
        return web.Response(
            text=html_page(
                "Günlük Analiz Hata",
                f"<div class='card'><h2>Günlük Analiz Hatası</h2><pre>{html.escape(str(e))}</pre></div>"
            ),
            content_type="text/html",
            status=500,
        )
        
async def panel_method_commissions(request: web.Request):
    await require_auth(request)
    ensure_extended_panel_tables()

    effective_date = request.query.get("date", datetime.now(TZ).strftime("%Y-%m-%d"))
    msg = ""

    if request.method == "POST":
        data = await request.post()
        effective_date = str(data.get("date", effective_date)).strip() or effective_date

        for method_key, method_display in get_all_known_method_pairs():
            deposit_rate = parse_rate_input(data.get(f"deposit_rate__{method_key}", "0"))
            withdraw_rate = parse_rate_input(data.get(f"withdraw_rate__{method_key}", "0"))

            save_method_commission_rate(
                method_key,
                method_display,
                deposit_rate,
                withdraw_rate,
                effective_date,
            )

        msg = "<div class='card'><b>Komisyon oranları kaydedildi.</b></div>"

    body = [msg]
    body.append(
        f"<div class='card'><h2>Yöntem Komisyon Oranları</h2>"
        f"<form method='get' class='inline'>"
        f"<label>Tarih</label>"
        f"<input type='date' name='date' value='{html.escape(effective_date)}'>"
        f"<button type='submit'>Göster</button>"
        f"</form>"
        f"</div>"
    )

    body.append(f"<form method='post'><input type='hidden' name='date' value='{html.escape(effective_date)}'>")
    body.append("<div class='card'><button type='submit'>Kaydet</button></div>")

    body.append(
        "<div class='card'><table>"
        "<tr>"
        "<th>Ödeme Yöntemi</th>"
        "<th>Yatırım Komisyon</th>"
        "<th>Çekim Komisyon</th>"
        "</tr>"
    )

    for method_key, method_display in get_all_known_method_pairs():
        deposit_rate, withdraw_rate = get_commission_rate_for_date(method_key, effective_date)

        body.append(
            f"<tr>"
            f"<td>{html.escape(method_display)}</td>"
            f"<td><input type='text' name='deposit_rate__{html.escape(method_key)}' value='{html.escape(fmt_rate_value(deposit_rate))}' style='width:90px'></td>"
            f"<td><input type='text' name='withdraw_rate__{html.escape(method_key)}' value='{html.escape(fmt_rate_value(withdraw_rate))}' style='width:90px'></td>"
            f"</tr>"
        )

    body.append("</table></div>")
    body.append("<div class='card'><button type='submit'>Kaydet</button></div>")
    body.append("</form>")

    return web.Response(
        text=html_page("Yöntem Komisyon Oranları", "".join(body)),
        content_type="text/html"
    )


async def panel_missing_deposit_methods(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    rows = get_missing_method_rows(start_date, end_date, "deposit")

    body = [
        f"<div class='card'><h2>Eksik Yatırım Yöntemleri</h2>"
        f"<form method='get' class='inline'>"
        f"<label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'>"
        f"<label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'>"
        f"<button type='submit'>Filtrele</button>"
        f"</form>"
        f"<table><tr><th>Tarih</th><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th><th>Detay</th></tr>"
    ]

    for r in rows:
        href = f"/panel/results?{urlencode({'start': r['business_date'], 'end': r['business_date'], 'method': r['method_display']})}"
        body.append(
            f"<tr class='mismatch'>"
            f"<td>{html.escape(r['business_date'])}</td>"
            f"<td>{html.escape(r['method_display'])}</td>"
            f"<td>{(r['bc_total'] or 0):,.2f} ({r['bc_count'] or 0})</td>"
            f"<td>{(r['atf_total'] or 0):,.2f} ({r['atf_count'] or 0})</td>"
            f"<td>{(r['diff_total'] or 0):,.2f}</td>"
            f"<td><a href='{html.escape(href)}'>Aç</a></td>"
            f"</tr>"
        )

    body.append("</table></div>")
    return web.Response(text=html_page("Eksik Yatırım Yöntemleri", "".join(body)), content_type="text/html")


async def panel_missing_withdraw_methods(request: web.Request):
    await require_auth(request)
    start_date, end_date = parse_date_range(request)
    rows = get_missing_method_rows(start_date, end_date, "withdraw")

    body = [
        f"<div class='card'><h2>Eksik Çekim Yöntemleri</h2>"
        f"<form method='get' class='inline'>"
        f"<label>Başlangıç</label><input type='date' name='start' value='{html.escape(start_date)}'>"
        f"<label>Bitiş</label><input type='date' name='end' value='{html.escape(end_date)}'>"
        f"<button type='submit'>Filtrele</button>"
        f"</form>"
        f"<table><tr><th>Tarih</th><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th><th>Detay</th></tr>"
    ]

    for r in rows:
        href = f"/panel/withdraw-results?{urlencode({'start': r['business_date'], 'end': r['business_date'], 'method': r['method_display']})}"
        body.append(
            f"<tr class='mismatch'>"
            f"<td>{html.escape(r['business_date'])}</td>"
            f"<td>{html.escape(r['method_display'])}</td>"
            f"<td>{(r['bc_total'] or 0):,.2f} ({r['bc_count'] or 0})</td>"
            f"<td>{(r['atf_total'] or 0):,.2f} ({r['atf_count'] or 0})</td>"
            f"<td>{(r['diff_total'] or 0):,.2f}</td>"
            f"<td><a href='{html.escape(href)}'>Aç</a></td>"
            f"</tr>"
        )

    body.append("</table></div>")
    return web.Response(text=html_page("Eksik Çekim Yöntemleri", "".join(body)), content_type="text/html")


async def create_web_app():
    app = web.Application()

    async def start_background_tasks(app):
        app["recon_task"] = asyncio.create_task(background_recon_loop())

    async def cleanup_background_tasks(app):
        task = app.get("recon_task")
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    app.add_routes([
        web.get("/", healthz),
        web.get("/healthz", healthz),
        web.get("/panel", panel_home),

        web.get("/panel/results", panel_results),
        web.post("/panel/results", panel_results),

        web.get("/panel/withdraw-results", panel_withdraw_results),
        web.post("/panel/withdraw-results", panel_withdraw_results),

        web.get("/panel/deposit-missing", panel_deposit_missing),
        web.get("/panel/withdraw-missing", panel_withdraw_missing),
        web.get("/panel/bc", panel_bc),
        web.get("/panel/atf", panel_atf),
        web.get("/panel/atf-changed", panel_atf_changed),
        web.get("/panel/atf-deleted", panel_atf_deleted),
        web.get("/panel/withdrawals", panel_withdrawals),
        web.get("/panel/bc-withdrawals", panel_bc_withdrawals),
        web.get("/panel/atf-withdrawals", panel_atf_withdrawals),

        web.get("/panel/method-aliases", panel_method_aliases),
        web.post("/panel/method-aliases", panel_method_aliases),

        web.get("/panel/method-commissions", panel_method_commissions),
        web.post("/panel/method-commissions", panel_method_commissions),

        web.get("/panel/analysis", panel_daily_analysis),
        web.post("/panel/analysis", panel_daily_analysis),
        web.get("/panel/daily-analysis", panel_daily_analysis),
        web.post("/panel/daily-analysis", panel_daily_analysis),

        web.get("/panel/missing-deposit-methods", panel_missing_deposit_methods),
        web.get("/panel/missing-withdraw-methods", panel_missing_withdraw_methods),
    ])
    return app

def run_forever():
    init_recon_db()
    ensure_extended_panel_tables()
    rebuild_method_normalizations()

    stop_flag = {"stop": False}

    def _handle_signal(signum, frame):
        log.info("Signal received: %s", signum)
        stop_flag["stop"] = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            pass

    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()

    log.info("Panel + otomatik senkron çalışıyor. Telegram gerekli değil.")
    while not stop_flag["stop"]:
        time.sleep(2)


# === YÖNTEM KOMİSYON SİSTEMİ (BC) ===
# Her yöntem için yatırım ve çekim oranı tutulur ve tarih bazlı uygulanır

def ensure_method_commission_tables():
    with closing(get_db()) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS method_commission_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            method_key TEXT,
            method_display TEXT,
            deposit_rate REAL DEFAULT 0,
            withdraw_rate REAL DEFAULT 0,
            effective_date TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """)
        conn.commit()


def get_commission_rate(method_key: str, business_date: str):
    """Belirli gün için geçerli komisyon oranını getirir"""
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT deposit_rate, withdraw_rate
            FROM method_commission_rates
            WHERE method_key=? AND effective_date<=?
            ORDER BY effective_date DESC, id DESC
            LIMIT 1
            """,
            (method_key, business_date),
        ).fetchone()

        if not row:
            return 0.0, 0.0

        return float(row[0] or 0), float(row[1] or 0)


def calculate_bc_commission(method_key: str, business_date: str, bc_deposit: float, bc_withdraw: float):
    dep_rate, wdr_rate = get_commission_rate(method_key, business_date)

    dep_comm = (bc_deposit * dep_rate) / 100.0
    wdr_comm = (bc_withdraw * wdr_rate) / 100.0

    return dep_comm, wdr_comm, dep_comm + wdr_comm


# === GÜNLÜK ANALİZ SIRALAMA ===
# işlem olan yöntemler üstte

def sort_analysis_rows(rows):

    def score(r):
        return (
            r.get("bc_deposit", 0)
            or r.get("bc_withdraw", 0)
            or r.get("atf_deposit", 0)
            or r.get("atf_withdraw", 0)
            or r.get("atf_commission", 0)
        )

    rows.sort(key=lambda r: score(r), reverse=True)
    return rows


# === SADECE İŞLEM OLAN YÖNTEMLER ===

def filter_active_methods(rows):
    out = []

    for r in rows:
        if (
            r.get("bc_deposit", 0) != 0
            or r.get("bc_withdraw", 0) != 0
            or r.get("atf_deposit", 0) != 0
            or r.get("atf_withdraw", 0) != 0
        ):
            out.append(r)

    return out


# === EŞLEŞMEYEN YÖNTEM RAPORU ===

async def panel_method_mismatch(request: web.Request):
    await require_auth(request)

    start_date, end_date = parse_date_range(request)

    sql = """
        SELECT business_date,
               COALESCE(bc_payment_method, atf_payment_method) as method_name,
               SUM(bc_amount) as bc_total,
               SUM(atf_amount) as atf_total
        FROM reconciliation_results
        WHERE business_date BETWEEN ? AND ?
        GROUP BY business_date, method_name
        HAVING ABS(COALESCE(SUM(bc_amount),0) - COALESCE(SUM(atf_amount),0)) > 0.01
        ORDER BY business_date DESC
    """

    with closing(get_db()) as conn:
        rows = conn.execute(sql, (start_date, end_date)).fetchall()

    body = ["<div class='card'><h2>Yöntem Farkları</h2>"]

    body.append("<table>")
    body.append("<tr><th>Tarih</th><th>Yöntem</th><th>BC</th><th>ATF</th><th>Fark</th></tr>")

    for r in rows:

        bc = float(r["bc_total"] or 0)
        atf = float(r["atf_total"] or 0)
        diff = bc - atf

        body.append(
            f"<tr><td>{r['business_date']}</td><td>{r['method_name']}</td><td>{bc:,.2f}</td><td>{atf:,.2f}</td><td>{diff:,.2f}</td></tr>"
        )

    body.append("</table></div>")

    return web.Response(text=html_page("Yöntem Farkları", "".join(body)), content_type="text/html")


if __name__ == "__main__":
    run_forever()

    












