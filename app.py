import os
import csv
from io import StringIO
from urllib.parse import urlencode
from datetime import timedelta
import time
import json
import re
import uuid
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict, deque
from functools import wraps

from dotenv import load_dotenv
from flask import Flask, request, jsonify, abort, render_template_string, make_response
from werkzeug.exceptions import HTTPException

import psycopg2
from psycopg2.extras import RealDictCursor

from assistant_store import AssistantStore
from llm_client import MistralClient
from db_store import DBAssistantStore

APP_BUILD = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()[:7] or "dev"

# Load .env ONCE, early
load_dotenv(override=True)

APP_STARTED_AT = datetime.now(timezone.utc).isoformat()


# ---------- Paths / Store ----------
BASE_DIR = Path(__file__).resolve().parent
ASSISTANTS_DIR = Path(os.getenv("ASSISTANTS_DIR") or (BASE_DIR / "assistants"))
STORE = AssistantStore(base_dir=str(ASSISTANTS_DIR))

# ---------- Flask ----------
app = Flask(__name__)
import logging
from werkzeug.exceptions import HTTPException

DEBUG_MODE = (os.getenv("FLASK_DEBUG") or "").strip().lower() in ("1", "true", "yes", "on")

@app.errorhandler(Exception)
def _handle_any_exception(e):
    # Always log full traceback to server console
    app.logger.exception("Unhandled exception")
    if isinstance(e, HTTPException):
        return jsonify(error=e.name, detail=e.description), (e.code or 500)

    # For local debugging only
    if DEBUG_MODE:
        return jsonify(
            error="server_error",
            type=e.__class__.__name__,
            detail=str(e),
        ), 500

    # Production-safe generic error
    return jsonify(error="server_error"), 500

app.json.ensure_ascii = False

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
db_store = DBAssistantStore(DATABASE_URL) if DATABASE_URL else None
if db_store:
    db_store.init_db()

# ---------------------------
# Finance DB helpers (Postgres)
# ---------------------------
def _finance_conn():
    dsn = (os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        return None

    # Render often needs SSL, local usually not.
    # We'll try SSL first, then fallback.
    try:
        return psycopg2.connect(dsn, sslmode=os.getenv("PGSSLMODE", "require"))
    except Exception:
        return psycopg2.connect(dsn)


def ensure_finance_schema():
    con = _finance_conn()
    if not con:
        return
    with con:
        with con.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS finance_entries (
              id TEXT PRIMARY KEY,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              entry_date DATE NOT NULL,
              property_slug TEXT NOT NULL,
              entry_type TEXT NOT NULL CHECK (entry_type IN ('expense','income')),
              amount NUMERIC(12,2) NOT NULL,
              currency TEXT NOT NULL DEFAULT 'EUR',
              category TEXT,
              label TEXT,
              note TEXT,
              raw_text TEXT
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_finance_entries_date ON finance_entries(entry_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_finance_entries_prop ON finance_entries(property_slug);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_finance_entries_created ON finance_entries(created_at);")

            # merchant memory
            cur.execute("""
            CREATE TABLE IF NOT EXISTS finance_merchant_map (
              token TEXT PRIMARY KEY,
              category TEXT NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
    con.close()


# Ensure finance schema once at startup (if DB configured)
if DATABASE_URL:
    ensure_finance_schema()


def ensure_finance_pending_schema():
    con = _finance_conn()
    if not con:
        return
    with con:
        with con.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS finance_pending (
              public_id TEXT NOT NULL,
              client_id TEXT NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              data TEXT NOT NULL,
              PRIMARY KEY (public_id, client_id)
            );
            """)
    con.close()


def finance_pending_get(public_id: str, client_id: str):
    ensure_finance_pending_schema()
    con = _finance_conn()
    if not con:
        return None
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT data FROM finance_pending WHERE public_id=%s AND client_id=%s", (public_id, client_id))
            row = cur.fetchone()
    con.close()
    if not row:
        return None
    try:
        return json.loads(row["data"])
    except Exception:
        return None


def finance_pending_upsert(public_id: str, client_id: str, data: dict):
    ensure_finance_pending_schema()
    con = _finance_conn()
    if not con:
        return
    payload = json.dumps(data, ensure_ascii=False)
    with con:
        with con.cursor() as cur:
            cur.execute("""
            INSERT INTO finance_pending (public_id, client_id, data, updated_at)
            VALUES (%s,%s,%s,NOW())
            ON CONFLICT (public_id, client_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """, (public_id, client_id, payload))
    con.close()


def finance_pending_clear(public_id: str, client_id: str):
    ensure_finance_pending_schema()
    con = _finance_conn()
    if not con:
        return
    with con:
        with con.cursor() as cur:
            cur.execute("DELETE FROM finance_pending WHERE public_id=%s AND client_id=%s", (public_id, client_id))
    con.close()


def finance_insert(entry: dict):
    con = _finance_conn()
    if not con:
        raise RuntimeError("db_not_configured")

    with con:
        with con.cursor() as cur:
            cur.execute("""
              INSERT INTO finance_entries
              (id, entry_date, property_slug, entry_type, amount, currency, category, label, note, raw_text)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                entry["id"], entry["entry_date"], entry["property_slug"], entry["entry_type"],
                entry["amount"], entry.get("currency", "EUR"),
                entry.get("category"), entry.get("label"), entry.get("note"), entry.get("raw_text")
            ))
    con.close()


def finance_list(
    limit=50,
    property_slug=None,
    entry_type=None,
    date_from=None,
    date_to=None,
    order="DESC",
):
    con = _finance_conn()
    if not con:
        raise RuntimeError("db_not_configured")

    order = (order or "DESC").upper().strip()
    if order not in ("ASC", "DESC"):
        order = "DESC"

    where = []
    args = []
    if property_slug:
        where.append("property_slug=%s"); args.append(property_slug)
    if entry_type:
        where.append("entry_type=%s"); args.append(entry_type)
    if date_from:
        where.append("entry_date>=%s"); args.append(date_from)
    if date_to:
        where.append("entry_date<=%s"); args.append(date_to)

    sql = "SELECT * FROM finance_entries"
    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += f" ORDER BY entry_date {order}, created_at {order}"

    if limit is not None:
        sql += " LIMIT %s"
        args.append(int(limit))

    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    con.close()
    return rows
@app.errorhandler(Exception)
def _json_errors(e):
    app.logger.exception("Unhandled exception")
    if isinstance(e, HTTPException):
        return jsonify(error=e.name, detail=e.description), (e.code or 500)

    # Μη διαρρέεις λεπτομέρειες σε production
    if (os.getenv("FLASK_DEBUG") or "").strip() in ("1", "true", "True"):
        return jsonify(error="server_error", detail=str(e)), 500
    return jsonify(error="server_error"), 500


# ---------------------------
# Assistant helpers
# ---------------------------
def _rec_get(rec, key, default=None):
    if isinstance(rec, dict):
        return rec.get(key, default)
    return getattr(rec, key, default)


def _assistant_id(a):
    if a is None:
        return None
    if isinstance(a, dict):
        return a.get("slug") or a.get("assistant_id") or a.get("id")
    return getattr(a, "slug", None) or getattr(a, "assistant_id", None) or getattr(a, "id", None)


def _assistant_enabled(a) -> bool:
    return bool(_rec_get(a, "enabled", False))


def _assistant_config(a) -> dict:
    cfg = _rec_get(a, "config", None) or _rec_get(a, "config_json", None) or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}
    return cfg if isinstance(cfg, dict) else {}


def require_key_if_needed(cfg: dict):
    cfg = cfg or {}
    if not cfg.get("requires_key"):
        return

    expected = (os.getenv("FINANCE_KEY") or "").strip()
    if not expected:
        abort(500, description="FINANCE_KEY is not configured")

    provided = (request.args.get("k") or request.headers.get("X-FINANCE-KEY") or "").strip()
    if provided != expected:
        abort(401)


def _get_public_assistant(public_id: str):
    # DB-first
    if db_store:
        try:
            rec = db_store.get_by_public_id(public_id)
            if rec is not None:
                return rec
        except Exception:
            app.logger.exception("get_by_public_id failed")

    # Fallback: scan filesystem store (legacy mode)
    for a in STORE.list(enabled_only=False):
        pid = _rec_get(a, "public_id", None)
        is_pub = bool(_rec_get(a, "is_public", False))
        if pid == public_id and is_pub:
            return a

    return None


# ---------------------------
# Admin auth
# ---------------------------
def _is_admin(req) -> bool:
    expected = (os.getenv("ADMIN_API_KEY") or "").strip()
    provided = (req.headers.get("X-ADMIN-KEY") or "").strip()
    return bool(expected) and (provided == expected)


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_admin(request):
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


# ---------------------------
# Rate limiting for public chat
# ---------------------------
RATE = defaultdict(lambda: deque())
WINDOW_SECONDS = int(os.getenv("RL_WINDOW_SECONDS", "60"))
MAX_REQ_PER_WINDOW = int(os.getenv("RL_MAX_REQ", "20"))


def get_client_ip() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "unknown").strip()


def rate_limited():
    ip = get_client_ip()
    now = time.time()
    q = RATE[ip]

    while q and now - q[0] > WINDOW_SECONDS:
        q.popleft()

    if len(q) >= MAX_REQ_PER_WINDOW:
        return jsonify(error="Too many requests, slow down."), 429

    q.append(now)
    return None


# ---------------------------
# Finance parsing (Greek-friendly)
# ---------------------------
_EXPENSE_WORDS = [
    "πλήρωσα", "πληρωσα",
    "εδωσα", "έδωσα",
    "αγόρασα", "αγορασα",
    "ψώνισα", "ψωνισα",
    "χρεώθηκα", "χρεωθηκα",
    "έξοδο", "εξοδο", "expense",
]
_INCOME_WORDS  = [
    "εισέπραξα", "εισπραξα",
    "πήρα", "πηρα",
    "πληρώθηκα", "πληρωθηκα",
    "έλαβα", "ελαβα",
    "μπήκαν", "μπηκαν",
    "έσοδο", "εσοδο", "income",
]

_PROP_MAP = {
    "thessaloniki": ["θεσσαλονικη", "θεσ", "thessaloniki"],
    "vourvourou":   ["βουρβουρου", "σιθωνια", "φαβα", "vourvourou"],
}

_CAT_RULES = [
    ("utilities", ["δεη","ρεύμα","ρευμα","νερό","νερο","ιντερνετ","internet","κοινόχρηστα","κοινοχρηστα"]),
    ("home_maintenance", ["συντηρ","επισκευ","υδραυλ","ηλεκτρολογ","κηπ","garden","service","repair"]),
    ("groceries", [
        "σουπερ", "μάρκετ", "μαρκετ", "τροφ", "supermarket",
        "super market", "mini market", "minimarket",
        "souper", "supper", "super mart"
    ]),
    ("transport", ["parking","παρκινγκ","διόδια","διοδια","βενζιν","fuel","diesel","κτεο","service αυτοκιν","αντιπροσωπ","ασφαλεια αυτοκιν"]),
    ("dining", ["εστιατ","restaurant","ταβερν","taverna","φαγητο","γεύμα"]),
    ("bars_coffee", ["καφε","coffee","μπαρ","bar","ποτο","drink","cocktail"]),
    ("rental_income", ["airbnb","booking","ενοίκ","ενοικ","βραχυχρόν","βραχυχρον"]),
]

_DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
_EUR_AMOUNT_RE = re.compile(r"(?:€\s*)?(\d+(?:[.,]\d{1,2})?)\s*(?:€|eur|euro|ευρώ)?", re.IGNORECASE)


def _norm(s: str) -> str:
    s = (s or "").casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # strip accents


def _detect_property(t: str):
    tl = _norm(t)
    for slug, keys in _PROP_MAP.items():
        for k in keys:
            if _norm(k) in tl:
                return slug
    return None


def _detect_type(t: str):
    tl = _norm(t)
    if any(_norm(w) in tl for w in _INCOME_WORDS):
        return "income"
    if any(_norm(w) in tl for w in _EXPENSE_WORDS):
        return "expense"
    return None


_LABEL_NOISE = {
    "θεσσαλονικη","thessaloniki",
    "βουρβουρου","vourvourou",
    "πληρωσα","πλήρωσα",
    "εισπραξα","εισέπραξα",
    "εξοδο","έξοδο","εσοδο","έσοδο",
}

_NUM_ONLY_RE = re.compile(r"^\s*\d+(?:[.,]\d{1,2})?\s*$")

def _label_candidate(text: str):
    t = (text or "").strip()
    if not t:
        return None
    tl = _norm(t)
    if tl in _LABEL_NOISE:
        return None
    if _NUM_ONLY_RE.match(t):
        return None
    # Αν είναι απλά μια λέξη τύπου “Θεσσαλονίκη” μην την κάνεις label
    if len(t.split()) == 1 and tl in _PROP_MAP.get("thessaloniki", []) + _PROP_MAP.get("vourvourou", []):
        return None
    return t


def _detect_date(t: str):
    m = _DATE_RE.search(t)
    if not m:
        return date.today().isoformat()
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100:
        y += 2000
    return date(y, mo, d).isoformat()


def _detect_amount(t: str):
    # ignore date parts so we don't parse 05/01/2026 as 05 €
    tmp = _DATE_RE.sub(" ", t)
    candidates = [m.group(1) for m in _EUR_AMOUNT_RE.finditer(tmp)]
    if not candidates:
        return None
    raw = candidates[-1].replace(",", ".")
    try:
        return round(float(raw), 2)
    except Exception:
        return None


def _detect_category(t: str):
    tl = _norm(t)
    for cat, keys in _CAT_RULES:
        if any(_norm(k) in tl for k in keys):
            return cat
    return "uncategorized"


def parse_finance_fields(text: str) -> dict:
    return {
        "entry_type": _detect_type(text),
        "property_slug": _detect_property(text),
        "amount": _detect_amount(text),
        "entry_date": _detect_date(text),
        "category": _detect_category(text),
        "label": _label_candidate(text),
        "raw_text": (text or "").strip(),
    }


def missing_fields(state: dict):
    missing = []
    if not state.get("entry_type"):
        missing.append("type")
    if not state.get("property_slug"):
        missing.append("property")
    if state.get("amount") is None:
        missing.append("amount")
    return missing


def looks_like_new_entry(fields: dict) -> bool:
    has_amount = fields.get("amount") is not None
    has_prop = bool(fields.get("property_slug"))
    has_type = bool(fields.get("entry_type"))
    return has_amount and (has_prop or has_type)


def merchant_map_set(token: str, category: str):
    con = _finance_conn()
    if not con:
        raise RuntimeError("db_not_configured")
    token = (token or "").strip()
    category = (category or "").strip()
    with con:
        with con.cursor() as cur:
            cur.execute("""
              INSERT INTO finance_merchant_map(token, category, updated_at)
              VALUES (%s, %s, NOW())
              ON CONFLICT(token)
              DO UPDATE SET category=EXCLUDED.category, updated_at=NOW()
            """, (token, category))
    con.close()


def merchant_map_guess_category(text: str):
    """Very simple: if any stored token appears in text, return its category."""
    con = _finance_conn()
    if not con:
        return None
    tl = _norm(text)
    with con:
        with con.cursor() as cur:
            cur.execute("SELECT token, category FROM finance_merchant_map")
            rows = cur.fetchall()
    con.close()

    for token, cat in rows:
        if _norm(token) in tl:
            return cat
    return None


def _is_greeting(msg: str) -> bool:
    t = _norm(msg)
    return t in ("γεια", "γεια σου", "καλημερα", "καλημέρα", "καλησπερα", "καληνυχτα", "hello", "hi")


def _get_client_id_for_state(public_id: str) -> str:
    # UI already sends X-CLIENT-ID. If missing, fallback to IP.
    return (request.headers.get("X-CLIENT-ID") or "").strip() or get_client_ip()


def _finance_auth_and_get_clerk(public_id: str):
    a = _get_public_assistant(public_id)
    if a is None:
        return None, (jsonify(error="assistant_not_found"), 404)
    if not _assistant_enabled(a):
        return None, (jsonify(error="assistant_disabled"), 404)

    cfg = _assistant_config(a)
    try:
        require_key_if_needed(cfg)
    except HTTPException as e:
        return None, (jsonify(error="unauthorized", code=e.code), (e.code or 401))

    slug = str(_assistant_id(a) or "")
    if slug != "finance_clerk":
        return None, (jsonify(error="not_finance_clerk", slug=slug), 404)

    return a, None


@app.after_request
def force_utf8(resp):
    if resp.mimetype == "application/json":
        resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


# ---------------------------
# Bootstrap: import assistants from filesystem if DB empty
# ---------------------------
def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def bootstrap_db_from_filesystem() -> None:
    if not db_store:
        return

    try:
        existing = db_store.list_admin()
    except Exception:
        return

    if existing:
        return

    base = str(ASSISTANTS_DIR)
    if not os.path.isdir(base):
        return

    for slug in os.listdir(base):
        folder = os.path.join(base, slug)
        if not os.path.isdir(folder):
            continue

        cfg_path = os.path.join(folder, "config.json")
        prompt_path = os.path.join(folder, "prompt.md")
        know_path = os.path.join(folder, "knowledge.md")

        if not (os.path.isfile(cfg_path) and os.path.isfile(prompt_path) and os.path.isfile(know_path)):
            continue

        try:
            config = _read_json(cfg_path)
            prompt = _read_text(prompt_path)
            knowledge = _read_text(know_path)
            name = (config.get("name") or config.get("title") or slug)

            db_store.create_assistant(
                slug=slug,
                name=name,
                config=config,
                prompt=prompt,
                knowledge=knowledge,
                created_by="bootstrap",
            )
        except Exception:
            continue


bootstrap_db_from_filesystem()


# ---------------------------
# Public page HTML
# ---------------------------
PUBLIC_CHAT_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title or "Assistant" }}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 18px; background: #fff; color: #111; }
    .wrap { max-width: 900px; margin: 0 auto; }
    .muted { color:#666; font-size: 0.95rem; }
    .card { border:1px solid #e5e5e5; border-radius: 12px; padding: 12px; margin: 12px 0; }
    .row { display:flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    input[type="password"], input[type="text"], textarea {
      border:1px solid #ccc; border-radius: 10px; padding: 10px; font-size: 1rem;
    }
    textarea { width: 100%; min-height: 90px; resize: vertical; }
    button {
      border:1px solid #ccc; background:#f7f7f7; border-radius: 10px;
      padding: 10px 14px; cursor:pointer; font-size: 1rem;
    }
    button:hover { background:#efefef; }
    button.primary { background:#111; color:#fff; border-color:#111; }
    button.primary:hover { background:#000; }
    .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; background:#f1f1f1; font-size: 0.9rem; }
    #log { white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.95rem; }
    .err { color:#b00020; }
    .ok { color:#0a7b34; }
    .line.user { font-weight: 600; }
    .line.bot  { margin-left: 14px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h2>{{ title or "Assistant" }}</h2>
    <div class="muted">Key is stored locally in your browser (one-time per device).</div>

    <div class="card" id="keyBox" style="{{ '' if requires_key else 'display:none;' }}">
      <div class="row">
        <input id="apiKey" type="password" placeholder="Paste finance key once" autocomplete="off" style="min-width: 280px;">
        <button id="toggleKey" type="button">Show</button>
        <button id="saveKey" class="primary" type="button">Save</button>
        <button id="forgetKey" type="button">Forget</button>
        <span id="keyStatus" class="pill"></span>
      </div>
      <div class="muted" style="margin-top:8px;">
        Tip: If you previously used ?k=... in the URL, this page will auto-save it once and remove it from the address bar.
      </div>
    </div>

    <div class="card">
      <div class="row" style="justify-content: space-between;">
        <div>
          <strong>Finance</strong>
          <div class="muted">Example: “Πλήρωσα κήπο Βουρβουρού 60€ 2/1/2026”</div>
        </div>
        <div class="row">
          <button id="undoLast" type="button">Undo last</button>
          <button id="deleteById" type="button">Delete by ID</button>
          <button id="downloadCsv" type="button">Download CSV</button>
        </div>
      </div>

      <div id="log" style="margin-top:12px; padding:12px; background:#fafafa; border-radius: 12px; border:1px solid #eee; min-height: 120px;"></div>

      <div style="margin-top:12px;">
        <textarea id="msg" placeholder="Type a message..."></textarea>
      </div>

      <div class="row" style="margin-top:10px;">
        <button id="send" class="primary" type="button">Send</button>
        <span id="status" class="muted"></span>
      </div>
    </div>
  </div>

<script>
(function () {
  const publicId = "{{ public_id }}";
  const assistantSlug = "{{ assistant_slug }}";
  const requiresKey = {{ "true" if requires_key else "false" }};

  const STORAGE_KEY = `finance_key:${publicId}`;
  const CLIENT_KEY  = `client_id:${publicId}`;

  const elKey = document.getElementById("apiKey");
  const elToggle = document.getElementById("toggleKey");
  const elSave = document.getElementById("saveKey");
  const elForget = document.getElementById("forgetKey");
  const elKeyStatus = document.getElementById("keyStatus");

  const elLog = document.getElementById("log");
  const elMsg = document.getElementById("msg");
  const elSend = document.getElementById("send");
  const elStatus = document.getElementById("status");
  const elDownload = document.getElementById("downloadCsv");
  const elUndo = document.getElementById("undoLast");
  const elDelete = document.getElementById("deleteById");

  function setStatus(text, isError=false) {
    elStatus.textContent = text || "";
    elStatus.className = isError ? "muted err" : "muted";
  }

  function appendLog(text, cls) {
    const line = document.createElement("div");
    line.className = "line" + (cls ? (" " + cls) : "");
    line.textContent = text;
    elLog.appendChild(line);
    elLog.scrollTop = elLog.scrollHeight;
  }

  function safeGet(key) {
    try { return (localStorage.getItem(key) || "").trim(); }
    catch (e) { return ""; }
  }
  function safeSet(key, val) {
    try { localStorage.setItem(key, (val || "").trim()); return true; }
    catch (e) {
      console.error("localStorage set failed:", e);
      setStatus("Browser storage blocked. Allow site data / disable strict privacy mode.", true);
      return false;
    }
  }
  function safeRemove(key) {
    try { localStorage.removeItem(key); return true; }
    catch (e) { return false; }
  }

  function uuidLike() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === "x" ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  function getOrCreateClientId() {
    let id = safeGet(CLIENT_KEY);
    if (!id) {
      id = uuidLike();
      safeSet(CLIENT_KEY, id);
    }
    return id;
  }

  function getSavedKey() { return safeGet(STORAGE_KEY); }
  function setSavedKey(k) { return safeSet(STORAGE_KEY, k); }
  function clearSavedKey() { return safeRemove(STORAGE_KEY); }

  function updateKeyUI() {
    if (!requiresKey) return;
    const has = !!getSavedKey();
    elKeyStatus.textContent = has ? "Key saved" : "No key saved";
    elKeyStatus.className = has ? "pill ok" : "pill err";
  }

  // Migration helper: if URL has ?k=..., save it once then remove from URL
  try {
    const url = new URL(window.location.href);
    const k = (url.searchParams.get("k") || "").trim();
    if (k && !getSavedKey()) setSavedKey(k);
    if (url.searchParams.has("k")) {
      url.searchParams.delete("k");
      window.history.replaceState({}, "", url.toString());
    }
  } catch (e) {}

  if (requiresKey) {
    elToggle.addEventListener("click", () => {
      if (elKey.type === "password") {
        elKey.type = "text";
        elToggle.textContent = "Hide";
      } else {
        elKey.type = "password";
        elToggle.textContent = "Show";
      }
    });

    elSave.addEventListener("click", () => {
      const k = (elKey.value || "").trim();
      if (!k) { alert("Paste the finance key first."); return; }
      if (!setSavedKey(k)) return;
      elKey.value = "";
      elKey.type = "password";
      elToggle.textContent = "Show";
      updateKeyUI();
      setStatus("Key saved.");
    });

    elForget.addEventListener("click", () => {
      clearSavedKey();
      updateKeyUI();
      setStatus("Key removed.");
    });
  }

  async function downloadWithAuth(path, filenamePrefix) {
    const clientId = getOrCreateClientId();
    const key = requiresKey ? getSavedKey() : "";

    if (requiresKey && !key) {
      throw new Error("No finance key saved.");
    }

    const resp = await fetch(path, {
      method: "GET",
      headers: {
        ...(requiresKey ? { "X-FINANCE-KEY": key } : {}),
        "X-CLIENT-ID": clientId
      }
    });

    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(t || "download_failed");
    }

    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    const now = new Date();
    const stamp = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
    a.href = url;
    a.download = `${filenamePrefix}_${stamp}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function postChat(message) {
    const clientId = getOrCreateClientId();
    const key = requiresKey ? getSavedKey() : "";

    if (requiresKey && !key) {
      alert("No finance key saved. Paste it once and hit Save.");
      return;
    }

    setStatus("Sending...");
    elSend.disabled = true;

    try {
      const resp = await fetch(`/p/${publicId}/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(requiresKey ? { "X-FINANCE-KEY": key } : {}),
          "X-CLIENT-ID": clientId
        },
        body: JSON.stringify({ assistant_id: assistantSlug, message })
      });

      const text = await resp.text();
      let data = null;
      try { data = JSON.parse(text); } catch (e) {}

      appendLog(message, "user");

      if (!resp.ok) {
        const errMsg = (data && (data.error || data.message || data.detail)) ? (data.error || data.message || data.detail) : text;
        setStatus("Error.", true);
        appendLog("ERROR: " + errMsg, "err");
        return;
      }

      const botText = data && (data.reply || data.answer || data.message || data.error);
      appendLog(botText || "(empty)", "bot");

      // Auto-download report CSV if backend provided a download_url
      if (data && data.download_url) {
        try {
          setStatus("Downloading report CSV...");
          await downloadWithAuth(data.download_url, `report_${publicId}`);
          appendLog("Report CSV downloaded.", "bot");
        } catch (e) {
          appendLog("ERROR: " + (e && e.message ? e.message : String(e)), "err");
          setStatus("Report download failed.", true);
          return;
        }
      }

      setStatus("Sent.");
    } catch (e) {
      setStatus("Network error.", true);
      appendLog("ERROR: " + (e && e.message ? e.message : String(e)), "err");
    } finally {
      elSend.disabled = false;
    }
  }

  async function downloadCsv() {
    const key = requiresKey ? getSavedKey() : "";
    if (requiresKey && !key) { alert("No finance key saved. Paste it once and hit Save."); return; }

    try {
      setStatus("Downloading CSV...");
      await downloadWithAuth(`/p/${publicId}/export.csv`, `finance_${publicId}`);
      setStatus("CSV downloaded.");
    } catch (e) {
      setStatus("CSV download failed.", true);
      appendLog("ERROR: " + (e && e.message ? e.message : String(e)), "err");
    }
  }

  async function undoLast() {
    const clientId = getOrCreateClientId();
    const key = requiresKey ? getSavedKey() : "";
    if (requiresKey && !key) { alert("No finance key saved."); return; }

    const prop = (prompt("Property slug? (vourvourou / thessaloniki). Leave empty for global undo.") || "").trim();
    if (!confirm("Undo last entry" + (prop ? (" for " + prop) : "") + "?")) return;

    setStatus("Undoing...");
    try {
      const resp = await fetch(`/p/${publicId}/finance/undo_last`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(requiresKey ? { "X-FINANCE-KEY": key } : {}),
          "X-CLIENT-ID": clientId
        },
        body: JSON.stringify({ confirm: true, property_slug: prop || null })
      });
      const text = await resp.text();
      let data = null;
      try { data = JSON.parse(text); } catch (e) {}

      if (!resp.ok) {
        setStatus("Undo failed.", true);
        appendLog("ERROR: " + (data && (data.error || data.detail) ? (data.error || data.detail) : text), "err");
        return;
      }

      appendLog("Undone: " + (data.undone_id || data.message || "ok"), "bot");
      setStatus("Undone.");
    } catch (e) {
      setStatus("Network error.", true);
      appendLog("ERROR: " + (e && e.message ? e.message : String(e)), "err");
    }
  }

  async function deleteById() {
    const clientId = getOrCreateClientId();
    const key = requiresKey ? getSavedKey() : "";
    if (requiresKey && !key) { alert("No finance key saved."); return; }

    const id = (prompt("Paste entry id to delete:") || "").trim();
    if (!id) return;
    if (!confirm("Delete entry id:\n" + id + "\n\nThis cannot be undone.")) return;

    setStatus("Deleting...");
    try {
      const resp = await fetch(`/p/${publicId}/finance/delete`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(requiresKey ? { "X-FINANCE-KEY": key } : {}),
          "X-CLIENT-ID": clientId
        },
        body: JSON.stringify({ confirm: true, id })
      });
      const text = await resp.text();
      let data = null;
      try { data = JSON.parse(text); } catch (e) {}

      if (!resp.ok) {
        setStatus("Delete failed.", true);
        appendLog("ERROR: " + (data && (data.error || data.detail) ? (data.error || data.detail) : text), "err");
        return;
      }

      appendLog("Deleted: " + (data.deleted_id || "ok"), "bot");
      setStatus("Deleted.");
    } catch (e) {
      setStatus("Network error.", true);
      appendLog("ERROR: " + (e && e.message ? e.message : String(e)), "err");
    }
  }

  elSend.addEventListener("click", () => {
    const m = (elMsg.value || "").trim();
    if (!m) return;
    elMsg.value = "";
    postChat(m);
  });

  // Enter to send, Shift+Enter for newline
  elMsg.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      elSend.click();
    }
  });

  elDownload.addEventListener("click", downloadCsv);
  elUndo.addEventListener("click", undoLast);
  elDelete.addEventListener("click", deleteById);

  updateKeyUI();
  getOrCreateClientId();
  appendLog("Ready.", "muted");
})();
</script>


# ---------------------------
# Routes
# ---------------------------
@app.get("/health")
def health():
    assistants = STORE.list(enabled_only=False)
    return jsonify(
    ok=True,
    build=APP_BUILD,
    started_at=APP_STARTED_AT,
    assistants_dir=str(ASSISTANTS_DIR),
    assistants_count=len(assistants),
    db_enabled=bool(db_store),
)


@app.post("/admin/reset_finance")
@admin_required
def admin_reset_finance():
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL not set"}), 500

    try:
        with psycopg2.connect(DATABASE_URL, sslmode=os.getenv("PGSSLMODE", "require")) as con:
            with con.cursor() as cur:
                cur.execute("DELETE FROM finance_entries")
                deleted = cur.rowcount
            con.commit()
        return jsonify({"ok": True, "deleted": deleted}), 200
    except Exception as e:
        app.logger.exception("admin_reset_finance failed")
        return jsonify({"ok": False, "error": str(e), "type": type(e).__name__}), 500


@app.get("/assistants")
@admin_required
def list_assistants():
    if not db_store:
        return jsonify({"error": "db_not_configured"}), 500
    return jsonify({"assistants": db_store.list_admin()})


@app.post("/reload")
@admin_required
def reload_assistants():
    try:
        if not db_store:
            return jsonify(ok=False, error="db_not_configured"), 500
        seeded = db_store.seed_from_filesystem(str(ASSISTANTS_DIR))
        return jsonify(ok=True, seeded=seeded, assistants=db_store.list_admin())
    except Exception as e:
        app.logger.exception("reload failed")
        return jsonify(ok=False, error=str(e), type=type(e).__name__), 500


@app.post("/admin/assistants/<assistant_id>/publish")
@admin_required
def admin_publish(assistant_id: str):
    if not db_store:
        return jsonify({"error": "db_not_configured"}), 500
    try:
        pid = db_store.publish(assistant_id)
        return jsonify({"public_id": pid})
    except ValueError as e:
        if str(e) == "assistant_not_found":
            return jsonify({"error": "assistant_not_found", "assistant_id": assistant_id}), 404
        return jsonify({"error": "value_error", "detail": str(e)}), 400
    except Exception as e:
        app.logger.exception("publish failed")
        return jsonify({"error": "publish_failed", "type": type(e).__name__, "detail": str(e)}), 500


@app.post("/admin/assistants/<assistant_id>/unpublish")
@admin_required
def admin_unpublish(assistant_id: str):
    if not db_store:
        return jsonify({"error": "db_not_configured"}), 500
    try:
        db_store.unpublish(assistant_id)
        return jsonify({"ok": True})
    except Exception as e:
        app.logger.exception("unpublish failed")
        return jsonify({"error": "unpublish_failed", "type": type(e).__name__, "detail": str(e)}), 500


@app.post("/admin/assistants/<assistant_id>/rotate_public_id")
@admin_required
def admin_rotate_public_id(assistant_id: str):
    if not db_store:
        return jsonify({"error": "db_not_configured"}), 500
    try:
        pid = db_store.rotate_public_id(assistant_id)
        return jsonify({"public_id": pid})
    except Exception as e:
        app.logger.exception("rotate_public_id failed")
        return jsonify({"error": "rotate_failed", "type": type(e).__name__, "detail": str(e)}), 500


@app.get("/p/<public_id>")
def public_page(public_id):
    a = _get_public_assistant(public_id)
    if a is None or not _assistant_enabled(a):
        abort(404)

    resp = make_response(render_template_string(
        PUBLIC_CHAT_HTML,
        public_id=public_id,
        title="Finance Clerk",
        assistant_slug="finance_clerk",
        requires_key=bool(_assistant_config(a).get("requires_key"))
    ))

    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _run_assistant(rec, message: str) -> str:
    cfg = _assistant_config(rec)

    prompt = (_rec_get(rec, "prompt", "") or "").strip()
    knowledge = (_rec_get(rec, "knowledge", "") or "").strip()

    system = prompt
    if knowledge:
        system = (system + "\n\n" if system else "") + "### Knowledge\n" + knowledge
    if not system:
        system = "You are a helpful assistant."

    model = cfg.get("model") or _rec_get(rec, "model", None) or "mistral-large-latest"
    temperature = cfg.get("temperature", _rec_get(rec, "temperature", 0.2))
    max_tokens = cfg.get("max_tokens", _rec_get(rec, "max_tokens", 600))

    try:
        temperature = float(temperature)
    except Exception:
        temperature = 0.2
    try:
        max_tokens = int(max_tokens)
    except Exception:
        max_tokens = 600

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": message},
    ]

    client = MistralClient()
    return client.chat(model=model, messages=messages, temperature=temperature, max_tokens=max_tokens)


def _ask_next_missing(missing: list) -> str:
    if not missing:
        return ""
    m = missing[0]
    if m == "amount":
        return "Λείπει το ποσό. Πες μου πόσο ήταν (π.χ. 35€)."
    if m == "property":
        return "Λείπει το ακίνητο. Είναι Θεσσαλονίκη ή Βουρβουρού;"
    if m == "type":
        return "Είναι έξοδο ή έσοδο; (π.χ. “Πλήρωσα …” ή “Εισέπραξα …”)."
    return "Λείπουν στοιχεία. Συνέχισε με ποσό/ακίνητο/τύπο."
import calendar
from urllib.parse import urlencode

REPORT_PREFIXES = ("report", "αναφορά", "αναφορα")

def _strip_accents(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = _strip_accents(s)
    s = re.sub(r"\s+", " ", s)
    return s

_PROPERTY_ALIASES = {
    "thessaloniki": "thessaloniki",
    "thessaloniki ": "thessaloniki",
    "θεσσαλονικη": "thessaloniki",
    "vourvourou": "vourvourou",
    "βουρβουρου": "vourvourou",
}

def _detect_property_slug(msg: str) -> str | None:
    m = _norm(msg)
    for k, v in _PROPERTY_ALIASES.items():
        if k in m:
            return v
    # last resort: accept slugs if user typed them
    for slug in ("thessaloniki", "vourvourou"):
        if re.search(rf"\b{slug}\b", m):
            return slug
    return None

def _detect_entry_type(msg: str) -> str | None:
    m = _norm(msg)
    if re.search(r"\b(expense|expenses)\b", m) or "εξοδ" in m:
        return "expense"
    if re.search(r"\b(income)\b", m) or "εσοδ" in m:
        return "income"
    return None

def _parse_date_token(tok: str) -> str | None:
    tok = (tok or "").strip()
    if not tok:
        return None

    # YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", tok):
        return tok

    # D/M/YY or D/M/YYYY
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2}|\d{4})", tok)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return date(y, mo, d).isoformat()
        except Exception:
            return None

    return None

def _month_range(ym: str) -> tuple[str, str] | None:
    # ym: YYYY-MM
    if not re.fullmatch(r"\d{4}-\d{2}", ym or ""):
        return None
    y = int(ym[:4]); m = int(ym[5:7])
    last = calendar.monthrange(y, m)[1]
    return (date(y, m, 1).isoformat(), date(y, m, last).isoformat())

def _parse_report_request(message: str) -> dict | None:
    raw = (message or "").strip()
    if not raw:
        return None

    low = _norm(raw)
    if not any(low.startswith(p) for p in REPORT_PREFIXES):
        return None

    entry_type = _detect_entry_type(raw)
    property_slug = _detect_property_slug(raw)

    # date range detection
    date_from = None
    date_to = None

    # month commands: "report month" / "αναφορά μήνα" + optional YYYY-MM
    if (" month" in low) or (" μηνα" in low) or (" μηνας" in low) or low.startswith("report month") or low.startswith("αναφορα μηνα") or low.startswith("αναφορα μηνας"):
        m = re.search(r"\b(\d{4}-\d{2})\b", low)
        if m:
            rng = _month_range(m.group(1))
        else:
            today = date.today()
            rng = _month_range(f"{today.year:04d}-{today.month:02d}")
        if rng:
            date_from, date_to = rng

    # range commands: "report range 2026-01-02 2026-01-07"
    if not date_from:
        m = re.search(r"\brange\s+(\S+)\s+(\S+)", low)
        if m:
            df = _parse_date_token(m.group(1))
            dt = _parse_date_token(m.group(2))
            if df and dt:
                date_from, date_to = df, dt

    # greek: "απο 2/1/26 εως 7/1/26"
    if not date_from:
        m = re.search(r"\bαπο\s+(\S+)\s+εως\s+(\S+)", low)
        if m:
            df = _parse_date_token(m.group(1))
            dt = _parse_date_token(m.group(2))
            if df and dt:
                date_from, date_to = df, dt

    # fallback: pick first two date tokens anywhere
    if not date_from:
        toks = re.findall(r"(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})", raw)
        if len(toks) >= 2:
            df = _parse_date_token(toks[0])
            dt = _parse_date_token(toks[1])
            if df and dt:
                date_from, date_to = df, dt

    # default if still nothing: current month
    if not date_from:
        today = date.today()
        rng = _month_range(f"{today.year:04d}-{today.month:02d}")
        date_from, date_to = rng

    return {
        "entry_type": entry_type,
        "property_slug": property_slug,
        "date_from": date_from,
        "date_to": date_to,
    }

def handle_report_in_chat(*args, **kwargs):
    # Returns dict response if message is a report command, else None.
    # Works even if caller passes args in different order (so you don't get TypeError later).
    # try kwargs first
    message = kwargs.get("message") or kwargs.get("text") or kwargs.get("user_message")
    public_id = kwargs.get("public_id") or kwargs.get("publicId")

    # infer from args
    str_args = [a for a in args if isinstance(a, str)]
    if not message:
        for s in str_args:
            if any(_norm(s).startswith(p) for p in REPORT_PREFIXES):
                message = s
                break
    if not public_id:
        # public_id usually looks like a token without spaces
        for s in str_args:
            if " " not in (s or "") and len(s) >= 8 and not any(_norm(s).startswith(p) for p in REPORT_PREFIXES):
                public_id = s
                break

    req = _parse_report_request(message or "")
    if not req:
        return None

    rows = finance_list(
        limit=50000,
        property_slug=req["property_slug"],
        entry_type=req["entry_type"],
        date_from=req["date_from"],
        date_to=req["date_to"],
    )

    total = 0.0
    by_cat = defaultdict(lambda: {"count": 0, "sum": 0.0})

    for r in rows:
        try:
            amt = float(r.get("amount") or 0)
        except Exception:
            amt = 0.0
        total += amt
        cat = (r.get("category") or "uncategorized").strip() or "uncategorized"
        by_cat[cat]["count"] += 1
        by_cat[cat]["sum"] += amt

    top = sorted(by_cat.items(), key=lambda kv: kv[1]["sum"], reverse=True)[:5]

    kind = "Κινήσεις"
    if req["entry_type"] == "expense":
        kind = "Έξοδα"
    elif req["entry_type"] == "income":
        kind = "Έσοδα"

    prop_txt = f" · {req['property_slug']}" if req["property_slug"] else ""
    reply_lines = [
        f"{kind} από {req['date_from']} έως {req['date_to']}{prop_txt}",
        f"Σύνολο: {total:.2f} EUR · Πλήθος: {len(rows)}",
    ]
    if top:
        reply_lines.append("Top κατηγορίες:")
        for cat, s in top:
            reply_lines.append(f"- {cat}: {s['sum']:.2f} EUR ({s['count']})")

    params = {}
    if req["entry_type"]:
        params["type"] = req["entry_type"]
    if req["property_slug"]:
        params["property"] = req["property_slug"]
    params["from"] = req["date_from"]
    params["to"] = req["date_to"]

    download_url = f"/p/{public_id}/report.csv?{urlencode(params)}" if public_id else None

    return {
        "reply": "\n".join(reply_lines),
        "download_url": download_url,
    }

</script>
</body>
</html>
"""

@app.post("/p/<public_id>/chat")
def public_chat(public_id):
    rl = rate_limited()
    if rl:
        return rl
    # 1) Πάρε input από JSON ή form
    data = request.get_json(silent=True) or {}
    user_text = (
        (data.get("message") or data.get("text") or data.get("input") or "")
        if isinstance(data, dict) else ""
    ).strip()

    # fallback: αν στέλνεις form-data
    if not user_text:
        user_text = (request.form.get("message") or request.form.get("text") or "").strip()

    # 2) Reports: αν είναι report/αναφορά, ΜΗΝ το περνάς σαν καταχώρηση
    rep = handle_report_in_chat(public_id, user_text)
    if rep:
        return jsonify(rep)

    # 3) από εδώ και κάτω συνεχίζεις το υπάρχον flow (insert / llm / categorization)


    try:
        data = request.get_json(silent=True) or {}
        assistant_id = (data.get("assistant_id") or "").strip()
        message = (data.get("message") or "").strip()

        if not assistant_id:
            return jsonify(error="Missing assistant_id"), 400
        if not message:
            return jsonify(error="Missing message"), 400

        a = _get_public_assistant(public_id)
        if a is None or not _assistant_enabled(a):
            return jsonify(error="Unknown/disabled public assistant"), 404

        cfg = _assistant_config(a)
        require_key_if_needed(cfg)

        slug = str(_assistant_id(a) or "")
        if assistant_id != slug:
            return jsonify(
                error="assistant_id does not match public_id",
                expected=slug,
                provided=assistant_id
            ), 400

        # --- Finance clerk (stateful wizard) ---
        if slug == "finance_clerk":
            if _is_greeting(message):
                return jsonify(public_id=public_id, assistant_slug=slug,
                               reply="Γράψε μια καταχώρηση, π.χ. “Πλήρωσα νερό Βουρβουρού 20€ 05/01/2026”.")

            client_id = _get_client_id_for_state(public_id)
            norm_msg = _norm(message)

            # cancel/reset current pending
            if norm_msg in ("ακυρο", "ακυρώ", "akuro", "cancel", "reset", "clear"):
                finance_pending_clear(public_id, client_id)
                return jsonify(public_id=public_id, assistant_slug=slug, reply="ΟΚ, ακυρώθηκε η τρέχουσα καταχώρηση ✅")

            pending = finance_pending_get(public_id, client_id) or {}
            fields = parse_finance_fields(message)

            date_in_msg = bool(_DATE_RE.search(message))

            # Start new entry if message looks like one (amount + (property or type))
            if looks_like_new_entry(fields):
                pending = {
                    "entry_type": fields.get("entry_type"),
                    "property_slug": fields.get("property_slug"),
                    "amount": fields.get("amount"),
                    "entry_date": fields.get("entry_date"),
                    "category": fields.get("category"),
                    "label": fields.get("label"),
                    "raw_text": fields.get("raw_text") or message.strip(),
                }
            else:
                # If no pending and message isn't an entry, guide user
                if not pending:
                    return jsonify(public_id=public_id, assistant_slug=slug,
                                   reply="Δεν το έπιασα σαν καταχώρηση. Π.χ. “Πλήρωσα νερό Βουρβουρού 20€” ή “Εισέπραξα Airbnb 300€”.")
                # Merge into pending (slot filling)
                if not pending.get("entry_type") and fields.get("entry_type"):
                    pending["entry_type"] = fields["entry_type"]
                if not pending.get("property_slug") and fields.get("property_slug"):
                    pending["property_slug"] = fields["property_slug"]
                if pending.get("amount") is None and fields.get("amount") is not None:
                    pending["amount"] = fields["amount"]
                if date_in_msg:
                    pending["entry_date"] = fields.get("entry_date") or pending.get("entry_date")

                # category: upgrade from uncategorized if we found a better one
                if fields.get("category") and fields.get("category") != "uncategorized":
                    pending["category"] = fields["category"]

                if not pending.get("label") and fields.get("label"):
                    pending["label"] = fields["label"]

                # keep raw text history
                rt = (pending.get("raw_text") or "").strip()
                msg2 = message.strip()
                if msg2 and msg2 not in rt:
                    pending["raw_text"] = (rt + " | " + msg2).strip(" |")

            # merchant map: if still uncategorized, try guess
            if (pending.get("category") in (None, "", "uncategorized")):
                guess = merchant_map_guess_category(pending.get("raw_text") or "")
                if guess:
                    pending["category"] = guess

            miss = missing_fields(pending)
            if miss:
                finance_pending_upsert(public_id, client_id, pending)
                return jsonify(public_id=public_id, assistant_slug=slug, reply=_ask_next_missing(miss))

            # ready to commit
            entry = {
                "id": str(uuid.uuid4()),
                "entry_date": pending.get("entry_date") or date.today().isoformat(),
                "property_slug": pending["property_slug"],
                "entry_type": pending["entry_type"],
                "amount": float(pending["amount"]),
                "currency": "EUR",
                "category": pending.get("category") or "uncategorized",
                "label": pending.get("label") or None,
                "note": None,
                "raw_text": pending.get("raw_text") or message.strip(),
            }

            finance_insert(entry)
            finance_pending_clear(public_id, client_id)

            return jsonify(
                public_id=public_id,
                assistant_slug=slug,
                reply=f"Καταχωρήθηκε ✅ {entry['entry_type']} {entry['amount']}€ | {entry['property_slug']} | {entry['entry_date']} | {entry['category']}",
            )

        # --- Default: LLM assistant ---
        reply_text = _run_assistant(a, message)
        return jsonify(public_id=public_id, assistant_slug=slug, reply=reply_text)

    except Exception as e:
        app.logger.exception("public_chat failed")
        return jsonify(error=str(e), type=type(e).__name__), 500


# ---------------------------
# Finance extra endpoints
# ---------------------------
@app.post("/p/<public_id>/finance/undo_last")
def finance_undo_last(public_id):
    a, err = _finance_auth_and_get_clerk(public_id)
    if err:
        return err

    data = request.get_json(silent=True) or {}
    confirm = data.get("confirm") is True
    property_slug = (data.get("property_slug") or "").strip() or None

    if not confirm:
        return jsonify(error="Need {confirm:true, property_slug?:string}"), 400

    con = _finance_conn()
    if not con:
        return jsonify(error="db_not_configured"), 500

    entry_id = None
    try:
        with con:
            with con.cursor() as cur:
                if property_slug:
                    cur.execute(
                        """
                        SELECT id
                        FROM finance_entries
                        WHERE property_slug=%s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (property_slug,)
                    )
                else:
                    cur.execute(
                        """
                        SELECT id
                        FROM finance_entries
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    )

                row = cur.fetchone()
                if not row:
                    return jsonify(ok=True, message="Nothing to undo")

                entry_id = row[0]
                cur.execute("DELETE FROM finance_entries WHERE id=%s", (entry_id,))
                if cur.rowcount == 0:
                    return jsonify(ok=True, message="Nothing to undo")

        return jsonify(ok=True, undone_id=entry_id, property_slug=property_slug)
    finally:
        try:
            con.close()
        except Exception:
            pass

              


@app.post("/p/<public_id>/finance/delete")
def finance_delete(public_id):
    a, err = _finance_auth_and_get_clerk(public_id)
    if err:
        return err

    data = request.get_json(silent=True) or {}
    entry_id = (data.get("id") or "").strip()
    confirm = data.get("confirm") is True

    if not entry_id or not confirm:
        return jsonify(error="Need {id:string, confirm:true}"), 400

    con = _finance_conn()
    if not con:
        return jsonify(error="db_not_configured"), 500

    with con:
        with con.cursor() as cur:
            cur.execute("DELETE FROM finance_entries WHERE id=%s", (entry_id,))
            if cur.rowcount == 0:
                return jsonify(error="Entry not found"), 404

    con.close()
    return jsonify(ok=True, deleted_id=entry_id)


@app.get("/p/<public_id>/finance/recent")
def finance_recent(public_id):
    a, err = _finance_auth_and_get_clerk(public_id)
    if err:
        return err

    limit = request.args.get("limit", "20")
    try:
        limit = max(1, min(200, int(limit)))
    except Exception:
        limit = 20

    prop = (request.args.get("property_slug") or "").strip() or None

    con = _finance_conn()
    if not con:
        return jsonify(error="db_not_configured"), 500

    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            if prop:
                cur.execute(
    "SELECT id, created_at, entry_date, property_slug, entry_type, amount, currency, category, label "
    "FROM finance_entries "
    "WHERE property_slug=%s "
    "ORDER BY created_at DESC "
    "LIMIT %s",
    (prop, limit)
)
            else:
                cur.execute(
    "SELECT id, created_at, entry_date, property_slug, entry_type, amount, currency, category, label "
    "FROM finance_entries "
    "ORDER BY created_at DESC "
    "LIMIT %s",
    (limit,)
)

            rows = cur.fetchall()

    con.close()
    return jsonify(ok=True, rows=rows)


@app.get("/p/<public_id>/finance/summary")
def finance_summary(public_id):
    a, err = _finance_auth_and_get_clerk(public_id)
    if err:
        return err

    date_from = (request.args.get("from") or "").strip() or None
    date_to = (request.args.get("to") or "").strip() or None
    prop = (request.args.get("property_slug") or "").strip() or None
    entry_type = (request.args.get("type") or "").strip() or None

    # basic validation (ISO date)
    def _valid_date(s):
        if not s:
            return True
        try:
            datetime.fromisoformat(s)
            return True
        except Exception:
            return False

    if not _valid_date(date_from) or not _valid_date(date_to):
        return jsonify(error="Invalid date. Use YYYY-MM-DD"), 400

    where = []
    args = []
    if prop:
        where.append("property_slug=%s"); args.append(prop)
    if entry_type:
        if entry_type not in ("expense", "income"):
            return jsonify(error="type must be expense|income"), 400
        where.append("entry_type=%s"); args.append(entry_type)
    if date_from:
        where.append("entry_date>=%s"); args.append(date_from)
    if date_to:
        where.append("entry_date<=%s"); args.append(date_to)

    sql = "SELECT entry_type, COUNT(*) AS count, COALESCE(SUM(amount),0) AS total FROM finance_entries"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY entry_type ORDER BY entry_type"

    con = _finance_conn()
    if not con:
        return jsonify(error="db_not_configured"), 500

    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    con.close()

    # normalize output
    out = {"expense": {"count": 0, "total": 0}, "income": {"count": 0, "total": 0}}
    for r in rows:
        et = r.get("entry_type")
        out[et] = {"count": int(r.get("count") or 0), "total": float(r.get("total") or 0)}

    return jsonify(ok=True, filters={"from": date_from, "to": date_to, "property_slug": prop, "type": entry_type}, summary=out)
@app.get("/p/<public_id>/report.csv")
def finance_report_csv(public_id):
    try:
        a = _get_public_assistant(public_id)
        if a is None:
            return jsonify(error="assistant_not_found"), 404
        if not _assistant_enabled(a):
            return jsonify(error="assistant_disabled"), 404

        cfg = _assistant_config(a)
        require_key_if_needed(cfg)

        slug = str(_assistant_id(a) or "")
        if slug != "finance_clerk":
            return jsonify(error="not_finance_clerk", slug=slug), 404

        entry_type = (request.args.get("type") or "").strip() or None
        property_slug = (request.args.get("property") or "").strip() or None
        date_from = (request.args.get("from") or "").strip() or None
        date_to = (request.args.get("to") or "").strip() or None

        rows = finance_list(
            limit=50000,
            property_slug=property_slug,
            entry_type=entry_type,
            date_from=date_from,
            date_to=date_to,
        )

        import io, csv
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["date", "property", "type", "amount", "currency", "category", "label", "id"])
        for r in rows:
            w.writerow([
                r.get("entry_date"),
                r.get("property_slug"),
                r.get("entry_type"),
                r.get("amount"),
                r.get("currency", "EUR"),
                r.get("category", ""),
                r.get("label", ""),
                r.get("id"),
            ])

        csv_text = "\ufeff" + buf.getvalue()  # BOM for OpenOffice Greek
        resp = make_response(csv_text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = "attachment; filename=finance_report.csv"
        return resp

    except HTTPException as e:
        return jsonify(error="unauthorized", detail=str(e)), (e.code or 401)
    except Exception as e:
        app.logger.exception("finance_report_csv failed")
        return jsonify(error="report_failed", detail=str(e)), 500


@app.get("/p/<public_id>/export.csv")
def finance_export(public_id):
    try:
        a = _get_public_assistant(public_id)
        if a is None:
            return jsonify(error="assistant_not_found"), 404
        if not _assistant_enabled(a):
            return jsonify(error="assistant_disabled"), 404

        cfg = _assistant_config(a)
        try:
            require_key_if_needed(cfg)
        except HTTPException as e:
            return jsonify(error="unauthorized", code=e.code), (e.code or 401)

        slug = str(_assistant_id(a) or "")
        if slug != "finance_clerk":
            return jsonify(error="not_finance_clerk", slug=slug), 404

        rows = finance_list(limit=5000)

        import io, csv
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(["date", "property", "type", "amount", "currency", "category", "label", "id"])
        for r in rows:
            w.writerow([
                r.get("entry_date"),
                r.get("property_slug"),
                r.get("entry_type"),
                r.get("amount"),
                r.get("currency", "EUR"),
                r.get("category", ""),
                r.get("label", ""),
                r.get("id"),
            ])

        csv_text = "\ufeff" + buf.getvalue()
        resp = make_response(csv_text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = "attachment; filename=finance_entries.csv"
        return resp

    except Exception as e:
        app.logger.exception("finance_export failed")
        return jsonify(error="export_failed", detail=str(e)), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=DEBUG_MODE)


