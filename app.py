import os
import time
from pathlib import Path
from collections import defaultdict, deque
import json
from functools import wraps
from flask import Flask, request, jsonify, Response, render_template_string
from db_store import DBAssistantStore


from dotenv import load_dotenv
from flask import Flask, request, jsonify, abort

import os
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

import json
import os
from flask import request, abort

def _assistant_config(a) -> dict:
    cfg = None
    if isinstance(a, dict):
        cfg = a.get("config")
    else:
        cfg = getattr(a, "config", None)

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
    public_id = (public_id or "").strip()
    if not public_id:
        return None

    # Prefer db_store if it exposes a getter by public_id
    if db_store:
        for fn_name in ("get_by_public_id", "get_public_by_public_id", "public_get", "get_public"):
            if hasattr(db_store, fn_name):
                try:
                    a = getattr(db_store, fn_name)(public_id)
                    if a:
                        return a
                except Exception:
                    pass

    # Fallback: scan STORE (works if STORE objects include public_id/is_public)
    for a in STORE.list(enabled_only=False):
        pid = (a.get("public_id") if isinstance(a, dict) else getattr(a, "public_id", None))
        is_pub = (a.get("is_public") if isinstance(a, dict) else getattr(a, "is_public", False))
        if pid == public_id and is_pub:
            return a

    return None


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


# Load .env ONCE, early, and override any old env vars
load_dotenv(override=True)

from assistant_store import AssistantStore
from llm_client import MistralClient, LLMError

# ---------- Paths / Store (Render-safe) ----------
BASE_DIR = Path(__file__).resolve().parent
ASSISTANTS_DIR = Path(os.getenv("ASSISTANTS_DIR") or (BASE_DIR / "assistants"))
STORE = AssistantStore(base_dir=str(ASSISTANTS_DIR))

# ---------- Flask ----------
app = Flask(__name__)
app.json.ensure_ascii = False  # allow Greek in JSON responses
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

db_store = DBAssistantStore(DATABASE_URL) if DATABASE_URL else None
if db_store:
    db_store.init_db()


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def bootstrap_db_from_filesystem() -> None:
    """
    One-time import: if DB has zero assistants, import from ./assistants/<slug>/{config.json,prompt.md,knowledge.md}
    so we don't lose existing assistants on first DB run.
    """
    if not db_store:
        return

    try:
        existing = db_store.list_admin()
    except Exception:
        return

    if existing:
        return

    base = os.path.join(os.path.dirname(__file__), "assistants")
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

PUBLIC_CHAT_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Assistant</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 24px auto; padding: 0 12px; }
    #log { border: 1px solid #ddd; padding: 12px; height: 55vh; overflow: auto; white-space: pre-wrap; }
    textarea { width: 100%; height: 90px; }
    button { padding: 10px 14px; margin-top: 8px; }
    .me { color: #111; }
    .bot { color: #333; }
    .muted { color: #777; font-size: 12px; }
  </style>
</head>
<body>
  <h2>Chat</h2>
  <div class="muted">Public ID: {{ public_id }}</div>
  <div id="log"></div>

  <textarea id="msg" placeholder="Type your message..."></textarea>
  <br/>
  <button id="send">Send</button>

  <script>
    const log = document.getElementById('log');
    const msg = document.getElementById('msg');
    const send = document.getElementById('send');
    const endpoint = "/p/{{ public_id }}/chat";

    function addLine(cls, text) {
      const div = document.createElement('div');
      div.className = cls;
      div.textContent = text;
      log.appendChild(div);
      log.scrollTop = log.scrollHeight;
    }

    async function doSend() {
      const text = msg.value.trim();
      if (!text) return;
      addLine('me', "You: " + text);
      msg.value = "";

      try {
        const params = new URLSearchParams(window.location.search);
const k = params.get("k");

const chatUrl = k
  ? `/p/${publicId}/chat?k=${encodeURIComponent(k)}`
  : `/p/${publicId}/chat`;

fetch(chatUrl, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ message })
})


    send.addEventListener('click', doSend);
    msg.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) doSend();
    });
  </script>
</body>
</html>
"""


@app.after_request
def force_utf8(resp):
    # Make JSON responses explicitly UTF-8 (helps PowerShell decoding too)
    if resp.mimetype == "application/json":
        resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


# ---------- Admin protection (for private endpoints like /reload) ----------
def require_admin_key():
    if not _is_admin(request):
        return jsonify(error="Unauthorized"), 401
    return None


# ---------- Public anti-abuse (rate limiting for /chat) ----------
RATE = defaultdict(lambda: deque())
WINDOW_SECONDS = int(os.getenv("RL_WINDOW_SECONDS", "60"))
MAX_REQ_PER_WINDOW = int(os.getenv("RL_MAX_REQ", "20"))


def get_client_ip() -> str:
    # Render / proxies usually provide X-Forwarded-For
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


# ---------- Helpers ----------
def _assistant_id(a):
    return getattr(a, "assistant_id", getattr(a, "id", None))


def _assistant_enabled(a):
    return bool(getattr(a, "enabled", False))


def _assistant_to_dict(a):
    return {
        "id": _assistant_id(a),
        "name": getattr(a, "name", None),
        "enabled": _assistant_enabled(a),
        "model": getattr(a, "model", None),
        "temperature": getattr(a, "temperature", None),
        "max_tokens": getattr(a, "max_tokens", None),
    }


def _get_assistant(assistant_id: str):
    # Prefer STORE.get(...) if it exists
    if hasattr(STORE, "get"):
        a = STORE.get(assistant_id)
        if a is not None:
            return a

    # Fallback: search list
    for a in STORE.list(enabled_only=False):
        if _assistant_id(a) == assistant_id:
            return a
    return None


def _get_prompt_and_knowledge(a):
    prompt = (
        getattr(a, "prompt", None)
        or getattr(a, "prompt_text", None)
        or getattr(a, "system_prompt", None)
        or ""
    )
    knowledge = (
        getattr(a, "knowledge", None)
        or getattr(a, "knowledge_text", None)
        or ""
    )
    return str(prompt), str(knowledge)

# ---------- Admin helpers ----------
from functools import wraps

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

# ---------- Health / Assistants ----------
# ---------- Public rate limiting (MVP) ----------
_RATE = {}  # ip -> [timestamps]
RATE_LIMIT_RPM = int(os.getenv("PUBLIC_RPM", "30"))

def _client_ip():
    return (request.headers.get("CF-Connecting-IP")
            or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.remote_addr
            or "unknown")

def public_rate_limit_ok() -> bool:
    ip = _client_ip()
    now = time.time()
    arr = _RATE.get(ip, [])
    arr = [t for t in arr if now - t < 60.0]
    if len(arr) >= RATE_LIMIT_RPM:
        _RATE[ip] = arr
        return False
    arr.append(now)
    _RATE[ip] = arr
    return True

@app.get("/health")
def health():
    assistants = STORE.list(enabled_only=False)
    return jsonify(
        ok=True,
        assistants_dir=str(ASSISTANTS_DIR),
        assistants_count=len(assistants),
    )    

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
        if db_store:
            seeded = db_store.seed_from_filesystem(str(ASSISTANTS_DIR))
            return jsonify(ok=True, seeded=seeded, assistants=db_store.list_admin())
        return jsonify(ok=False, error="db_not_configured"), 500
    except Exception as e:
        app.logger.exception("reload failed")
        return jsonify(ok=False, error=str(e), type=type(e).__name__), 500

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
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



# ---------- Chat (public, rate-limited) ----------
def _reply_from_record(rec, message: str) -> str:
    cfg = rec.config or {}

    model = cfg.get("model", "mistral-large-latest")
    temperature = float(cfg.get("temperature", 0.2))
    max_tokens = int(cfg.get("max_tokens", 600))

    parts = []
    if (rec.prompt or "").strip():
        parts.append(rec.prompt.strip())
    if (rec.knowledge or "").strip():
        parts.append("KNOWLEDGE:\n" + rec.knowledge.strip())
    system_text = "\n\n".join(parts).strip()

    messages = []
    if system_text:
        messages.append({"role": "system", "content": system_text})
    messages.append({"role": "user", "content": message})

    client = MistralClient()
    return client.chat(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
from flask import make_response

@app.get("/p/<public_id>")
@app.get("/p/<public_id>")
def public_page(public_id):
    a = _get_public_assistant(public_id)
    if a is None or not _assistant_enabled(a):
        abort(404)

    cfg = _assistant_config(a)
    require_key_if_needed(cfg)

    resp = make_response(render_template_string(PUBLIC_CHAT_HTML, public_id=public_id))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

def _run_assistant(rec, message: str) -> str:
    """
    Shared runner for both /chat and /p/<public_id>/chat.
    Builds a simple system prompt from prompt + knowledge and calls Mistral.
    """
    import json

    cfg = getattr(rec, "config", None)
    if cfg is None:
        cfg = getattr(rec, "config_json", None)
    if cfg is None:
        cfg = {}

    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}

    prompt = (getattr(rec, "prompt", None) or "").strip()
    knowledge = (getattr(rec, "knowledge", None) or "").strip()

    system = prompt
    if knowledge:
        system = (system + "\n\n" if system else "") + "### Knowledge\n" + knowledge

    if not system:
        system = "You are a helpful assistant."

    model = cfg.get("model") or getattr(rec, "model", None) or "mistral-large-latest"
    temperature = cfg.get("temperature", getattr(rec, "temperature", 0.2))
    max_tokens = cfg.get("max_tokens", getattr(rec, "max_tokens", 600))

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



@app.post("/p/<public_id>/chat")
def public_chat(public_id):
    rl = rate_limited()
    if rl:
        return rl

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify(error="Missing message"), 400

    a = _get_public_assistant(public_id)
    if a is None or not _assistant_enabled(a):
        return jsonify(error="Unknown/disabled public assistant"), 404

    cfg = _assistant_config(a)
    require_key_if_needed(cfg)

    prompt, knowledge = _get_prompt_and_knowledge(a)

    system_text = prompt.strip()
    if knowledge.strip():
        system_text += "\n\nKNOWLEDGE:\n" + knowledge.strip()

    messages = [
        {"role": "system", "content": system_text.strip()},
        {"role": "user", "content": message},
    ]

    try:
        client = MistralClient()
        reply_text = client.chat(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return jsonify(public_id=public_id, assistant_slug=_assistant_id(a), reply=reply_text)

    except LLMError as e:
        status = int(getattr(e, "status_code", 500) or 500)
        return jsonify(error=f"LLM error {status}: {e}"), 500

    except Exception as e:
        return jsonify(error=str(e)), 500


# ---------- Local dev only ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    app.run(host="0.0.0.0", port=port)
