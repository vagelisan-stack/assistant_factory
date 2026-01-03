import os
import time
from pathlib import Path
from collections import defaultdict, deque
import json
from functools import wraps
from flask import Flask, request, jsonify, Response
from db_store import DBAssistantStore


from dotenv import load_dotenv
from flask import Flask, request, jsonify

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

@app.post("/chat")
# ---------- Public (unlisted) page + chat ----------
@app.get("/p/<public_id>")
def public_page(public_id: str):
    if not db_store:
        return ("Not Found", 404)

    rec = db_store.get_by_public_id(public_id)
    if not rec or not rec.enabled or not rec.is_public:
        return ("Not Found", 404)

    title = rec.name or "Assistant"

    html = f"""<!doctype html>
<html lang="el">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; max-width: 820px; margin: 24px auto; padding: 0 12px; }}
    .box {{ border: 1px solid #ddd; border-radius: 12px; padding: 12px; }}
    textarea {{ width: 100%; height: 90px; }}
    button {{ padding: 10px 14px; border-radius: 10px; border: 1px solid #ccc; cursor: pointer; }}
    pre {{ white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h2>{title}</h2>
  <div class="box">
    <textarea id="msg" placeholder="Γράψε μήνυμα..."></textarea>
    <div style="margin-top:10px;">
      <button id="send">Send</button>
    </div>
    <pre id="out" style="margin-top:12px;"></pre>
  </div>

<script>
const out = document.getElementById('out');
document.getElementById('send').onclick = async () => {{
  const message = document.getElementById('msg').value.trim();
  if (!message) return;
  out.textContent = "…";
  const r = await fetch(`/p/{public_id}/chat`, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ message }})
  }});
  const data = await r.json().catch(() => ({{error:"bad_json"}}));
  out.textContent = data.answer || data.response || JSON.stringify(data, null, 2);
}};
</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@app.post("/p/<public_id>/chat")
def public_chat(public_id: str):
    if not db_store:
        return jsonify({"error": "not_found"}), 404

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "missing_message"}), 400

    rec = db_store.get_by_public_id(public_id)
    if not rec or not rec.enabled or not rec.is_public:
        return jsonify({"error": "not_found"}), 404

    try:
        # reuse the same assistant runner you use in POST /chat
        answer = _run_assistant(rec, message)
        return jsonify({"answer": answer})
    except Exception as e:
        app.logger.exception("public chat failed")
        return jsonify({"error": "chat_failed", "type": type(e).__name__, "detail": str(e)}), 500

def chat():
    rl = rate_limited()
    if rl:
        return rl

    data = request.get_json(silent=True) or {}

    assistant_id = (data.get("assistant_id") or "").strip()
    message = (data.get("message") or "").strip()

    if not assistant_id:
        return jsonify(error="Missing assistant_id"), 400
    if not message:
        return jsonify(error="Missing message"), 400

    a = _get_assistant(assistant_id)
    if a is None or not _assistant_enabled(a):
        return jsonify(error=f"Unknown/disabled assistant: {assistant_id}"), 400

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
        return jsonify(assistant_id=assistant_id, reply=reply_text)

    except LLMError as e:
        status = int(getattr(e, "status_code", 500) or 500)
        return jsonify(error=f"LLM error {status}: {e}"), 500

    except Exception as e:
        return jsonify(error=str(e)), 500


# ---------- Local dev only ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    app.run(host="0.0.0.0", port=port)
