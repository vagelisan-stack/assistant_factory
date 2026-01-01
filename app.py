import os
import time
from pathlib import Path
from collections import defaultdict, deque

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


@app.after_request
def force_utf8(resp):
    # Make JSON responses explicitly UTF-8 (helps PowerShell decoding too)
    if resp.mimetype == "application/json":
        resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


# ---------- Admin protection (for private endpoints like /reload) ----------
def require_admin_key():
    expected = (os.getenv("ADMIN_API_KEY") or "").strip()
    if not expected:
        # If not set, don't block (useful for local dev)
        return None
    provided = (request.headers.get("X-ADMIN-KEY") or "").strip()
    if provided != expected:
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


# ---------- Health / Assistants ----------
@app.get("/health")
def health():
    try:
        assistants = STORE.list(enabled_only=False)
        return jsonify(
            ok=True,
            assistants_dir=str(ASSISTANTS_DIR),
            assistants_count=len(assistants),
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e), assistants_dir=str(ASSISTANTS_DIR)), 500


@app.get("/assistants")
def assistants():
    enabled_only = request.args.get("enabled_only", "1") != "0"
    items = STORE.list(enabled_only=enabled_only)
    return jsonify(assistants=[_assistant_to_dict(a) for a in items])


@app.post("/reload")
def reload_assistants():
    # ADMIN ONLY
    auth = require_admin_key()
    if auth:
        return auth

    global STORE
    try:
        if hasattr(STORE, "reload"):
            STORE.reload()
        else:
            STORE = AssistantStore(base_dir=str(ASSISTANTS_DIR))

        return jsonify(ok=True, assistants=[_assistant_to_dict(a) for a in STORE.list(enabled_only=False)])
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# ---------- Chat (public, rate-limited) ----------
@app.post("/chat")
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

    model = getattr(a, "model", "mistral-large-latest")
    temperature = float(getattr(a, "temperature", 0.2) or 0.2)
    max_tokens = int(getattr(a, "max_tokens", 600) or 600)

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
        match=(expected == provided and expected != ""),
        expected_len=len(expected),
        provided_len=len(provided),
        expected_sha12=sha12(expected),
        provided_sha12=sha12(provided),
    )


# ---------- Local dev only ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    app.run(host="0.0.0.0", port=port)
