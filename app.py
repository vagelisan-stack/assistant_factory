import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify

# Load .env ONCE, early, and override any old env vars (important for Render)
load_dotenv(override=True)

from assistant_store import AssistantStore
from llm_client import MistralClient, LLMError

# ---------- Paths / Store (Render-safe) ----------
BASE_DIR = Path(__file__).resolve().parent
ASSISTANTS_DIR = Path(os.getenv("ASSISTANTS_DIR") or (BASE_DIR / "assistants"))

STORE = AssistantStore(base_dir=str(ASSISTANTS_DIR))

# ---------- Flask ----------
app = Flask(__name__)
app.json.ensure_ascii = False  # allow Greek in JSON

@app.after_request
def force_utf8(resp):
    # Make JSON responses explicitly UTF-8 (helps PowerShell decoding too)
    if resp.mimetype == "application/json":
        resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


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
    # Be tolerant to different attribute names
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


# ---------- Debug / Introspection ----------
@app.get("/_routes")
def routes():
    return jsonify(sorted([f"{sorted(list(r.methods))} {r.rule}" for r in app.url_map.iter_rules()]))

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
    global STORE
    # If AssistantStore has reload(), use it, otherwise re-instantiate
    try:
        if hasattr(STORE, "reload"):
            STORE.reload()
        else:
            STORE = AssistantStore(base_dir=str(ASSISTANTS_DIR))
        return jsonify(ok=True, assistants=[_assistant_to_dict(a) for a in STORE.list(enabled_only=False)])
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# ---------- Chat ----------
@app.post("/chat")
def chat():
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

    # Build messages for Mistral chat completions
    system_text = prompt
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
        # Assumption: MistralClient has a .chat(model, messages, temperature, max_tokens) method
        reply_text = client.chat(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return jsonify(assistant_id=assistant_id, reply=reply_text)

    except LLMError as e:
        # Your LLMError likely has status_code + message/detail; be tolerant.
        status = int(getattr(e, "status_code", 500) or 500)
        return jsonify(error=f"LLM error {status}: {e}"), 500

    except Exception as e:
        return jsonify(error=str(e)), 500


# ---------- Local dev only ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    app.run(host="0.0.0.0", port=port)
