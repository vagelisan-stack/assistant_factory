import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify

# IMPORTANT: load .env BEFORE importing llm_client, and override any old env vars
load_dotenv(override=True)

from assistant_store import AssistantStore
from llm_client import MistralClient, LLMError
load_dotenv()

app = Flask(__name__)
@app.get("/_routes")
def _routes():
    return jsonify(sorted([f"{r.methods} {r.rule}" for r in app.url_map.iter_rules()]))


STORE = AssistantStore(base_dir=r"C:\mistral_tests\assistant_factory\assistants")
print("CWD:", os.getcwd())
print("ASSISTANTS_DIR env:", os.getenv("ASSISTANTS_DIR"))
print("Loaded assistants:", [a.assistant_id for a in STORE.list(enabled_only=False)])


LLM = MistralClient()


def build_system_message(assistant) -> str:
    parts = []
    if assistant.prompt:
        parts.append(assistant.prompt)
    if assistant.knowledge:
        parts.append("\n\n---\n\nKNOWLEDGE (facts, use as ground truth):\n" + assistant.knowledge)
    return "\n".join(parts).strip()


@app.get("/health")
def health():
    return jsonify(
        ok=True,
        assistants=[{"id": a.assistant_id, "name": a.name, "enabled": a.enabled} for a in STORE.list(enabled_only=False)],
    )


@app.get("/assistants")
def assistants():
    enabled_only = request.args.get("enabled_only", "1") != "0"
    return jsonify(
        assistants=[
            {"id": a.assistant_id, "name": a.name, "enabled": a.enabled, "model": a.model}
            for a in STORE.list(enabled_only=enabled_only)
        ]
    )


@app.post("/chat")
def chat():
    body = request.get_json(silent=True) or {}
    assistant_id = body.get("assistant_id", "fava_guest")
    message = (body.get("message") or "").strip()
    history = body.get("history") or []  # optional: list of {role, content}

    if not message:
        return jsonify(error="Missing 'message'"), 400

    a = STORE.get(assistant_id)
    if not a or not a.enabled:
        return jsonify(error=f"Unknown/disabled assistant: {assistant_id}"), 404

    system_msg = build_system_message(a)

    messages = []
    if system_msg:
        messages.append({"role": "system", "content": system_msg})

    # accept minimal history (optional)
    if isinstance(history, list):
        for m in history[-20:]:
            if isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str):
                messages.append({"role": m["role"], "content": m["content"]})

    messages.append({"role": "user", "content": message})

    try:
        reply = LLM.chat(
            model=a.model,
            messages=messages,
            temperature=a.temperature,
            max_tokens=a.max_tokens,
        )
    except LLMError as e:
        return jsonify(error=str(e)), 500

    return jsonify(assistant_id=assistant_id, reply=reply)


@app.post("/reload")
def reload_store():
    # dev convenience: reload assistant files without redeploy
    STORE.reload()
    return jsonify(ok=True, assistants=[a.assistant_id for a in STORE.list(enabled_only=False)])


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

