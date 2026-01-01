import os
import requests
from typing import List, Dict, Any, Optional


class LLMError(RuntimeError):
    pass


class MistralClient:
    """
    Calls Mistral Chat Completions endpoint:
    https://api.mistral.ai/v1/chat/completions  :contentReference[oaicite:4]{index=4}
    """

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, timeout: int = 40):
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        self.base_url = base_url or os.getenv("MISTRAL_BASE_URL", "https://api.mistral.ai/v1/chat/completions")
        self.timeout = timeout

    def chat(self, model: str, messages: List[Dict[str, str]], temperature: float = 0.2, max_tokens: int = 600) -> str:
        if not self.api_key:
            raise LLMError("Λείπει το MISTRAL_API_KEY (βάλε το σε .env ή env var).")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            r = requests.post(self.base_url, headers=headers, json=payload, timeout=self.timeout)
        except requests.RequestException as e:
            raise LLMError(f"Network error προς LLM: {e}") from e

        if r.status_code >= 400:
            raise LLMError(f"LLM error {r.status_code}: {r.text}")

        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            raise LLMError(f"Απρόσμενο response format: {data}")

