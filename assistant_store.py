from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class AssistantConfig:
    assistant_id: str
    name: str
    enabled: bool
    model: str
    temperature: float
    max_tokens: int
    prompt: str = ""
    knowledge: str = ""


class AssistantStore:
    """
    Loads assistants from a directory like:

    assistants/
      fava_guest/
        config.json
        prompt.md
        knowledge.md
    """

    def __init__(self, base_dir: str = "assistants"):
        self.base_dir = Path(base_dir)
        self._assistants: Dict[str, AssistantConfig] = {}
        self.reload()

    def reload(self) -> None:
        self._assistants = {}

        if not self.base_dir.exists() or not self.base_dir.is_dir():
            print(f"[AssistantStore] base_dir not found or not a dir: {self.base_dir}")
            return

        for folder in sorted(self.base_dir.iterdir()):
            if not folder.is_dir():
                continue

            assistant_id = folder.name
            cfg_path = folder / "config.json"
            if not cfg_path.exists():
                # No config = skip silently
                continue

            try:
                # utf-8-sig handles UTF-8 BOM (EF BB BF)
                cfg_text = cfg_path.read_text(encoding="utf-8-sig")
                cfg = json.loads(cfg_text)
            except Exception as e:
                print(f"[AssistantStore] Skip '{assistant_id}': bad config.json ({e})")
                continue

            try:
                name = str(cfg.get("name", assistant_id))
                enabled = bool(cfg.get("enabled", True))
                model = str(cfg.get("model", "mistral-large-latest"))
                temperature = float(cfg.get("temperature", 0.2))
                max_tokens = int(cfg.get("max_tokens", 600))
            except Exception as e:
                print(f"[AssistantStore] Skip '{assistant_id}': invalid config fields ({e})")
                continue

            prompt_path = folder / "prompt.md"
            knowledge_path = folder / "knowledge.md"

            prompt = ""
            knowledge = ""

            try:
                if prompt_path.exists():
                    prompt = prompt_path.read_text(encoding="utf-8-sig")
            except Exception as e:
                print(f"[AssistantStore] '{assistant_id}': couldn't read prompt.md ({e})")

            try:
                if knowledge_path.exists():
                    knowledge = knowledge_path.read_text(encoding="utf-8-sig")
            except Exception as e:
                print(f"[AssistantStore] '{assistant_id}': couldn't read knowledge.md ({e})")

            self._assistants[assistant_id] = AssistantConfig(
                assistant_id=assistant_id,
                name=name,
                enabled=enabled,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                prompt=prompt,
                knowledge=knowledge,
            )

        print(f"[AssistantStore] Loaded assistants: {list(self._assistants.keys())}")

    def list(self, enabled_only: bool = True) -> List[AssistantConfig]:
        items = list(self._assistants.values())
        if enabled_only:
            items = [a for a in items if a.enabled]
        return sorted(items, key=lambda a: a.assistant_id)

    def get(self, assistant_id: str) -> Optional[AssistantConfig]:
        return self._assistants.get(assistant_id)
