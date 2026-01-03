import os
import json
import secrets
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row


def _normalize_db_url(url: str) -> str:
    # Some providers give postgres:// but drivers expect postgresql://
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


@dataclass
class AssistantRecord:
    slug: str
    name: str
    enabled: bool
    is_public: bool
    public_id: Optional[str]
    config: Dict[str, Any]
    prompt: str
    knowledge: str


class DBAssistantStore:
    def __init__(self, database_url: str):
        self.database_url = _normalize_db_url(database_url)

    def _conn(self):
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def init_db(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS assistants (
            id BIGSERIAL PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            is_public BOOLEAN NOT NULL DEFAULT FALSE,
            public_id TEXT UNIQUE,
            current_revision_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS assistant_revisions (
            id BIGSERIAL PRIMARY KEY,
            assistant_id BIGINT NOT NULL REFERENCES assistants(id) ON DELETE CASCADE,
            revision INT NOT NULL,
            config JSONB NOT NULL,
            prompt TEXT NOT NULL,
            knowledge TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_by TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS uq_assistant_revision
        ON assistant_revisions(assistant_id, revision);
        """
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute(ddl)

    # ---------- Read paths ----------
    def get_by_slug(self, slug: str) -> Optional[AssistantRecord]:
        q = """
        SELECT a.slug, a.name, a.enabled, a.is_public, a.public_id,
               r.config, r.prompt, r.knowledge
        FROM assistants a
        JOIN assistant_revisions r ON r.id = a.current_revision_id
        WHERE a.slug = %s
        """
        with self._conn() as con:
            row = con.execute(q, (slug,)).fetchone()
            if not row:
                return None
            return AssistantRecord(
                slug=row["slug"],
                name=row["name"],
                enabled=row["enabled"],
                is_public=row["is_public"],
                public_id=row["public_id"],
                config=row["config"],
                prompt=row["prompt"],
                knowledge=row["knowledge"],
            )

    def get_by_public_id(self, public_id: str) -> Optional[AssistantRecord]:
        q = """
        SELECT a.slug, a.name, a.enabled, a.is_public, a.public_id,
               r.config, r.prompt, r.knowledge
        FROM assistants a
        JOIN assistant_revisions r ON r.id = a.current_revision_id
        WHERE a.public_id = %s AND a.is_public = TRUE
        """
        with self._conn() as con:
            row = con.execute(q, (public_id,)).fetchone()
            if not row:
                return None
            return AssistantRecord(
                slug=row["slug"],
                name=row["name"],
                enabled=row["enabled"],
                is_public=row["is_public"],
                public_id=row["public_id"],
                config=row["config"],
                prompt=row["prompt"],
                knowledge=row["knowledge"],
            )

    def list_admin(self) -> List[Dict[str, Any]]:
        q = """
        SELECT slug, name, enabled, is_public, public_id, created_at, updated_at
        FROM assistants
        ORDER BY created_at DESC
        """
        with self._conn() as con:
            return con.execute(q).fetchall()

    def list_revisions(self, slug: str) -> List[Dict[str, Any]]:
        q = """
        SELECT r.id, r.revision, r.created_at, r.created_by
        FROM assistants a
        JOIN assistant_revisions r ON r.assistant_id = a.id
        WHERE a.slug = %s
        ORDER BY r.revision DESC
        """
        with self._conn() as con:
            return con.execute(q, (slug,)).fetchall()

    # ---------- Write paths ----------
    def create_assistant(
        self,
        slug: str,
        name: str,
        config: Dict[str, Any],
        prompt: str,
        knowledge: str,
        created_by: str = "admin",
    ) -> Dict[str, Any]:
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO assistants(slug, name) VALUES(%s, %s) RETURNING id",
                    (slug, name),
                )
                assistant_id = cur.fetchone()["id"]
                cur.execute(
                    """
                    INSERT INTO assistant_revisions(assistant_id, revision, config, prompt, knowledge, created_by)
                    VALUES(%s, 1, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (assistant_id, json.dumps(config), prompt, knowledge, created_by),
                )
                rev_id = cur.fetchone()["id"]
                cur.execute(
                    "UPDATE assistants SET current_revision_id=%s, updated_at=NOW() WHERE id=%s",
                    (rev_id, assistant_id),
                )
        return {"slug": slug, "revision_id": rev_id}

    def update_assistant(
        self,
        slug: str,
        config: Dict[str, Any],
        prompt: str,
        knowledge: str,
        created_by: str = "admin",
    ) -> Dict[str, Any]:
        with self._conn() as con:
            with con.cursor() as cur:
                cur.execute("SELECT id FROM assistants WHERE slug=%s", (slug,))
                arow = cur.fetchone()
                if not arow:
                    raise ValueError("assistant not found")
                assistant_id = arow["id"]

                cur.execute(
                    "SELECT COALESCE(MAX(revision), 0) AS maxrev FROM assistant_revisions WHERE assistant_id=%s",
                    (assistant_id,),
                )
                next_rev = int(cur.fetchone()["maxrev"]) + 1

                cur.execute(
                    """
                    INSERT INTO assistant_revisions(assistant_id, revision, config, prompt, knowledge, created_by)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (assistant_id, next_rev, json.dumps(config), prompt, knowledge, created_by),
                )
                rev_id = cur.fetchone()["id"]

                cur.execute(
                    "UPDATE assistants SET current_revision_id=%s, updated_at=NOW() WHERE id=%s",
                    (rev_id, assistant_id),
                )
        return {"slug": slug, "revision": next_rev, "revision_id": rev_id}

    def set_enabled(self, slug: str, enabled: bool) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE assistants SET enabled=%s, updated_at=NOW() WHERE slug=%s",
                (enabled, slug),
            )

    def rollback(self, slug: str, revision_id: int) -> None:
        q = """
        UPDATE assistants a
        SET current_revision_id = r.id, updated_at = NOW()
        FROM assistant_revisions r
        WHERE a.slug = %s AND r.id = %s AND r.assistant_id = a.id
        """
        with self._conn() as con:
            res = con.execute(q, (slug, revision_id))
            if res.rowcount == 0:
                raise ValueError("invalid revision_id for this assistant")
def seed_from_filesystem(self, assistants_dir: str) -> dict:
    """
    Loads assistants from folders (config.json, prompt.md, knowledge.md)
    into DB (upsert by slug). Returns {"upserted": N, "slugs":[...]}.
    """
    import json
    from pathlib import Path

    base = Path(assistants_dir)
    if not base.exists():
        return {"upserted": 0, "slugs": []}

    # discover existing columns to avoid schema mismatch surprises
    with self._conn() as con:
        cols_rows = con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='assistants'
        """).fetchall()

    def _colname(r):
        # psycopg row_factory may give dict-like or tuple-like
        try:
            return r["column_name"]
        except Exception:
            return r[0]

    cols = set(_colname(r) for r in cols_rows)

    # columns we WANT to set, but only if they exist
    def pick(data: dict) -> dict:
        return {k: v for k, v in data.items() if k in cols}

    upserted = 0
    slugs = []

    for d in base.iterdir():
        if not d.is_dir():
            continue

        slug = d.name
        cfg_path = d / "config.json"
        prompt_path = d / "prompt.md"
        knowledge_path = d / "knowledge.md"

        if not cfg_path.exists():
            continue

        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            cfg = {}

        prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
        knowledge = knowledge_path.read_text(encoding="utf-8") if knowledge_path.exists() else ""

        # sensible defaults
        name = cfg.get("name") or slug
        enabled = bool(cfg.get("enabled", True))
        model = cfg.get("model", "mistral-large-latest")
        temperature = float(cfg.get("temperature", 0.2))
        max_tokens = int(cfg.get("max_tokens", 600))

        row = pick({
            "slug": slug,
            "name": name,
            "enabled": enabled,
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "config_json": json.dumps(cfg, ensure_ascii=False),
            "prompt": prompt,
            "knowledge": knowledge,
            "is_public": False,
            "public_id": None,
        })

        # build INSERT ... ON CONFLICT (slug) DO UPDATE dynamically
        insert_cols = list(row.keys())
        if "slug" not in insert_cols:
            # cannot seed without slug column
            continue

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_list = ", ".join(insert_cols)

        # update everything except slug
        update_cols = [c for c in insert_cols if c != "slug"]
        update_set = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols]) if update_cols else ""

        q = f"""
        INSERT INTO assistants ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (slug)
        DO UPDATE SET {update_set}
        """

        values = [row[c] for c in insert_cols]

        with self._conn() as con:
            con.execute(q, values)

        upserted += 1
        slugs.append(slug)

    return {"upserted": upserted, "slugs": slugs}

    def publish(self, slug: str) -> str:
        public_id = secrets.token_urlsafe(18)
        q = """
        UPDATE assistants
        SET is_public=TRUE, public_id=%s, updated_at=NOW()
        WHERE slug=%s
        RETURNING public_id
        """
        with self._conn() as con:
            row = con.execute(q, (public_id, slug)).fetchone()
            if not row:
                raise ValueError("assistant not found")
            return row["public_id"]

    def unpublish(self, slug: str) -> None:
        with self._conn() as con:
            res = con.execute(
                "UPDATE assistants SET is_public=FALSE, updated_at=NOW() WHERE slug=%s",
                (slug,),
            )
            if res.rowcount == 0:
                raise ValueError("assistant not found")

    def rotate_public_id(self, slug: str) -> str:
        new_id = secrets.token_urlsafe(18)
        q = """
        UPDATE assistants
        SET public_id=%s, is_public=TRUE, updated_at=NOW()
        WHERE slug=%s
        RETURNING public_id
        """
        with self._conn() as con:
            row = con.execute(q, (new_id, slug)).fetchone()
            if not row:
                raise ValueError("assistant not found")
            return row["public_id"]
# -----------------------------
# Hotfix: bind required APIs to DBAssistantStore (seed + publish)
# -----------------------------

def _db_seed_from_filesystem(self, assistants_dir: str, created_by: str = "seed") -> dict:
    import json
    from pathlib import Path
    from psycopg.types.json import Json

    base = Path(assistants_dir)
    if not base.exists():
        return {"upserted": 0, "revisions_added": 0, "slugs": []}

    upserted = 0
    revisions_added = 0
    slugs = []

    with self._conn() as con:
        for d in sorted(base.iterdir()):
            if not d.is_dir():
                continue

            slug = d.name
            cfg_path = d / "config.json"
            if not cfg_path.exists():
                continue

            cfg = json.loads(cfg_path.read_text(encoding="utf-8") or "{}")
            prompt = (d / "prompt.md").read_text(encoding="utf-8") if (d / "prompt.md").exists() else ""
            knowledge = (d / "knowledge.md").read_text(encoding="utf-8") if (d / "knowledge.md").exists() else ""

            name = cfg.get("name") or slug
            enabled = bool(cfg.get("enabled", True))

            # upsert assistant row (by slug)
            q_upsert = """
            INSERT INTO assistants (slug, name, enabled, is_public, public_id, created_at, updated_at)
            VALUES (%s, %s, %s, FALSE, NULL, NOW(), NOW())
            ON CONFLICT (slug) DO UPDATE SET
                name=EXCLUDED.name,
                enabled=EXCLUDED.enabled,
                updated_at=NOW()
            RETURNING id
            """
            arow = con.execute(q_upsert, (slug, name, enabled)).fetchone()
            assistant_db_id = arow["id"]

            upserted += 1
            slugs.append(slug)

            # If latest revision already matches, just point current_revision_id to it
            q_last = """
            SELECT id, revision, config, prompt, knowledge
            FROM assistant_revisions
            WHERE assistant_id=%s
            ORDER BY revision DESC
            LIMIT 1
            """
            last = con.execute(q_last, (assistant_db_id,)).fetchone()
            if last and last["config"] == cfg and (last["prompt"] or "") == prompt and (last["knowledge"] or "") == knowledge:
                con.execute(
                    "UPDATE assistants SET current_revision_id=%s, updated_at=NOW() WHERE id=%s",
                    (last["id"], assistant_db_id),
                )
                continue

            q_next = "SELECT COALESCE(MAX(revision),0)+1 AS rev FROM assistant_revisions WHERE assistant_id=%s"
            rev = con.execute(q_next, (assistant_db_id,)).fetchone()["rev"]

            q_ins = """
            INSERT INTO assistant_revisions (assistant_id, revision, config, prompt, knowledge, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            rid = con.execute(q_ins, (assistant_db_id, rev, Json(cfg), prompt, knowledge, created_by)).fetchone()["id"]

            con.execute(
                "UPDATE assistants SET current_revision_id=%s, updated_at=NOW() WHERE id=%s",
                (rid, assistant_db_id),
            )
            revisions_added += 1

    return {"upserted": upserted, "revisions_added": revisions_added, "slugs": slugs}


def _db_publish(self, slug: str) -> str:
    import secrets
    public_id = secrets.token_urlsafe(18)
    q = """
    UPDATE assistants
    SET is_public=TRUE, public_id=%s, updated_at=NOW()
    WHERE slug=%s
    RETURNING public_id
    """
    with self._conn() as con:
        row = con.execute(q, (public_id, slug)).fetchone()
        if not row:
            raise ValueError("assistant_not_found")
        return row["public_id"]


def _db_unpublish(self, slug: str) -> None:
    q = """
    UPDATE assistants
    SET is_public=FALSE, public_id=NULL, updated_at=NOW()
    WHERE slug=%s
    """
    with self._conn() as con:
        con.execute(q, (slug,))


def _db_rotate_public_id(self, slug: str) -> str:
    import secrets
    public_id = secrets.token_urlsafe(18)
    q = """
    UPDATE assistants
    SET public_id=%s, is_public=TRUE, updated_at=NOW()
    WHERE slug=%s
    RETURNING public_id
    """
    with self._conn() as con:
        row = con.execute(q, (public_id, slug)).fetchone()
        if not row:
            raise ValueError("assistant_not_found")
        return row["public_id"]


# Bind (overwrite to guarantee availability)
DBAssistantStore.seed_from_filesystem = _db_seed_from_filesystem
DBAssistantStore.publish = _db_publish
DBAssistantStore.unpublish = _db_unpublish
DBAssistantStore.rotate_public_id = _db_rotate_public_id

