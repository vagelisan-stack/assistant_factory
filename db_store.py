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
