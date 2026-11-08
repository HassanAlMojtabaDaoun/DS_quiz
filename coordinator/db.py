"""PostgreSQL schema and connection helper (the metadata store)."""

import psycopg
from psycopg.rows import dict_row

from coordinator import config


def connect_db():
    return psycopg.connect(config.DATABASE_URL, row_factory=dict_row)


def init_db():
    with connect_db() as conn:
        # Liveness and capacity of every storage node (from heartbeats).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS node_heartbeats (
                node_id TEXT PRIMARY KEY,
                free_bytes BIGINT NOT NULL,
                total_bytes BIGINT NOT NULL,
                chunk_count BIGINT NOT NULL,
                stored_bytes BIGINT NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL
            )
        """)
        conn.execute(
            "ALTER TABLE node_heartbeats ADD COLUMN IF NOT EXISTS url TEXT")

        # Membership of the consistent hashing ring.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ring_members (
                node_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        if conn.execute("SELECT COUNT(*) AS n FROM ring_members"
                        ).fetchone()["n"] == 0:
            for node_id, url in config.SEED_NODES.items():
                conn.execute(
                    "INSERT INTO ring_members (node_id, url) VALUES (%s, %s)",
                    (node_id, url))

        # Buckets and their redundancy policy.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                redundancy TEXT NOT NULL
                    CHECK (redundancy IN ('replication', 'ec')),
                ec_k INT,
                ec_m INT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # One row per version of an object (S3 model: bucket + key + version).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS object_versions (
                bucket TEXT NOT NULL,
                object_key TEXT NOT NULL,
                version BIGINT NOT NULL,
                size BIGINT NOT NULL CHECK (size >= 0),
                chunk_hashes JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (bucket, object_key, version),
                CHECK (jsonb_typeof(chunk_hashes) = 'array')
            )
        """)
        for column in ("object_id UUID UNIQUE", "sha256 TEXT",
                       "chunking TEXT NOT NULL DEFAULT 'fixed'",
                       "policy TEXT NOT NULL DEFAULT 'replication'",
                       "content_type TEXT",
                       "tags JSONB NOT NULL DEFAULT '{}'::jsonb",
                       "etag TEXT"):
            conn.execute(
                f"ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS {column}")
        # Indexes used by listing and search (prefix, size, date, tags).
        for index in (
            "idx_object_versions_created_at ON object_versions (created_at DESC)",
            "idx_object_versions_prefix ON object_versions "
            "(bucket, object_key text_pattern_ops, version DESC)",
            "idx_object_versions_size ON object_versions (size)",
            "idx_object_versions_tags ON object_versions USING GIN (tags)",
        ):
            conn.execute(f"CREATE INDEX IF NOT EXISTS {index}")

        # Multipart uploads in progress (S3 style: initiate, parts, complete).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id UUID PRIMARY KEY,
                bucket TEXT NOT NULL,
                object_key TEXT NOT NULL,
                policy TEXT NOT NULL,
                chunking TEXT NOT NULL,
                content_type TEXT,
                tags JSONB NOT NULL DEFAULT '{}'::jsonb,
                parts JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # API keys (only a SHA-256 of each key is stored).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL
                    CHECK (role IN ('admin', 'readwrite', 'readonly')),
                rate_limit_rps DOUBLE PRECISION,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_used_at TIMESTAMPTZ
            )
        """)
        if config.ADMIN_API_KEY:
            import hashlib
            import uuid
            conn.execute(
                "INSERT INTO api_keys (key_id, name, key_hash, role) "
                "VALUES (%s, 'bootstrap-admin', %s, 'admin') "
                "ON CONFLICT (key_hash) DO NOTHING",
                (uuid.uuid4(),
                 hashlib.sha256(config.ADMIN_API_KEY.encode()).hexdigest()))

        # One row per unique chunk: how it is protected and where it lives.
        #   replication: `nodes` = nodes holding a full copy
        #   ec:          `shards` = [{index, hash, size, node}, ...]
        #                `nodes`  = distinct nodes holding at least one shard
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chunk_catalog (
                chunk_hash TEXT PRIMARY KEY,
                size BIGINT NOT NULL CHECK (size >= 0),
                ref_count BIGINT NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
                nodes JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (jsonb_typeof(nodes) = 'array')
            )
        """)
        for column in ("scheme TEXT NOT NULL DEFAULT 'replication'",
                       "ec_k INT", "ec_m INT", "codec TEXT",
                       "shards JSONB",
                       "policy TEXT NOT NULL DEFAULT 'replication'"):
            conn.execute(
                f"ALTER TABLE chunk_catalog ADD COLUMN IF NOT EXISTS {column}")

        # Deduplication happens within a redundancy policy: the same content
        # in a replicated bucket and in an EC bucket is stored once per
        # policy, so every bucket keeps the protection it asked for.
        pk_columns = conn.execute(
            """
            SELECT COUNT(*) AS n FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                               AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'chunk_catalog'::regclass AND i.indisprimary
            """).fetchone()["n"]
        if pk_columns == 1:
            conn.execute(
                "ALTER TABLE chunk_catalog DROP CONSTRAINT chunk_catalog_pkey, "
                "ADD PRIMARY KEY (chunk_hash, policy)")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunk_catalog_shards
            ON chunk_catalog USING GIN (shards jsonb_path_ops)
        """)

        conn.execute("DROP TABLE IF EXISTS manifests")
