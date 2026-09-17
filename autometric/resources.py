"""
Dagster resources — injected into assets (not globals), so every asset is unit-testable.

Resources:
  - PostgresResource            : connection to autometric_v2 (holds l0_*, l1_silver, feature, gold).
                                  call_procedure() runs CALL sp_sync_*; count_rows() for metadata.
  - SentenceTransformerResource  : loads the multilingual NLP model once per run.
  - SentimentModelResource       : loads the IndoRoBERTa sentiment classifier once per run.
  - RedisResource                : invalidates dashboard cache per brand (brand_id is UUID).

NOTE: Silver/Gold are plain TABLES filled by stored procedures / TRUNCATE+INSERT,
NOT materialized views. There is no REFRESH MATERIALIZED VIEW anywhere.

SECURITY: credentials are NEVER hard-coded. They come from environment variables.
Set these in your shell / .env (do not commit .env):

  AUTOMETRIC_DB_URL = postgresql://<user>:<password>@<host>:5432/autometric_v2
  REDIS_HOST        = <redis host>   (optional, defaults to localhost)
"""

import os

from dagster import ConfigurableResource, InitResourceContext

# WDAC (Application Control) di laptop kantor memblokir DLL native pip-installed
# (psycopg2 binary, psycopg-binary) karena tidak "Enterprise signed". libpq.dll dari
# installer resmi PostgreSQL LOLOS kebijakan itu, jadi psycopg (v3) dipaksa pakai
# mode "python" (ctypes) yang me-load libpq.dll tsb, bukan psycopg-binary/c.
# psycopg cari libpq lewat ctypes.util.find_library, yang menyusuri PATH proses --
# bukan lewat os.add_dll_directory -- jadi folder bin Postgres di-prepend ke PATH
# di sini (bukan diedit permanen di sistem) supaya berjalan sendiri (self-contained).
for _pg_bin in (
    r"C:\Program Files\PostgreSQL\18\bin",
    r"C:\Program Files\PostgreSQL\17\bin",
    r"C:\Program Files\PostgreSQL\16\bin",
):
    if os.path.isdir(_pg_bin):
        os.environ["PATH"] = _pg_bin + os.pathsep + os.environ.get("PATH", "")
        break

import psycopg


class PostgresResource(ConfigurableResource):
    """Wraps a psycopg (v3) connection to the primary autometric_v2 database."""

    connection_string: str  # injected from env in repository.py

    def get_conn(self):
        return psycopg.connect(self.connection_string)

    def call_procedure(self, proc_call: str) -> None:
        """
        Jalankan stored procedure CALL. Dipakai Silver yang berupa TABEL
        (diisi sp_sync_*), bukan materialized view.
        proc_call contoh: 'CALL l1_silver.sp_sync_unified_post()'
        """
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(proc_call)
            conn.commit()
        finally:
            conn.close()

    def count_rows(self, table_name: str) -> int:
        """
        Hitung jumlah baris sebuah tabel/view (buat metadata Dagster).
        table_name harus schema-qualified, mis. 'l1_silver.unified_post'.
        """
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cur.fetchone()[0]
        finally:
            conn.close()


class SentenceTransformerResource(ConfigurableResource):
    """
    Loads the NLP model once per Dagster run, shared across assets.
    Avoids reloading the model for every batch.
    """

    model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"
    _model: object = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(self.model_name)

    @property
    def model(self):
        return self._model


class SentimentModelResource(ConfigurableResource):
    """
    Loads the sentiment classifier once per Dagster run, shared across assets.
    Model: w11wo/indonesian-roberta-base-sentiment-classifier (HuggingFace
    transformers), 3 label: positive / neutral / negative.

    Sama pola persis dengan SentenceTransformerResource di atas -- model
    di-load di setup_for_execution (sekali per run), BUKAN saat Dagster
    daemon start / repository di-load. Ini penting: menghindari nambah beban
    startup timeout, karena setup_for_execution baru jalan waktu job yang
    butuh resource ini beneran dieksekusi.
    """

    model_name: str = "w11wo/indonesian-roberta-base-sentiment-classifier"
    _pipeline: object = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from transformers import pipeline
        self._pipeline = pipeline(
            "sentiment-analysis",
            model=self.model_name,
            tokenizer=self.model_name,
            truncation=True,  # komentar panjang tetap aman meski > 512 token
        )

    @property
    def pipeline(self):
        return self._pipeline


class RedisResource(ConfigurableResource):
    """Invalidates dashboard cache keys for a given brand. brand_id is a UUID string."""

    host: str = "redis.railway.internal"
    port: int = 6379
    ttl_seconds: int = 900

    def invalidate_brand(self, brand_id: str) -> int:
        import redis
        client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        cursor, deleted = 0, 0
        while True:
            cursor, keys = client.scan(cursor, match=f"dash:{brand_id}:*", count=100)
            if keys:
                client.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        return deleted
