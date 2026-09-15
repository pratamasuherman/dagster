"""
Feature assets — output NLP ditulis balik ke warehouse (schema feature).

Langkah 15:
  - comment_relevance_scores: cosine(comment_text, caption) -> 0-100.
    ⚠️ Diubah 2026-09-04 dari REPLACE penuh ke INCREMENTAL (UPSERT) -- sebelumnya
    encode ULANG SEMUA histori komentar tiap run (linear makin lambat seiring
    histori bertambah), padahal comment_sentiment_scores di sebelahnya sudah lama
    incremental dengan pola yang sama. Sekarang cuma comment yang BELUM ada skornya
    yang di-encode (LEFT JOIN ... WHERE s.comment_id IS NULL, sama pola dengan
    _fetch_comments_for_sentiment). Caption yang berubah SETELAH komentar sudah
    discore TIDAK memicu re-score otomatis -- trade-off yang diterima karena caption
    post jarang berubah setelah publish.
  - word_frequencies: top-50 kata per (brand, platform), TETAP REPLACE penuh --
    ini cuma regex tokenize + Counter (murah, bukan model inference), dan perlu
    scan SEMUA komentar tiap run supaya top-N kata akurat mencakup seluruh histori.
  - Jalan SETELAH unified_comment & unified_post.
  - Setelah tulis, invalidate cache Redis tiap brand yang datanya berubah.

Tambahan — comment_sentiment_scores:
  - Sentimen komentar (positive/neutral/negative) pakai model IndoRoBERTa
    (w11wo/indonesian-roberta-base-sentiment-classifier).
  - INCREMENTAL (UPSERT), BEDA dari comment_relevance_scores yang REPLACE penuh —
    inference transformer jauh lebih berat daripada cosine similarity, jadi
    re-score seluruh histori tiap run bakal boros compute & waktu.
  - Jalan SETELAH unified_comment saja (tidak butuh caption/unified_post).

Tambahan 2026-09-08 — post_caption_sentiment_scores & tagged_post_caption_sentiment_scores:
  - Section "Audience Sentiment" di report butuh sentiment BUKAN cuma dari komentar,
    tapi juga dari caption post sendiri dan caption tagged post (UGC, IG-only).
    Comment di tagged post TIDAK bisa discore -- raw-nya nggak pernah ada sama
    sekali (unified_comment.post_id vs unified_tagged_post.post_id overlap = 0).
  - Keduanya REUSE compute_sentiment_scores() dari comment_sentiment_scorer.py
    APA ADANYA (tidak diubah) -- dict input di-mapping field-nya
    (comment_id<-post_id, comment_text<-caption) sebelum dipanggil, lalu
    di-mapping balik saat INSERT. Ini supaya jalur comment_sentiment_scores yang
    sudah production TIDAK disentuh sama sekali.
  - Sama seperti comment_sentiment_scores: INCREMENTAL (UPSERT), tanpa Redis
    invalidation (comment_sentiment_scores juga tidak invalidate Redis).
"""

from __future__ import annotations

from dagster import asset, Output, AssetKey
from autometric.pg_compat import execute_values

from autometric.resources import (
    PostgresResource,
    SentenceTransformerResource,
    RedisResource,
    SentimentModelResource,
)
from autometric.feature.comment_relevance_scorer import (
    compute_relevance_scores,
    compute_word_frequencies,
)
from autometric.feature.comment_sentiment_scorer import compute_sentiment_scores


def _fetch_comments(postgres: PostgresResource) -> list[dict]:
    """Ambil semua comment yang punya teks. brand_id di-cast ke str (UUID)."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT comment_id, platform, brand_id::text, post_id, comment_text
                FROM l1_silver.unified_comment
                WHERE comment_text IS NOT NULL AND comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_new_comments_for_relevance(postgres: PostgresResource) -> list[dict]:
    """Ambil comment yang BELUM punya skor relevansi (incremental, sama pola
    seperti _fetch_comments_for_sentiment). Butuh post_id (beda dari fetch
    sentiment) karena relevansi butuh JOIN ke caption post induk. Comment
    tanpa teks di-skip."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.comment_id, c.platform, c.brand_id::text, c.post_id, c.comment_text
                FROM l1_silver.unified_comment c
                LEFT JOIN feature.comment_relevance_scores s
                    ON s.comment_id = c.comment_id AND s.platform = c.platform
                WHERE s.comment_id IS NULL
                  AND c.comment_text IS NOT NULL AND c.comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_captions(postgres: PostgresResource) -> dict[tuple[str, str], str]:
    """Mapping (post_id, platform) -> caption. Hanya post yang punya caption."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT post_id, platform, caption
                FROM l1_silver.unified_post
                WHERE caption IS NOT NULL AND caption <> ''
            """)
            return {(r[0], r[1]): r[2] for r in cur.fetchall()}
    finally:
        conn.close()


def _fetch_comments_for_sentiment(postgres: PostgresResource) -> list[dict]:
    """Ambil comment yang BELUM punya skor sentimen (incremental, beda dari
    _fetch_comments di atas yang REPLACE penuh). Comment tanpa teks di-skip,
    sama seperti relevance scorer."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.comment_id, c.platform, c.brand_id::text, c.comment_text
                FROM l1_silver.unified_comment c
                LEFT JOIN feature.comment_sentiment_scores s
                    ON s.comment_id = c.comment_id AND s.platform = c.platform
                WHERE s.comment_id IS NULL
                  AND c.comment_text IS NOT NULL AND c.comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_post_captions_for_sentiment(postgres: PostgresResource) -> list[dict]:
    """Ambil caption post yang BELUM punya skor sentimen (incremental, sama pola
    seperti _fetch_comments_for_sentiment). Field di-rename ke comment_id/
    comment_text supaya bisa langsung dipakai compute_sentiment_scores() tanpa
    ubah modul itu."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.post_id AS comment_id, p.platform, p.brand_id::text, p.caption AS comment_text
                FROM l1_silver.unified_post p
                LEFT JOIN feature.post_caption_sentiment_scores s
                    ON s.post_id = p.post_id AND s.platform = p.platform
                WHERE s.post_id IS NULL
                  AND p.caption IS NOT NULL AND p.caption <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_tagged_post_captions_for_sentiment(postgres: PostgresResource) -> list[dict]:
    """Ambil caption tagged post (IG-only, UGC) yang BELUM punya skor sentimen.
    Sama pola field-rename seperti _fetch_post_captions_for_sentiment."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.post_id AS comment_id, t.platform, t.brand_id::text, t.caption AS comment_text
                FROM l1_silver.unified_tagged_post t
                LEFT JOIN feature.tagged_post_caption_sentiment_scores s
                    ON s.post_id = t.post_id AND s.platform = t.platform
                WHERE s.post_id IS NULL
                  AND t.caption IS NOT NULL AND t.caption <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@asset(
    group_name="feature",
    deps=[AssetKey("unified_comment"), AssetKey("unified_post")],  # Langkah 15: setelah keduanya
    kinds={"postgres", "python"},
    description=(
        "Skor relevansi comment vs caption (0-100), INCREMENTAL (UPSERT, sejak "
        "2026-09-04) -- cuma comment yang belum discore yang di-encode. "
        "+ word_frequencies (REPLACE penuh, murah). Invalidate Redis untuk brand yang berubah."
    ),
)
def comment_relevance_scores(
    postgres: PostgresResource,
    nlp_model: SentenceTransformerResource,
    redis: RedisResource,
) -> Output:
    # 1. Ambil comment BARU yang belum discore (incremental -- bagian mahal/encode).
    new_comments = _fetch_new_comments_for_relevance(postgres)
    captions = _fetch_captions(postgres)

    # 2. Hitung skor cuma utk comment baru (CPU/GPU-bound, model sudah dimuat
    #    sekali oleh resource).
    relevance_rows = compute_relevance_scores(new_comments, captions, nlp_model.model)

    # 3. Word frequencies TETAP full recompute -- regex tokenize + Counter itu
    #    murah (bukan model inference), dan perlu SEMUA comment biar top-N kata
    #    per brand/platform akurat mencakup seluruh histori.
    all_comments = _fetch_comments(postgres)
    word_rows = compute_word_frequencies(all_comments, top_n=50)

    # 4. Tulis: relevance INCREMENTAL (UPSERT, tanpa TRUNCATE), word_frequencies
    #    tetap REPLACE penuh -- keduanya dalam satu transaksi.
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            if relevance_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO feature.comment_relevance_scores
                        (comment_id, platform, brand_id, relevance_score)
                    VALUES %s
                    ON CONFLICT (comment_id, platform) DO NOTHING
                    """,
                    [
                        (r["comment_id"], r["platform"], r["brand_id"], r["relevance_score"])
                        for r in relevance_rows
                    ],
                )

            cur.execute("TRUNCATE feature.word_frequencies")
            if word_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO feature.word_frequencies
                        (brand_id, platform, word, frequency)
                    VALUES %s
                    """,
                    [
                        (r["brand_id"], r["platform"], r["word"], r["frequency"])
                        for r in word_rows
                    ],
                )
        conn.commit()
    finally:
        conn.close()

    # 5. Invalidate Redis untuk tiap brand yang datanya berubah (comment baru
    #    discore, ATAU word frequencies-nya berubah karena histori baru).
    brands = {r["brand_id"] for r in relevance_rows} | {r["brand_id"] for r in word_rows}
    deleted = 0
    for b in brands:
        deleted += redis.invalidate_brand(b)

    return Output(
        len(relevance_rows),
        metadata={
            "new_relevance_rows": len(relevance_rows),
            "new_comments_scanned": len(new_comments),
            "word_freq_rows": len(word_rows),
            "comments_scanned_for_wordfreq": len(all_comments),
            "captions_available": len(captions),
            "brands_invalidated": len(brands),
            "redis_keys_deleted": deleted,
        },
    )


@asset(
    group_name="feature",
    deps=[AssetKey("unified_comment")],
    kinds={"postgres", "python"},
    description=(
        "Skor sentimen komentar (positive/neutral/negative) pakai model "
        "w11wo/indonesian-roberta-base-sentiment-classifier. INCREMENTAL (UPSERT) -- "
        "beda dari comment_relevance_scores yang REPLACE penuh, karena inference "
        "transformer jauh lebih berat daripada cosine similarity. Comment tanpa teks "
        "di-skip (sama seperti comment_relevance_scores)."
    ),
)
def comment_sentiment_scores(
    postgres: PostgresResource,
    sentiment_model: SentimentModelResource,
) -> Output:
    # 1. Ambil comment baru yang belum discore.
    comments = _fetch_comments_for_sentiment(postgres)

    if not comments:
        return Output(0, metadata={"new_comments_scored": 0})

    # 2. Score batch (model dimuat sekali oleh resource lewat setup_for_execution,
    #    sama persis pola nlp_model.model di comment_relevance_scores).
    scored_rows = compute_sentiment_scores(comments, sentiment_model.pipeline)

    # 3. UPSERT (bukan TRUNCATE) -- cuma nambah/update baris yang baru discore.
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO feature.comment_sentiment_scores
                    (comment_id, platform, brand_id, sentiment_label, sentiment_score)
                VALUES %s
                ON CONFLICT (comment_id, platform) DO UPDATE SET
                    sentiment_label = EXCLUDED.sentiment_label,
                    sentiment_score = EXCLUDED.sentiment_score,
                    scored_at       = now()
                """,
                [
                    (r["comment_id"], r["platform"], r["brand_id"], r["sentiment_label"], r["sentiment_score"])
                    for r in scored_rows
                ],
            )
        conn.commit()
    finally:
        conn.close()

    label_counts: dict[str, int] = {}
    for r in scored_rows:
        label_counts[r["sentiment_label"]] = label_counts.get(r["sentiment_label"], 0) + 1

    return Output(
        len(scored_rows),
        metadata={
            "new_comments_scored": len(scored_rows),
            **{f"count_{k}": v for k, v in label_counts.items()},
        },
    )


@asset(
    group_name="feature",
    deps=[AssetKey("unified_post")],
    kinds={"postgres", "python"},
    description=(
        "Skor sentimen caption post (positive/neutral/negative), sama model dengan "
        "comment_sentiment_scores. INCREMENTAL (UPSERT). REUSE compute_sentiment_scores() "
        "apa adanya lewat field-rename (comment_id<-post_id, comment_text<-caption)."
    ),
)
def post_caption_sentiment_scores(
    postgres: PostgresResource,
    sentiment_model: SentimentModelResource,
) -> Output:
    captions = _fetch_post_captions_for_sentiment(postgres)

    if not captions:
        return Output(0, metadata={"new_captions_scored": 0})

    scored_rows = compute_sentiment_scores(captions, sentiment_model.pipeline)

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO feature.post_caption_sentiment_scores
                    (post_id, platform, brand_id, sentiment_label, sentiment_score)
                VALUES %s
                ON CONFLICT (post_id, platform) DO UPDATE SET
                    sentiment_label = EXCLUDED.sentiment_label,
                    sentiment_score = EXCLUDED.sentiment_score,
                    scored_at       = now()
                """,
                [
                    (r["comment_id"], r["platform"], r["brand_id"], r["sentiment_label"], r["sentiment_score"])
                    for r in scored_rows
                ],
            )
        conn.commit()
    finally:
        conn.close()

    label_counts: dict[str, int] = {}
    for r in scored_rows:
        label_counts[r["sentiment_label"]] = label_counts.get(r["sentiment_label"], 0) + 1

    return Output(
        len(scored_rows),
        metadata={
            "new_captions_scored": len(scored_rows),
            **{f"count_{k}": v for k, v in label_counts.items()},
        },
    )


@asset(
    group_name="feature",
    deps=[AssetKey("unified_tagged_post")],
    kinds={"postgres", "python"},
    description=(
        "Skor sentimen caption tagged post (IG-only, UGC), sama model dengan "
        "comment_sentiment_scores. INCREMENTAL (UPSERT). REUSE compute_sentiment_scores() "
        "apa adanya lewat field-rename (comment_id<-post_id, comment_text<-caption). "
        "Comment di tagged post TIDAK discore -- raw-nya tidak pernah ada."
    ),
)
def tagged_post_caption_sentiment_scores(
    postgres: PostgresResource,
    sentiment_model: SentimentModelResource,
) -> Output:
    captions = _fetch_tagged_post_captions_for_sentiment(postgres)

    if not captions:
        return Output(0, metadata={"new_captions_scored": 0})

    scored_rows = compute_sentiment_scores(captions, sentiment_model.pipeline)

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO feature.tagged_post_caption_sentiment_scores
                    (post_id, platform, brand_id, sentiment_label, sentiment_score)
                VALUES %s
                ON CONFLICT (post_id, platform) DO UPDATE SET
                    sentiment_label = EXCLUDED.sentiment_label,
                    sentiment_score = EXCLUDED.sentiment_score,
                    scored_at       = now()
                """,
                [
                    (r["comment_id"], r["platform"], r["brand_id"], r["sentiment_label"], r["sentiment_score"])
                    for r in scored_rows
                ],
            )
        conn.commit()
    finally:
        conn.close()

    label_counts: dict[str, int] = {}
    for r in scored_rows:
        label_counts[r["sentiment_label"]] = label_counts.get(r["sentiment_label"], 0) + 1

    return Output(
        len(scored_rows),
        metadata={
            "new_captions_scored": len(scored_rows),
            **{f"count_{k}": v for k, v in label_counts.items()},
        },
    )


feature_assets = [
    comment_relevance_scores,
    comment_sentiment_scores,
    post_caption_sentiment_scores,
    tagged_post_caption_sentiment_scores,
]