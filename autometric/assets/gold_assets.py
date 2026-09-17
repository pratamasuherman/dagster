"""
Gold assets — isi tabel l2_gold.* (mart) lewat stored procedure sp_build_*.

Pola sama dengan silver/harmonization: asset cuma CALL procedure + lapor row count.
Mart Gold = tabel agregat harian (BUKAN hypertable; UPSERT/REPLACE di dalam procedure).

Dependency (dari analisis FROM/JOIN tiap procedure):
  mart_brand_metric_daily      <- unified_post, unified_comment, unified_profile
  mart_comment_activity        <- unified_comment, feature.comment_relevance_scores
  mart_community_contributors  <- unified_post, unified_comment, feature (Langkah 17)
  mart_content_attributes      <- unified_post
  mart_pillar_performance      <- unified_post, unified_story
  mart_story_funnel            <- unified_profile, unified_story
  mart_tiktok_churn            <- unified_profile
  ugc_tagged_posts             <- unified_tagged_post   (IG-only, UGC)
  audience_demographics_daily  <- unified_audience      (age+gender, IG-only)
  audience_geo_daily           <- unified_audience      (city+country unnest, IG-only)
  posting_time_heatmap         <- unified_post          (weekday x hour, WIB)
  dim_content_pillar           <- pillar_performance_daily  (dimensi/konfig pillar, umbrella brand)
  comment_relevance_distribution <- comment_relevance_scores (distribusi tier komentar, umbrella brand)
  post_comment_timeline        <- unified_comment, unified_post  (timeline komentar per post, WIB harian)
  audience_sentiment_monthly   <- comment_sentiment_scores, post_caption_sentiment_scores,
                                   tagged_post_caption_sentiment_scores (3 source_type, bulanan)
  comment_wordcloud_sentiment  <- unified_comment, comment_sentiment_scores (word cloud per sentiment/bulan)
  ytd_performance               <- mart_brand_metric_daily (public.ytd_setting, bukan Silver/Feature)
  kpi_achievement               <- mart_brand_metric_daily (public.kpi_setting, bukan Silver/Feature)

Dua mart (comment_activity, community_contributors) baca feature.comment_relevance_scores,
jadi depend pada asset comment_relevance_scores (Feature).

CATATAN: v_campaign_posts (Langkah 18) adalah VIEW, bukan asset (dibuat saat diminta,
tidak dijadwalkan). Tidak disertakan di sini.
"""

from dagster import asset, Output, AssetKey
from autometric.pg_compat import execute_values

from autometric.resources import PostgresResource
from autometric.feature.comment_relevance_scorer import (
    compute_wordcloud_per_post,
    compute_wordcloud_per_sentiment_month,
)


# Asset keys Silver & Feature (untuk deps lintas-file tanpa import langsung)
_POST = AssetKey("unified_post")
_COMMENT = AssetKey("unified_comment")
_PROFILE = AssetKey("unified_profile")
_AUDIENCE = AssetKey("unified_audience")
_STORY = AssetKey("unified_story")
_TAGGED = AssetKey("unified_tagged_post")
_FEATURE = AssetKey("comment_relevance_scores")
_SENTIMENT_FEATURE = AssetKey("comment_sentiment_scores")
_POST_CAPTION_SENTIMENT_FEATURE = AssetKey("post_caption_sentiment_scores")
_TAGGED_CAPTION_SENTIMENT_FEATURE = AssetKey("tagged_post_caption_sentiment_scores")


def _build(postgres: PostgresResource, proc: str, table: str) -> Output:
    postgres.call_procedure(f"CALL l2_gold.{proc}()")
    n = postgres.count_rows(f"l2_gold.{table}")
    return Output(n, metadata={"rows": n, "table": f"l2_gold.{table}"})


# unified_story kini SUDAH jadi asset Silver (lihat silver_assets.py),
# jadi mart yang butuh story depend padanya dengan benar.


@asset(
    group_name="gold",
    deps=[_POST, _COMMENT, _PROFILE],
    kinds={"postgres"},
    description="mart_engagement_trend (harian) via sp_build_brand_metric_daily().",
)
def mart_brand_metric_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_brand_metric_daily", "brand_metric_daily")


@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description=(
        "l2_gold.post_metric (grain per-post) via sp_build_post_metric(). "
        "Proyeksi langsung dari unified_post: engagement owned/public + 4 ER per post. "
        "Developer query tabel ini langsung (Model 1)."
    ),
)
def post_metric(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_post_metric", "post_metric")


@asset(
    group_name="gold",
    # 2026-09-04: deps _FEATURE dihapus -- sp_build_comment_activity() cuma baca
    # l1_silver.unified_comment + public.brand_social_accounts, TIDAK pernah
    # menyentuh schema feature (dikonfirmasi dari definisi SP di DB). Deps lama
    # bikin asset ini nunggu comment_relevance_scores (langkah NLP paling lambat)
    # tanpa alasan -- sekarang bisa jalan paralel.
    deps=[_COMMENT],
    kinds={"postgres"},
    description="mart_comment_activity via sp_build_comment_activity(). Independen dari Feature layer.",
)
def mart_comment_activity(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_activity", "comment_activity_daily")


# 2026-09-04: procedure diubah dari TRUNCATE+INSERT ke UPSERT -- leaderboard nggak lagi
# full rebuild tiap run; user yang jatuh dari window/ranking TIDAK otomatis hilang,
# rank_in_window/composite_score lama bisa nyangkut basi.
@asset(
    group_name="gold",
    deps=[_POST, _COMMENT, _FEATURE],   # Langkah 17: setelah NLP
    kinds={"postgres"},
    description="mart_community_contributors via sp_build_community_contributors(). Setelah NLP.",
)
def mart_community_contributors(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_community_contributors", "community_contributors")


@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description="mart_content_attributes via sp_build_content_attribute_daily().",
)
def mart_content_attributes(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_content_attribute_daily", "content_attribute_daily")


@asset(
    group_name="gold",
    deps=[_POST, _STORY],
    kinds={"postgres"},
    description="mart_pillar_performance via sp_build_pillar_performance().",
)
def mart_pillar_performance(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_pillar_performance", "pillar_performance_daily")


@asset(
    group_name="gold",
    deps=[_PROFILE, _STORY],
    kinds={"postgres"},
    description="mart_story_funnel via sp_build_story_funnel() (IG-only).",
)
def mart_story_funnel(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_story_funnel", "story_metric_daily")


@asset(
    group_name="gold",
    deps=[_PROFILE],
    kinds={"postgres"},
    description="mart_tiktok_churn via sp_build_tiktok_churn() (TikTok-only).",
)
def mart_tiktok_churn(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_tiktok_churn", "tiktok_churn_daily")


# 2026-09-04: procedure diubah dari TRUNCATE+INSERT ke UPSERT -- tagged post yang
# tag-nya dicabut/dihapus dari sumber TIDAK otomatis hilang dari tabel ini lagi.
@asset(
    group_name="gold",
    deps=[_TAGGED],
    kinds={"postgres"},
    description=(
        "mart ugc_tagged_posts (Tagged Posts, Audience Deep Dive) via "
        "sp_build_ugc_tagged_posts(). Facebook, Instagram, TikTok -- sebelumnya "
        "IG-only, diperluas 2026-09-16 lewat harmonized_tagged_post + "
        "sp_sync_unified_tagged_post. FB/TikTok masih kosong sampai ingest "
        "l0_raw.fb_tagged_posts / l0_raw.tt_tagged_posts (Meta Graph API / "
        "TikTok Business API) berjalan."
    ),
)
def ugc_tagged_posts(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_ugc_tagged_posts", "ugc_tagged_posts")


# --- Audience marts (Gap #2 PETA_GOLD_DASHBOARD) --------------------------
# Dua mart baru dari l1_silver.unified_audience (IG-only saat ini).
# age/gender sudah kolom flat di Silver; city/country jsonb di-unnest di procedure.
# Melayani halaman AUDIENCE DEEP DIVE: Age Distribution, Gender Split, Top Audience Cities.
@asset(
    group_name="gold",
    deps=[_AUDIENCE],
    kinds={"postgres"},
    description=(
        "l2_gold.audience_demographics_daily (grain hari x tipe) via "
        "sp_build_audience_demographics_daily(). Age buckets + gender (count, bukan %). "
        "IG-only. Developer query tabel ini langsung (Model 1)."
    ),
)
def audience_demographics_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_audience_demographics_daily", "audience_demographics_daily")


@asset(
    group_name="gold",
    deps=[_AUDIENCE],
    kinds={"postgres"},
    description=(
        "l2_gold.audience_geo_daily (grain hari x tipe x level x key) via "
        "sp_build_audience_geo_daily(). Unnest city_breakdown & country_breakdown (jsonb). "
        "IG-only. Developer query tabel ini langsung (Model 1)."
    ),
)
def audience_geo_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_audience_geo_daily", "audience_geo_daily")


# --- Posting time heatmap (Gap #3 PETA_GOLD_DASHBOARD) --------------------
# Agregat weekday x hour (WIB) dari unified_post. post_date sudah WIB wall-clock,
# jadi EXTRACT(DOW/HOUR) langsung benar.
# 2026-09-04: procedure diubah dari TRUNCATE+INSERT ke UPSERT (ON CONFLICT DO UPDATE).
# Bukan full rebuild lagi -- bucket weekday x hour yang post-nya sudah hilang dari
# sumber TIDAK otomatis ke-nol-kan, bisa nyangkut dengan angka basi.
# Melayani OVERVIEW -> Best Posting Times.
@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description=(
        "l2_gold.posting_time_heatmap (grain weekday x hour, WIB) via "
        "sp_build_posting_time_heatmap(). Best Posting Times (OVERVIEW). "
        "Komponen additive; UPSERT (bukan full rebuild lagi sejak 2026-09-04). "
        "Developer query tabel ini langsung (Model 1)."
    ),
)
def posting_time_heatmap(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_posting_time_heatmap", "posting_time_heatmap")


# --- Dimensi content pillar (Gap #4 PETA_GOLD_DASHBOARD) ------------------
# Tabel konfig/dimensi pillar (nama+warna+urutan). Di-seed dari pillar_performance_daily
# (BUKAN unified_post) supaya brand_id = umbrella -> JOIN FE align. SP pakai DO NOTHING
# jadi tidak menimpa warna/edit user. READ-WRITE untuk app. Melayani CONTENT PILLARS page.
# CATATAN dep: depend ke mart_pillar_performance (Gold->Gold), bukan Silver -- kalau jalan
# duluan sebelum mart terisi, seed-nya kosong.
@asset(
    group_name="gold",
    deps=[mart_pillar_performance],
    kinds={"postgres"},
    description=(
        "l2_gold.dim_content_pillar (dimensi/konfig pillar) via sp_build_dim_content_pillar(). "
        "Seed (brand_id umbrella, content_pillar) dari pillar_performance_daily -> JOIN FE align. "
        "DO NOTHING (tidak menimpa warna user). READ-WRITE untuk app. CONTENT PILLARS page."
    ),
)
def dim_content_pillar(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_dim_content_pillar", "dim_content_pillar")


# --- Comment relevance distribution (Gap #5 PETA_GOLD_DASHBOARD) ----------
# Distribusi komentar per tier relevance (High>75 / Mid 40-75 / Low<40) dari
# feature.comment_relevance_scores. brand_id per-akun -> umbrella via brand_social_accounts
# (pola sama community_contributors). Simpan count (FE hitung pct). AUDIENCE DEEP DIVE.
# 2026-09-04: procedure diubah dari TRUNCATE+INSERT ke UPSERT -- tier yang commentnya
# sudah 0 di run terbaru TIDAK ke-reset, bisa nyangkut dengan comment_count basi.
@asset(
    group_name="gold",
    deps=[_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_relevance_distribution (brand umbrella x platform x tier) via "
        "sp_build_comment_relevance_distribution(). Distribusi tier komentar (High/Mid/Low). "
        "brand_id di-translate ke umbrella via brand_social_accounts. AUDIENCE DEEP DIVE."
    ),
)
def comment_relevance_distribution(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_relevance_distribution", "comment_relevance_distribution")


# --- Comment timeline per post (Campaign Analysis) ------------------------
# Timeline komentar harian per post (WIB). bucket_date = sumbu absolut;
# days_since_post = sumbu relatif (butuh JOIN ke unified_post untuk post_date).
# CATATAN: pakai comment_date (sudah WIB), BUKAN comment_time (UTC).
# 2026-09-04: procedure diubah dari TRUNCATE+INSERT ke UPSERT -- bucket tanggal yang
# commentnya sudah dihapus dari sumber TIDAK ke-reset, bisa nyangkut dengan angka basi.
@asset(
    group_name="gold",
    deps=[_COMMENT, _POST],
    kinds={"postgres"},
    description=(
        "l2_gold.post_comment_timeline (platform x post_id x bucket_date harian, WIB) via "
        "sp_build_post_comment_timeline(). Timeline komentar per post; sumbu absolut (bucket_date) "
        "+ relatif (days_since_post). CAMPAIGN ANALYSIS. Developer query tabel ini langsung (Model 1)."
    ),
)
def post_comment_timeline(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_post_comment_timeline", "post_comment_timeline")


# --- Word cloud per post (Python asset, bukan CALL procedure) -------------
# Beda pola dari mart lain: mart lain memanggil sp_build_* (SQL murni).
# post_wordcloud harus tokenize teks komentar di Python (buang stopword/emoji),
# jadi ia baca Silver, hitung di Python (reuse _tokenize via compute_wordcloud_per_post),
# lalu REPLACE penuh ke l2_gold.post_wordcloud. Pola sama dengan feature_assets.py.
#
# Strategi isi: full REPLACE (TRUNCATE + INSERT). Konsisten dengan Feature layer,
# cocok untuk volume saat ini, dan otomatis membuang kata dari komentar yang sudah
# dihapus/dimoderasi. Bisa diubah ke incremental nanti bila volume besar.
def _fetch_post_comments(postgres: PostgresResource) -> list[dict]:
    """Ambil komentar (post_id, platform, comment_text) untuk hitung word cloud."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT post_id, platform, comment_text
                FROM l1_silver.unified_comment
                WHERE comment_text IS NOT NULL AND comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@asset(
    group_name="gold",
    deps=[_COMMENT],
    kinds={"postgres", "python"},
    description=(
        "l2_gold.post_wordcloud — top-50 kata per (post_id, platform) dari komentar. "
        "Untuk word cloud Campaign Analysis. Baca Silver, tokenize di Python "
        "(reuse _tokenize), full REPLACE. Developer query tabel ini langsung (Model 1)."
    ),
)
def post_wordcloud(postgres: PostgresResource) -> Output:
    comments = _fetch_post_comments(postgres)
    rows = compute_wordcloud_per_post(comments, top_n=50)

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE l2_gold.post_wordcloud")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO l2_gold.post_wordcloud
                        (post_id, platform, word, frequency)
                    VALUES %s
                    """,
                    [(r["post_id"], r["platform"], r["word"], r["frequency"]) for r in rows],
                )
        conn.commit()
    finally:
        conn.close()

    posts = {(r["post_id"], r["platform"]) for r in rows}
    return Output(
        len(rows),
        metadata={
            "rows": len(rows),
            "posts_dengan_wordcloud": len(posts),
            "comments_scanned": len(comments),
            "table": "l2_gold.post_wordcloud",
        },
    )


# --- Comment sentiment (breakdown harian & per post) ----------------------
# Baca feature.comment_sentiment_scores (hasil model IndoRoBERTa). brand_id
# per-akun -> umbrella via brand_social_accounts (pola sama comment_relevance_
# distribution & community_contributors). Simpan count per label (additive),
# bukan persentase -- FE hitung sendiri utk window custom.
@asset(
    group_name="gold",
    deps=[_SENTIMENT_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_sentiment_daily (brand umbrella x platform x hari) via "
        "sp_build_comment_sentiment_daily(). Breakdown count positive/neutral/negative, "
        "additive -- untuk window custom: SUM per kategori, JANGAN AVG kolom ratio."
    ),
)
def comment_sentiment_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_sentiment_daily", "comment_sentiment_daily")


@asset(
    group_name="gold",
    deps=[_SENTIMENT_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_sentiment_post (grain per post) via "
        "sp_build_comment_sentiment_post(). Breakdown count + dominant_sentiment per post."
    ),
)
def comment_sentiment_post(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_sentiment_post", "comment_sentiment_post")


# --- Audience sentiment monthly (report: sentiment section) ---------------
# Gabung 3 sumber (comment, post_caption, tagged_post_caption) jadi satu mart
# additive per brand umbrella x platform x source_type x bulan. Persentase &
# comparison current-vs-previous-month DIHITUNG DI VIEW (l2_gold.v_audience_
# sentiment_mom, LAG per partition), bukan di sini -- pola sama dengan
# v_campaign_posts (VIEW on-demand, tidak dijadwalkan Dagster).
@asset(
    group_name="gold",
    deps=[_SENTIMENT_FEATURE, _POST_CAPTION_SENTIMENT_FEATURE, _TAGGED_CAPTION_SENTIMENT_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.audience_sentiment_monthly (brand umbrella x platform x source_type x bulan) via "
        "sp_build_audience_sentiment_monthly(). source_type = comment | post_caption | "
        "tagged_post_caption. Additive count -- persentase & MoM comparison ada di VIEW "
        "l2_gold.v_audience_sentiment_mom (LAG per partition), bukan di tabel ini."
    ),
)
def audience_sentiment_monthly(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_audience_sentiment_monthly", "audience_sentiment_monthly")


# --- Word cloud per sentiment per bulan (report: sentiment section) -------
# Sama pola dengan post_wordcloud (Python asset, bukan CALL procedure) --
# tokenisasi butuh stopword/emoji handling yang cuma ada di Python
# (reuse _tokenize via compute_wordcloud_per_sentiment_month). Grain beda:
# brand umbrella x platform x sentiment_label x bulan (bukan per post),
# supaya bisa difilter "kata apa yang paling sering muncul di komentar
# positif/negatif bulan ini vs bulan lalu" di report.
def _fetch_comments_with_sentiment_and_month(postgres: PostgresResource) -> list[dict]:
    """Ambil comment_text + brand umbrella + sentiment_label + period_month.
    brand_id di unified_comment sebenarnya social_account_id -> di-translate
    ke brand umbrella via brand_social_accounts (pola sama sp_build_comment_
    sentiment_daily)."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    bsa.brand_id,
                    c.platform,
                    s.sentiment_label,
                    date_trunc('month', c.comment_date)::date AS period_month,
                    c.comment_text
                FROM l1_silver.unified_comment c
                JOIN feature.comment_sentiment_scores s
                    ON s.comment_id = c.comment_id AND s.platform = c.platform
                JOIN public.brand_social_accounts bsa
                    ON bsa.social_account_id = c.brand_id
                WHERE c.comment_text IS NOT NULL AND c.comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@asset(
    group_name="gold",
    deps=[_COMMENT, _SENTIMENT_FEATURE],
    kinds={"postgres", "python"},
    description=(
        "l2_gold.comment_wordcloud_sentiment — top-50 kata per (brand umbrella, platform, "
        "sentiment_label, bulan) dari komentar. Untuk word cloud di section Audience Sentiment. "
        "Baca Silver+Feature, tokenize di Python (reuse _tokenize), full REPLACE."
    ),
)
def comment_wordcloud_sentiment(postgres: PostgresResource) -> Output:
    comments = _fetch_comments_with_sentiment_and_month(postgres)
    rows = compute_wordcloud_per_sentiment_month(comments, top_n=50)

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE l2_gold.comment_wordcloud_sentiment")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO l2_gold.comment_wordcloud_sentiment
                        (brand_id, platform, sentiment_label, period_month, word, frequency)
                    VALUES %s
                    """,
                    [
                        (r["brand_id"], r["platform"], r["sentiment_label"], r["period_month"], r["word"], r["frequency"])
                        for r in rows
                    ],
                )
        conn.commit()
    finally:
        conn.close()

    groups = {(r["brand_id"], r["platform"], r["sentiment_label"], r["period_month"]) for r in rows}
    return Output(
        len(rows),
        metadata={
            "rows": len(rows),
            "groups_dengan_wordcloud": len(groups),
            "comments_scanned": len(comments),
            "table": "l2_gold.comment_wordcloud_sentiment",
        },
    )


# --- YTD performance (public.ytd_setting + public.kpi_setting feature) ----
# Beda pola dari mart lain: sp_calculate_ytd_performance() butuh parameter
# (ytd_id, platform_id, metrics_target) per kombinasi -- bukan sp_build_*()
# tanpa argumen yang rebuild satu mart utuh. Kombinasi metric yang di-track
# per YTD ditentukan user dari app (app CALL procedure langsung saat user
# pertama kali pilih metric -> row pertama masuk ke ytd_performance). Asset
# ini cuma REFRESH kombinasi yang SUDAH ada (ytd_setting.is_active saja --
# YTD yang sudah dinonaktifkan dibiarkan beku, tidak di-recompute tiap hari).
# Procedure sendiri full recompute (delete+reinsert) per kombinasi, jadi
# aman dipanggil ulang tiap hari (idempotent, self-heals kalau brand_metric_daily
# di-backfill/dikoreksi setelah run sebelumnya).
@asset(
    group_name="gold",
    deps=[mart_brand_metric_daily],
    kinds={"postgres"},
    description=(
        "l2_gold.ytd_performance (grain: ytd_id x platform_id x metrics_target x metric_date) via "
        "sp_calculate_ytd_performance(ytd_id, platform_id, metrics_target). Refresh harian untuk "
        "tiap kombinasi yang sudah dipilih user (row sudah ada di ytd_performance) DAN "
        "public.ytd_setting-nya masih is_active. Kombinasi baru didaftarkan oleh app (bukan asset "
        "ini) saat user pertama kali memilih metric untuk sebuah YTD."
    ),
)
def ytd_performance(postgres: PostgresResource) -> Output:
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT yp.ytd_id, yp.platform_id, yp.metrics_target
                FROM l2_gold.ytd_performance yp
                JOIN public.ytd_setting ys ON ys.ytd_id = yp.ytd_id
                WHERE ys.is_active
            """)
            combos = cur.fetchall()
    finally:
        conn.close()

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            for ytd_id, platform_id, metrics_target in combos:
                cur.execute(
                    "CALL l2_gold.sp_calculate_ytd_performance(%s, %s, %s)",
                    (str(ytd_id), str(platform_id), metrics_target),
                )
                conn.commit()  # per-combo commit -- 1 kombinasi gagal tidak nge-rollback yang lain
    finally:
        conn.close()

    n = postgres.count_rows("l2_gold.ytd_performance")
    return Output(
        n,
        metadata={
            "rows": n,
            "combos_refreshed": len(combos),
            "table": "l2_gold.ytd_performance",
        },
    )


# --- KPI achievement (public.kpi_setting) ----------------------------------
# Beda dari ytd_performance: sp_calculate_kpi_achievement(p_kpi_id DEFAULT NULL)
# kalau dipanggil TANPA argumen otomatis loop SEMUA public.kpi_setting yang
# is_active (bukan cuma kombinasi yang sudah ada baris-nya) -- jadi cukup pola
# _build() biasa, tidak perlu query kombinasi dulu seperti ytd_performance.
@asset(
    group_name="gold",
    deps=[mart_brand_metric_daily],
    kinds={"postgres"},
    description=(
        "l2_gold.kpi_achievement via sp_calculate_kpi_achievement(). Refresh harian untuk "
        "semua public.kpi_setting yang is_active. KPI baru didaftarkan oleh app (bukan asset "
        "ini) saat user membuat target KPI."
    ),
)
def kpi_achievement(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_calculate_kpi_achievement", "kpi_achievement")


gold_assets = [
    mart_brand_metric_daily,
    post_metric,
    mart_comment_activity,
    mart_community_contributors,
    mart_content_attributes,
    mart_pillar_performance,
    mart_story_funnel,
    mart_tiktok_churn,
    ugc_tagged_posts,
    audience_demographics_daily,
    audience_geo_daily,
    posting_time_heatmap,
    dim_content_pillar,
    comment_relevance_distribution,
    post_comment_timeline,
    post_wordcloud,
    comment_sentiment_daily,
    comment_sentiment_post,
    audience_sentiment_monthly,
    comment_wordcloud_sentiment,
    ytd_performance,
    kpi_achievement,
]