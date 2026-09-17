"""
Harmonization assets — isi tabel l0_harmonization.* lewat stored procedure
sp_sync_*_from_raw(). Dipindah dari pg_cron ke Dagster supaya SEMUA orkestrasi
(raw -> harmonization -> silver -> feature -> gold) berada di satu tempat.

Pola sama dengan silver_assets.py: tiap asset cuma memanggil
PostgresResource.call_procedure(), TIDAK punya output data Python.

Pengelompokan: 1 asset PER ENTITAS (post/comment/profile/audience/story),
tiap asset menjalankan procedure platform yang relevan untuk entitas itu.
Ini menjaga jumlah asset tetap wajar sambil memberi lineage bermakna.

Catatan platform (sesuai DB asli, di-update 2026-08-19):
  - post     : facebook, instagram, tiktok
  - comment  : facebook, instagram, tiktok  (sp_sync_tiktok_comment_from_raw
               dibuat 2026-08-19, l0_raw.tt_comments sekarang ada)
  - profile  : facebook, instagram, tiktok
  - audience : instagram, tiktok  (sp_sync_tiktok_audience_from_raw dibuat
               2026-08-19. facebook_audience_from_raw MASIH TIDAK dipanggil
               di sini -- lihat catatan deprecated di harmonized_audience.)
  - story    : instagram saja
  - tagged   : facebook, instagram, tiktok  (sp_sync_facebook_tagged_post_from_raw
               & sp_sync_tiktok_tagged_post_from_raw ditambahkan 2026-09-16,
               l0_raw.fb_tagged_posts & l0_raw.tt_tagged_posts sekarang ada.
               Dipakai Silver via sp_sync_unified_tagged_post yang sudah
               di-UNION 3 platform di sisi DB)

Tambahan 2026-08-19 (audit l0_raw -> l2_gold, lihat NOTES_db_pipeline):
  - harmonized_tiktok_profile_native : l0_extra.tt_profile_native -> kolom
    video_views/profile_reach/profile_views/comments/shares/net_growth/
    new_followers/lost_followers di l0_harmonization.tiktok_profile (sebelumnya
    NULL terus, kolom ada tapi tidak ada sumbernya).
  - gapfilled_profile_dates : isi baris kosong (gap tanggal) profile snapshot
    dari tanggal akun didaftarkan ke public sampai hari ini (LOCF utk state
    field, 0 utk flow field). MineralQUA + kompetitornya sengaja di-exclude
    di dalam proc (data demo/sintetis, trailing gap-nya memang batas dataset,
    bukan kegagalan scraping) -- brand lain & akun baru otomatis ter-handle.
  - normalized_audience_percentage : koreksi drift pembulatan audience
    berformat persentase (total 95-105 -> tepat 100); format absolut (raw
    count API asli) tidak disentuh.

l0_raw dideklarasikan sebagai SourceAsset: diisi oleh ingest fullstack dev
(API Meta/TikTok), Dagster hanya mengamati sebagai titik awal lineage.
"""

from dagster import (
    asset,
    AssetKey,
    SourceAsset,
    Output,
)
from autometric.resources import PostgresResource


# --- Source layer: l0_raw (diisi ingest API di luar Dagster) ---------------
l0_raw = SourceAsset(
    key=AssetKey(["l0_raw"]),
    description="Diisi proses ingest (API Meta/TikTok) di luar Dagster. Titik awal lineage.",
    group_name="l0",
)


# --- Helper: jalankan satu/lebih procedure harmonization -------------------
def _harmonize(postgres: PostgresResource, procs: list[str], label: str) -> Output:
    for p in procs:
        postgres.call_procedure(f"CALL l0_harmonization.{p}()")
    return Output(len(procs), metadata={"procedures_dijalankan": procs, "entitas": label})


# --- Harmonization assets (per entitas) -----------------------------------
@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_post (facebook, instagram, tiktok).",
)
def harmonized_post(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_post_from_raw",
        "sp_sync_instagram_post_from_raw",
        "sp_sync_tiktok_post_from_raw",
    ], "post")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_comment (facebook, instagram, tiktok).",
)
def harmonized_comment(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_comment_from_raw",
        "sp_sync_instagram_comment_from_raw",
        "sp_sync_tiktok_comment_from_raw",
    ], "comment")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_profile (facebook, instagram, tiktok).",
)
def harmonized_profile(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_profile_from_raw",
        "sp_sync_instagram_profile_from_raw",
        "sp_sync_tiktok_profile_from_raw",
    ], "profile")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description=(
        "raw -> l0_harmonization.instagram_audience + tiktok_audience. "
        "sp_sync_tiktok_audience_from_raw dibuat 2026-08-19 (tabel "
        "tiktok_audience sebelumnya tidak ada sama sekali). "
        "CATATAN PENTING: facebook_audience_from_raw MASIH SENGAJA TIDAK "
        "dipanggil di sini -- proc lamanya di-deprecated tim ini karena "
        "'struktur raw FB audience berubah' (lihat komentar asli). Pada "
        "2026-08-19 proc itu di-REPLACE dari sisi DB (support format flat-"
        "object percentage, bukan cuma nested Meta Graph API), TAPI belum "
        "diverifikasi terhadap struktur raw FB audience PRODUCTION yang "
        "sekarang -- JANGAN aktifkan sp_sync_facebook_audience_from_raw di "
        "sini sebelum dicek ulang oleh yang paham alasan deprecated aslinya."
    ),
)
def harmonized_audience(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_audience_from_raw",
        "sp_sync_tiktok_audience_from_raw",
    ], "audience")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.instagram_story (instagram saja).",
)
def harmonized_story(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_story_from_raw",
    ], "story")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description=(
        "raw -> l0_harmonization.*_tagged_post (facebook, instagram, tiktok). "
        "facebook_tagged_post & tiktok_tagged_post ditambahkan 2026-09-16 "
        "(l0_raw.fb_tagged_posts & l0_raw.tt_tagged_posts, sumber rencana: "
        "Meta Graph API & TikTok Business API, bukan Apify). Sudah tersambung "
        "ke hilir lewat sp_sync_unified_tagged_post (di-UNION 3 platform) dan "
        "seterusnya ke ugc_tagged_posts & tagged_post_caption_sentiment_scores."
    ),
)
def harmonized_tagged_post(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_tagged_post_from_raw",
        "sp_sync_facebook_tagged_post_from_raw",
        "sp_sync_tiktok_tagged_post_from_raw",
    ], "tagged_post")


# --- Tambahan 2026-08-19 (audit l0_raw -> l2_gold) --------------------------

@asset(
    group_name="harmonization",
    deps=[harmonized_profile],
    kinds={"postgres"},
    description=(
        "l0_extra.tt_profile_native -> l0_harmonization.tiktok_profile "
        "(video_views, profile_reach, profile_views, comments, shares, "
        "net_growth, new_followers, lost_followers). UPDATE ke row yang "
        "dibuat harmonized_profile (match brand_id+date) -- HARUS jalan "
        "setelahnya, bukan independen."
    ),
)
def harmonized_tiktok_profile_native(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_tiktok_profile_native_from_extra",
    ], "tiktok_profile_native")


@asset(
    group_name="harmonization",
    deps=[harmonized_profile, harmonized_tiktok_profile_native],
    kinds={"postgres"},
    description=(
        "Isi baris kosong (gap tanggal) di l0_harmonization.*_profile dari "
        "tanggal akun didaftarkan ke public (LEAST(brand_social_accounts."
        "created_at, brand_competitors.created_at)) sampai hari ini. State "
        "field (follower_count, bio, dst) di-LOCF dari data real terakhir; "
        "flow field (likes, net_growth, dst) diisi 0, BUKAN diulang dari "
        "hari sebelumnya. MineralQUA + kompetitornya sengaja di-exclude di "
        "dalam proc (lihat DECLARE v_excluded_brand_id) -- trailing gap "
        "mereka memang batas dataset demo/sintetis, bukan kegagalan "
        "scraping. Akun orphan (tidak link ke brand manapun) otomatis "
        "ter-skip juga (tidak bisa tentukan tanggal registrasi). Re-run "
        "3 proc sp_sync_*_profile_from_raw sesudahnya (idempotent) supaya "
        "followers_growth (LAG per tanggal) dihitung ulang dengan seri "
        "tanggal yang sudah lengkap."
    ),
)
def gapfilled_profile_dates(postgres: PostgresResource) -> Output:
    procs = [
        "sp_gapfill_profile_dates",
        "sp_sync_facebook_profile_from_raw",
        "sp_sync_instagram_profile_from_raw",
        "sp_sync_tiktok_profile_from_raw",
    ]
    for p in procs:
        postgres.call_procedure(f"CALL l0_harmonization.{p}()")
    return Output(len(procs), metadata={
        "procedures_dijalankan": procs,
        "entitas": "gapfill_profile",
        "catatan": "3 proc terakhir re-run murni utk recalc followers_growth, bukan re-sync data baru",
    })


@asset(
    group_name="harmonization",
    deps=[harmonized_audience],
    kinds={"postgres"},
    description=(
        "Koreksi drift pembulatan pada l0_harmonization.*_audience. Deteksi "
        "otomatis per grup (brand_id, date, audience_type, dimension_key): "
        "kalau total value 95-105 dianggap format persentase -> dikoreksi "
        "presisi ke tepat 100 (largest-remainder ke kategori value terbesar). "
        "Kalau total jauh dari 100 (format absolut, raw follower count dari "
        "API asli) -> TIDAK disentuh sama sekali."
    ),
)
def normalized_audience_percentage(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_normalize_audience_percentage_rounding",
    ], "normalize_audience")


# Dikumpulkan untuk import di repository.py
harmonization_source_assets = [l0_raw]
harmonization_assets = [
    harmonized_post,
    harmonized_comment,
    harmonized_profile,
    harmonized_audience,
    harmonized_story,
    harmonized_tagged_post,
    harmonized_tiktok_profile_native,
    gapfilled_profile_dates,
    normalized_audience_percentage,
]