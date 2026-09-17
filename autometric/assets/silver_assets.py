"""
Silver assets — isi tabel l1_silver.* lewat stored procedure sp_sync_*.

PENTING: Silver di DB ini adalah TABEL biasa yang diisi stored procedure
(CALL l1_silver.sp_sync_*), BUKAN materialized view.

PERUBAHAN (orkestrasi penuh di Dagster):
  Dulu Silver depend pada SourceAsset l0_harmonization (diisi pg_cron).
  Sekarang harmonization JUGA asset Dagster (lihat harmonization_assets.py),
  jadi Silver depend pada harmonization assets yang relevan. Tidak ada pg_cron.

Pemetaan dependency entitas:
  unified_post        <- harmonized_post
  unified_comment     <- harmonized_comment  (DAN unified_post; Langkah 10)
  unified_audience    <- normalized_audience_percentage  (bukan langsung
                          harmonized_audience -- lihat catatan 2026-08-19)
  unified_profile     <- gapfilled_profile_dates  (bukan langsung
                          harmonized_profile -- lihat catatan 2026-08-19)
  unified_story       <- harmonized_story
  unified_tagged_post <- harmonized_tagged_post   (FB/IG/TikTok sejak 2026-09-16, UGC)

CATATAN 2026-08-19 (audit l0_raw -> l2_gold):
  unified_profile dan unified_audience sekarang depend ke asset TAMBAHAN
  (gapfilled_profile_dates, normalized_audience_percentage di
  harmonization_assets.py), BUKAN langsung ke harmonized_profile/
  harmonized_audience -- supaya gap-fill & koreksi pembulatan SELALU jalan
  duluan sebelum Silver baca datanya. Kedua asset baru itu tetap depend
  transitif ke harmonized_profile/harmonized_audience, jadi urutan raw ->
  harmonization tetap terjaga.

Freshness (Langkah 11): 25 jam via build_last_update_freshness_checks.
  CATATAN: unified_tagged_post TIDAK diikutkan freshness check — UGC sifatnya
  sporadis, tidak update harian, jadi guard 25 jam akan sering false-alarm.
"""

from datetime import timedelta

from dagster import (
    asset,
    AssetKey,
    Output,
    build_last_update_freshness_checks,
)
from autometric.resources import PostgresResource
from autometric.assets.harmonization_assets import (
    harmonized_post,
    harmonized_comment,
    harmonized_story,
    harmonized_tagged_post,
    gapfilled_profile_dates,
    normalized_audience_percentage,
)


# --- Helper: tiap asset cuma manggil procedure & lapor row count -----------
def _sync(postgres: PostgresResource, proc: str, table: str) -> Output:
    postgres.call_procedure(f"CALL l1_silver.{proc}()")
    n = postgres.count_rows(f"l1_silver.{table}")
    return Output(n, metadata={"rows": n, "table": f"l1_silver.{table}"})


# --- Silver assets --------------------------------------------------------
@asset(
    group_name="silver",
    # unified_profile HARUS sync duluan: sp_sync_unified_post membaca
    # unified_profile utk followers_on_post_day (carry-forward). Pakai AssetKey
    # karena unified_profile didefinisikan di bawah (hindari masalah urutan).
    deps=[harmonized_post, AssetKey("unified_profile")],
    kinds={"postgres"},
    description="Sinkronkan l1_silver.unified_post via sp_sync_unified_post(). Butuh unified_profile (followers_on_post_day).",
)
def unified_post(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_post", "unified_post")


@asset(
    group_name="silver",
    deps=[unified_post, harmonized_comment],  # Langkah 10: comment SETELAH post
    kinds={"postgres"},
    description="Sinkronkan l1_silver.unified_comment via sp_sync_unified_comment(). Jalan setelah unified_post.",
)
def unified_comment(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_comment", "unified_comment")


@asset(
    group_name="silver",
    deps=[normalized_audience_percentage],
    kinds={"postgres"},
    description=(
        "Sinkronkan l1_silver.unified_audience via sp_sync_unified_audience() "
        "(union instagram + facebook + tiktok, lihat catatan 2026-08-19). "
        "Depend ke normalized_audience_percentage supaya koreksi drift "
        "pembulatan sudah jalan duluan."
    ),
)
def unified_audience(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_audience", "unified_audience")


@asset(
    group_name="silver",
    deps=[gapfilled_profile_dates],
    kinds={"postgres"},
    description=(
        "Sinkronkan l1_silver.unified_profile via sp_sync_unified_profile(). "
        "Depend ke gapfilled_profile_dates (bukan langsung harmonized_profile) "
        "supaya baris gap-filled ikut kebaca."
    ),
)
def unified_profile(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_profile", "unified_profile")


@asset(
    group_name="silver",
    deps=[harmonized_story],
    kinds={"postgres"},
    description="Sinkronkan l1_silver.unified_story via sp_sync_unified_story() (IG-only). Independen.",
)
def unified_story(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_story", "unified_story")


@asset(
    group_name="silver",
    deps=[harmonized_tagged_post],
    kinds={"postgres"},
    description="Sinkronkan l1_silver.unified_tagged_post via sp_sync_unified_tagged_post() (FB/IG/TikTok, UGC). Independen.",
)
def unified_tagged_post(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_tagged_post", "unified_tagged_post")


# Dikumpulkan biar gampang di-import di repository.py
silver_assets = [
    unified_post,
    unified_comment,
    unified_audience,
    unified_profile,
    unified_story,
    unified_tagged_post,
]


# --- Freshness 25 jam (Langkah 11) ----------------------------------------
# unified_tagged_post sengaja TIDAK diikutkan (lihat catatan di docstring atas).
silver_freshness_checks = build_last_update_freshness_checks(
    assets=[unified_post, unified_comment, unified_audience, unified_profile, unified_story],
    lower_bound_delta=timedelta(hours=25),
)