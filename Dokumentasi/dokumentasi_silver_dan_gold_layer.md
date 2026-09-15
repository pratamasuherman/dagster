# Autometric — Referensi Layer & Dashboard (Detail)

Referensi lengkap struktur Silver & Gold beserta kolom penting, formula, stored procedure, dan pemetaan tiap visual dashboard. Scope platform: **Facebook, Instagram, TikTok**.

- [1. Arsitektur & Prinsip](#1-arsitektur--prinsip)
  - [1.1 Orkestrasi Dagster — Asset Graph & Urutan](#11-orkestrasi-dagster--asset-graph--urutan)
- [2. Layer Silver](#2-layer-silver-l1_silver)
- [3. Feature Layer (NLP)](#3-feature-layer-feature)
- [4. Layer Gold](#4-layer-gold-l2_gold)
- [5. Peta Dashboard → Gold (per visual)](#5-peta-dashboard--gold-per-visual)
- [6. Data Kompetitor (Brand vs Competitor)](#6-data-kompetitor-brand-vs-competitor)

---

## 1. Arsitektur & Prinsip

**Alur pipeline:**

```
l0_raw → l0_harmonization → l1_silver → feature (NLP) → l2_gold → (FastAPI/Redis) → dashboard
```

**Pembagian tanggung jawab:**
- **Dagster** — dependency ordering, scheduling (`daily_pipeline_job`, 03:15 WIB, ⚠️ digeser dari 02:00 WIB — lihat §1.1), freshness (25 jam), eksekusi NLP Python, invalidasi cache, plus 2 sensor event-driven (`new_account_sensor`, `csv_upload_sensor`). Tiap asset hanya memanggil stored procedure + melaporkan row count (kecuali Feature & `post_wordcloud`/`comment_wordcloud_sentiment`, yang jalan Python). Detail asset graph lengkap di [§1.1](#11-orkestrasi-dagster--asset-graph--urutan).
- **Stored procedure** — transformasi data sesungguhnya. `sp_sync_*` mengisi Silver dari harmonization; `sp_build_*` mengisi Gold dari Silver/Feature.
- **Frontend (Model 1)** — query tabel Gold langsung via SQL.

**Prinsip inti:**

- **Hybrid ratio** — Gold menyimpan komponen additive (numerator + denominator) **dan** ratio harian jadi. Untuk rentang custom (7D/30D), hitung `SUM(numerator)/SUM(denominator)`; jangan rata-rata ratio harian (ratio tidak additive).
- **WIB** — seluruh tanggal dinormalisasi ke `Asia/Jakarta`.
- **Brand umbrella** — Silver memakai `brand_id = social_accounts.id` (per-akun). Mart Gold yang butuh level brand naik lewat `public.brand_social_accounts (social_account_id → brand_id)` dengan INNER JOIN (akun yang belum dipetakan tidak masuk — by design).
- **Idempoten** — SP memakai UPSERT (`ON CONFLICT DO UPDATE` dengan guard `IS DISTINCT FROM`) untuk time-series. Re-run otomatis backfill.
  ⚠️ **Perubahan 2026-09-04:** 9 SP snapshot/leaderboard yang tadinya `TRUNCATE + INSERT` (full rebuild tiap run) sudah diganti jadi UPSERT (`ON CONFLICT DO UPDATE`) — lihat daftar & catatan regresi di [§2](#2-layer-silver-l1_silver), [§4.7](#47-community_contributors), [§4.8](#48-comment_relevance_distribution), [§4.9](#49-post_comment_timeline), [§4.13](#413-posting_time_heatmap), [§4.16](#416-lain), dan [§6](#6-data-kompetitor-brand-vs-competitor). **Trade-off penting:** baris yang sumbernya sudah tidak menghasilkan data (post/akun dihapus, user jatuh dari leaderboard, tier kosong) TIDAK lagi otomatis hilang seperti sebelumnya — nyangkut sebagai data basi sampai dibersihkan manual/DELETE terpisah. Ini juga mematahkan mekanisme "self-healing" yang didokumentasikan di `sensors.py` (lihat §1.1 & catatan sensor) yang tadinya mengandalkan TRUNCATE untuk otomatis membersihkan akun yang gagal diproses.
- **Data kompetitor punya jalur terpisah** — Silver & Gold dedicated (`unified_competitor_*`, `competitor_*`), tapi harmonization-nya **shared** dengan brand utama (bukan tabel terpisah). Detail di [§6](#6-data-kompetitor-brand-vs-competitor).

**Timezone per tabel (penting, mudah keliru):**

| Tabel | Kolom | Timezone |
|---|---|---|
| `unified_post` | `post_date` | WIB (wall-clock) |
| `unified_comment` | `comment_time` | UTC |
| `unified_comment` | `comment_date` / `comment_hour` / `comment_weekday` | WIB (sudah +7) |

→ Untuk bucketing waktu komentar, pakai `comment_date`/`comment_hour` (WIB), bukan `comment_time`.

### 1.1 Orkestrasi Dagster — Asset Graph & Urutan

Dikonfirmasi dari kode asset terbaru (`silver_assets.py`, `harmonization_assets.py`, `feature_assets.py`, `gold_assets.py`, `competitor_assets.py`, `jobs.py`, `repository.py`, `resources.py`, `sensors.py` — **direfresh 2026-09-10**, sebelumnya 10 Jul 2026). Ini jawab "gimana ngisinya pas run di Dagster", bukan cuma dari sisi SQL.

**Job & schedule:**
- `daily_pipeline_job` = `AssetSelection.all()` — materialize **seluruh** asset dalam satu run, urutan dijaga otomatis oleh dependency graph (bukan didaftar manual satu-satu).
- `daily_schedule`: cron `15 3 * * *`, timezone `Asia/Jakarta` → **03:15 WIB tiap hari** (⚠️ digeser dari 02:00 WIB — buffer setelah scraper competitor Apify jalan 03:00 WIB, datanya landing ~03:01-03:03 WIB). Seluruh transform (harmonization → gold) dipicu di sini. Nggak ada `pg_cron` lagi sama sekali.
- **Sensor: DUA aktif** (⚠️ bukan `sensors=[]` lagi) — `new_account_sensor` (akun baru siap diolah) dan `csv_upload_sensor` (⚠️ ditambahkan 2026-07-30, sempat digugurkan di blueprint awal karena sistem dianggap API-only, diaktifkan lagi karena kebutuhan riil upload CSV manual). `replica_lag_sensor` tetap gugur (replica Tiger Cloud murni buat HA/failover, pipeline nggak pernah baca dari replica). Detail lengkap ada di `dokumentasi_dagster_autometric.md` §6.
- **Freshness check**: 25 jam, cuma di 5 asset Silver (`unified_post`, `unified_comment`, `unified_audience`, `unified_profile`, `unified_story`) — **`unified_tagged_post` sengaja dikecualikan** karena UGC sifatnya sporadis, guard 25 jam bakal sering false-alarm.
- **Resources**: `SentenceTransformerResource` (model `paraphrase-multilingual-MiniLM-L12-v2`, buat `comment_relevance_scores`) dan `SentimentModelResource` (`w11wo/indonesian-roberta-base-sentiment-classifier`, buat `comment_sentiment_scores` **dan** dua asset caption-sentiment baru 2026-09-08) — keduanya di-load sekali per run lewat `setup_for_execution` (bukan saat Dagster daemon start), biar nggak nambah startup timeout.

**Urutan dependency per layer** (`A ← B` artinya asset A depend ke asset B, harus nunggu B selesai):

| Layer | Asset | Depend ke | Catatan |
|---|---|---|---|
| Harmonization | `harmonized_post` | `l0_raw` (SourceAsset) | FB+IG+TikTok sekaligus (3 proc dalam 1 asset) |
| Harmonization | `harmonized_comment` | `l0_raw` | FB+IG+TikTok (⚠️ proc TikTok — `sp_sync_tiktok_comment_from_raw` — dibuat 2026-08-19, sebelumnya TikTok comment belum ada proc harmonization) |
| Harmonization | `harmonized_profile` | `l0_raw` | FB+IG+TikTok |
| Harmonization | `harmonized_audience` | `l0_raw` | IG+TikTok (⚠️ proc TikTok dibuat 2026-08-19). FB audience masih sengaja tidak dipanggil (struktur raw FB belum diverifikasi ulang) |
| Harmonization | `harmonized_story` | `l0_raw` | IG saja |
| Harmonization | `harmonized_tagged_post` | `l0_raw` | IG saja |
| Harmonization | `harmonized_tiktok_profile_native` ⚠️ baru 2026-08-19 | `harmonized_profile` | Isi kolom TikTok yang sebelumnya NULL terus, dari `l0_extra.tt_profile_native` |
| Harmonization | `gapfilled_profile_dates` ⚠️ baru 2026-08-19 | `harmonized_profile` **DAN** `harmonized_tiktok_profile_native` | Gap-fill tanggal profile + re-run 3 proc `sp_sync_*_profile_from_raw` |
| Harmonization | `normalized_audience_percentage` ⚠️ baru 2026-08-19 | `harmonized_audience` | Koreksi drift pembulatan persentase audience |
| Silver | `unified_profile` | `gapfilled_profile_dates` (⚠️ bukan langsung `harmonized_profile` sejak 2026-08-19) | Independen, tapi **harus selesai duluan** — lihat baris di bawah |
| Silver | `unified_post` | `harmonized_post` **DAN** `unified_profile` | `sp_sync_unified_post` baca `unified_profile` buat `followers_on_post_day` (carry-forward). Kalau `unified_profile` gagal/belum jalan, `unified_post` ikut ketahan di Dagster. |
| Silver | `unified_comment` | `unified_post` **DAN** `harmonized_comment` | Comment sengaja jalan **setelah** post (bukan cuma harmonization) |
| Silver | `unified_audience` | `normalized_audience_percentage` (⚠️ bukan langsung `harmonized_audience` sejak 2026-08-19) | Independen |
| Silver | `unified_story` | `harmonized_story` | Independen |
| Silver | `unified_tagged_post` | `harmonized_tagged_post` | Independen |
| Feature | `comment_relevance_scores` (+ `word_frequencies`) | `unified_comment` **DAN** `unified_post` | Butuh caption post induk |
| Feature | `comment_sentiment_scores` | `unified_comment` saja | Nggak butuh caption/post |
| Feature | `post_caption_sentiment_scores` ⚠️ baru 2026-09-08 | `unified_post` | Sentimen caption POST sendiri, REUSE `compute_sentiment_scores()` via field-rename |
| Feature | `tagged_post_caption_sentiment_scores` ⚠️ baru 2026-09-08 | `unified_tagged_post` | Sentimen caption TAGGED POST (UGC, IG-only) |
| Gold | `mart_brand_metric_daily` → `brand_metric_daily` | `unified_post`, `unified_comment`, `unified_profile` | |
| Gold | `post_metric` | `unified_post` | |
| Gold | `mart_comment_activity` → `comment_activity_daily`+`_hourly` | `unified_comment` | 1 SP isi 2 tabel. ⚠️ Dep ke `comment_relevance_scores` (Feature) DIHAPUS 2026-09-04 — SP-nya tidak pernah baca schema `feature`, dep lama cuma bikin nunggu NLP step tanpa alasan. Sekarang bisa jalan paralel dengan Feature layer. |
| Gold | `mart_community_contributors` → `community_contributors` | `unified_post`, `unified_comment`, `comment_relevance_scores` (Feature) | Sengaja setelah NLP |
| Gold | `mart_content_attributes` → `content_attribute_daily` | `unified_post` | |
| Gold | `mart_pillar_performance` → `pillar_performance_daily` | `unified_post`, `unified_story` | |
| Gold | `mart_story_funnel` → `story_metric_daily`+`story_type_daily` | `unified_profile`, `unified_story` | 1 SP isi 2 tabel |
| Gold | `mart_tiktok_churn` → `tiktok_churn_daily` | `unified_profile` | |
| Gold | `ugc_tagged_posts` | `unified_tagged_post` | |
| Gold | `audience_demographics_daily` | `unified_audience` | |
| Gold | `audience_geo_daily` | `unified_audience` | |
| Gold | `posting_time_heatmap` | `unified_post` | |
| Gold | `dim_content_pillar` | **`mart_pillar_performance`** (asset Gold, bukan Silver!) | ⚠️ Kalau `dim_content_pillar` somehow jalan sebelum `pillar_performance_daily` terisi, seed-nya kosong. |
| Gold | `comment_relevance_distribution` | `comment_relevance_scores` (Feature) | |
| Gold | `post_comment_timeline` | `unified_comment`, `unified_post` | |
| Gold | `post_wordcloud` (Python) | `unified_comment` | TRUNCATE+INSERT, di luar `sp_build_*` (tidak diubah) |
| Gold | `comment_sentiment_daily` | `comment_sentiment_scores` (Feature) | |
| Gold | `comment_sentiment_post` | `comment_sentiment_scores` (Feature) | |
| Gold | `audience_sentiment_monthly` ⚠️ baru 2026-09-08 | `comment_sentiment_scores`, `post_caption_sentiment_scores`, `tagged_post_caption_sentiment_scores` | Gabung 3 source_type jadi 1 mart bulanan |
| Gold | `comment_wordcloud_sentiment` (Python) ⚠️ baru 2026-09-08 | `unified_comment`, `comment_sentiment_scores` | Word cloud per sentiment/bulan, TRUNCATE+INSERT |
| Gold | `ytd_performance` ⚠️ baru 2026-09-08 | `mart_brand_metric_daily` | Refresh harian kombinasi YTD yang sudah `is_active` |
| Kompetitor (Silver) | `unified_competitor_post` | `harmonized_post` (asset **sama** dengan brand utama) | |
| Kompetitor (Silver) | `unified_competitor_profile_daily` | `gapfilled_profile_dates` (⚠️ bukan langsung `harmonized_profile` sejak 2026-08-19) | |
| Kompetitor (Gold) | `competitor_post_metric` | `unified_competitor_post` | |
| Kompetitor (Gold) | `competitor_profile_metric_daily` | `unified_competitor_profile_daily` **DAN** `competitor_post_metric` | Proc gold-nya JOIN ke `unified_competitor_post` buat agregasi harian |

⚠️ **Diperbarui 2026-09-10:** total sekarang 9 asset Harmonization (dari 6), 4 asset
Feature (dari 2), dan 21 asset Gold (dari 18, di luar kompetitor) — lihat
`dokumentasi_dagster_autometric.md` §3 untuk breakdown & wave/topological order
lengkap (berubah dari 7 wave jadi 9 wave karena rantai `gapfilled_profile_dates`
menambah 2 langkah di critical path). Beberapa komentar di kode masih basi (nggak
sinkron sama asset yang beneran terdaftar) — dicatat biar nggak bingung kalau baca
kodenya langsung: docstring `jobs.py` masih bilang "8 gold mart assets" & "1
feature asset". Asset graph (yang beneran dieksekusi Dagster) yang jadi acuan,
bukan komentarnya.

---

## 2. Layer Silver (`l1_silver`)

Data ternormalisasi per entitas. Dibangun asset Silver (`sp_sync_*`) dari `l0_harmonization`. Semua tabel di bawah pakai UPSERT (`ON CONFLICT ... DO UPDATE`) kecuali disebutkan lain — jadi re-run SP aman, nggak bikin duplikat. Info grain/kolom di bawah confirmed via `pg_constraint` + `information_schema.columns` (10 Jul 2026).

### `unified_post` — grain: 1 baris / post

**Builder:** `sp_sync_unified_post()` · **PK:** `(id, post_date)` · **UNIQUE (conflict key):** `(platform, brand_id, post_id, post_date)` · **Duplicate handling:** UPSERT.

Sumber metrik konten utama. `brand_id` = per-akun. `post_date` = WIB (disimpan sebagai `timestamp without time zone`, sudah dikonversi ke WIB, bukan UTC).

- **Identitas/metadata:** platform, brand_id, post_id, post_date, title, caption, link, cover_image, post_type, `duration_s`, slide_count, `content_pillar` (text label, bukan FK), `hashtag_list` (array — sumber buat pillar hashtag-matching di Gold), brand_offering, `format`, flag (is_campaign, is_boosted, is_collab, is_aon, is_activity, is_event, is_repost).
- **Metrik mentah:** views, reach, impressions, reactions, likes, comments, shares, saves, repost_count, link_click, follows, profile_visits, avg_watch_time, video_view_total_time.
- **Metrik terhitung:** engagement (owned), engagement_public, er_reach/er_views/er_impressions/er_followers, followers_on_post_day, `engagement_rate_base` (text: 'reach'/'views'), completion_rate, reels_skip_rate.
- **Lineage/audit:** source_id, extra_source_id, facebook/instagram/tiktok_post_row_id, source_table, extra_source_table, source_post_key, created_at, updated_at.

**Formula engagement per platform:**

| Kolom | FB | IG | TikTok |
|---|---|---|---|
| `likes` | = reactions | likes | likes |
| `engagement` (owned) | reactions+comments+shares | likes+comments+shares+saves+reposts | likes+comments+shares+saves |
| `engagement_public` | = owned | likes+comments | = owned |
| `engagement_rate_base` | reach | reach | views |

`followers_on_post_day` = carry-forward dari snapshot `unified_profile` terakhir dengan `profile_date ≤ post_date`.

**Kolom & tipe (59 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| platform | character varying | NO |
| source_id | bigint | YES |
| extra_source_id | bigint | YES |
| facebook_post_row_id | bigint | YES |
| instagram_post_row_id | bigint | YES |
| tiktok_post_row_id | bigint | YES |
| brand_id | uuid | NO |
| post_id | text | NO |
| post_date | timestamp without time zone | NO |
| title | text | YES |
| caption | text | YES |
| link | text | YES |
| cover_image | text | YES |
| post_type | text | YES |
| duration_s | numeric | YES |
| slide_count | integer | YES |
| views | bigint | YES |
| reach | bigint | YES |
| impressions | bigint | YES |
| reactions | bigint | YES |
| likes | bigint | YES |
| comments | bigint | YES |
| shares | bigint | YES |
| saves | bigint | YES |
| repost_count | bigint | YES |
| engagement | bigint | YES |
| total_interactions | bigint | YES |
| engagement_rate | numeric | YES |
| engagement_rate_base | text | YES |
| link_click | bigint | YES |
| follows | bigint | YES |
| profile_visits | bigint | YES |
| avg_watch_time | numeric | YES |
| video_view_total_time | bigint | YES |
| completion_rate | text | YES |
| content_pillar | text | YES |
| brand_offering | text | YES |
| format | text | YES |
| is_collab | boolean | YES |
| is_aon | boolean | YES |
| is_campaign | boolean | YES |
| is_boosted | boolean | YES |
| is_activity | boolean | YES |
| is_event | boolean | YES |
| is_repost | boolean | YES |
| source_table | text | YES |
| extra_source_table | text | YES |
| source_post_key | text | YES |
| created_at | timestamp without time zone | YES |
| updated_at | timestamp without time zone | YES |
| engagement_public | bigint | YES |
| followers_on_post_day | bigint | YES |
| er_reach | numeric | YES |
| er_views | numeric | YES |
| er_impressions | numeric | YES |
| er_followers | numeric | YES |
| reels_skip_rate | numeric | YES |
| hashtag_list | ARRAY | YES |

### `unified_comment` — grain: 1 baris / komentar

**Builder:** `sp_sync_unified_comment()` · **PK:** `(id, comment_date)` · **UNIQUE (conflict key):** `(platform, brand_id, post_id, comment_id, comment_date)` · **Duplicate handling:** UPSERT.

platform, brand_id (per-akun), post_id, post_date, comment_id, `comment_time` (UTC), `comment_date`/`comment_hour`/`comment_weekday` (WIB), comment_text, comment_username, normalized_username, likes_count, replies_count. Linkage ke post via (`post_id`, `platform`).

**Kolom & tipe (25 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| platform | character varying | NO |
| brand_id | uuid | NO |
| post_id | text | NO |
| post_date | timestamp without time zone | YES |
| comment_id | text | NO |
| link_post | text | YES |
| link_comment | text | YES |
| comment_time | timestamp without time zone | YES |
| comment_date | date | NO |
| comment_hour | smallint | YES |
| comment_weekday | smallint | YES |
| comment_text | text | YES |
| comment_username | text | YES |
| normalized_username | text | YES |
| likes_count | bigint | YES |
| replies_count | bigint | YES |
| comment_count | integer | YES |
| source_table | text | YES |
| source_comment_key | text | YES |
| created_at | timestamp without time zone | YES |
| updated_at | timestamp without time zone | YES |
| facebook_comment_row_id | bigint | YES |
| instagram_comment_row_id | bigint | YES |
| tiktok_comment_row_id | bigint | YES |

### `unified_profile` — grain: 1 baris / (akun, tanggal)

**Builder:** `sp_sync_unified_profile()` · **PK:** `(id, profile_date)` · **UNIQUE (conflict key):** `(platform, brand_id, profile_date)` · **Duplicate handling:** UPSERT.

Snapshot channel harian. `profile_date` = WIB. Kolom: follower_count, following_count, page_like_count, account_total_post_count, followers_growth, new_followers, lost_followers, net_growth, profile_reach, profile_visit, content_views, link_clicks, profile_link_taps, likes, comments, shares, saves, replies, repost, total_interactions, **accounts_engaged** (jumlah akun unik yang engage — IG Insights; FB/TikTok = 0).
⚠️ TikTok: follower/profile terbatas (banyak 0/NULL — batasan platform).

**Kolom & tipe (33 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| platform | character varying | NO |
| source_id | bigint | YES |
| brand_id | uuid | NO |
| profile_date | date | NO |
| follower_count | bigint | YES |
| following_count | bigint | YES |
| page_like_count | bigint | YES |
| account_total_post_count | bigint | YES |
| followers_growth | bigint | YES |
| new_followers | bigint | YES |
| lost_followers | bigint | YES |
| net_growth | bigint | YES |
| profile_reach | bigint | YES |
| profile_visit | bigint | YES |
| content_views | bigint | YES |
| link_clicks | bigint | YES |
| profile_link_taps | bigint | YES |
| likes | bigint | YES |
| comments | bigint | YES |
| shares | bigint | YES |
| saves | bigint | YES |
| replies | bigint | YES |
| repost | bigint | YES |
| total_interactions | bigint | YES |
| source_table | text | YES |
| source_profile_key | text | YES |
| created_at | timestamp without time zone | YES |
| updated_at | timestamp without time zone | YES |
| facebook_profile_row_id | bigint | YES |
| instagram_profile_row_id | bigint | YES |
| tiktok_profile_row_id | bigint | YES |
| accounts_engaged | bigint | YES |

### `unified_audience` — grain: 1 baris / (akun, tanggal, tipe)

**Builder:** `sp_sync_unified_audience()` · **PK:** `(id, audience_date)` · **UNIQUE (conflict key):** `(platform, brand_id, audience_date, audience_type)` · **Duplicate handling:** UPSERT.

Demografi audience. IG-only. 2 `audience_type`: `follower_demographics` & `engaged_audience_demographics`.
- **Age (kolom flat numeric):** age_13_17, age_18_24, age_25_34, age_35_44, age_45_54, age_55_64, age_65_plus.
- **Gender (kolom flat numeric):** gender_female, gender_male, gender_unknown.
- **Geo (jsonb `{"key": count}`):** `city_breakdown` (key "Kota, Region"), `country_breakdown` (key ISO-2).

Nilai = count (bukan %). Age dan gender adalah breakdown independen (total bisa beda).

**Kolom & tipe (22 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| platform | character varying | NO |
| brand_id | uuid | NO |
| audience_date | date | NO |
| audience_type | character varying | NO |
| age_13_17 | numeric | YES |
| age_18_24 | numeric | YES |
| age_25_34 | numeric | YES |
| age_35_44 | numeric | YES |
| age_45_54 | numeric | YES |
| age_55_64 | numeric | YES |
| age_65_plus | numeric | YES |
| gender_female | numeric | YES |
| gender_male | numeric | YES |
| gender_unknown | numeric | YES |
| city_breakdown | jsonb | YES |
| country_breakdown | jsonb | YES |
| source_table | text | NO |
| source_audience_key | text | NO |
| instagram_audience_row_ids | ARRAY | YES |
| created_at | timestamp without time zone | YES |
| updated_at | timestamp without time zone | YES |

### `unified_story` — grain: 1 baris / story (IG)

**Builder:** `sp_sync_unified_story()` · **PK:** `(id, story_date)` · **UNIQUE (conflict key):** `(platform, brand_id, story_id, story_date)` · **Duplicate handling:** UPSERT.

reach, views, replies, taps_forward/back, exits, swipe_up, follows. IG-only.

**Kolom & tipe (26 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| platform | character varying | NO |
| brand_id | uuid | NO |
| story_id | text | NO |
| story_date | timestamp without time zone | NO |
| story_type | text | YES |
| link | text | YES |
| cover_image | text | YES |
| reach | bigint | YES |
| views | bigint | YES |
| replies | bigint | YES |
| shares | bigint | YES |
| reposts | bigint | YES |
| taps_forward | bigint | YES |
| taps_back | bigint | YES |
| exits | bigint | YES |
| swipe_up | bigint | YES |
| follows | bigint | YES |
| total_interactions | bigint | YES |
| profile_activity | bigint | YES |
| profile_visit | bigint | YES |
| instagram_story_row_id | bigint | YES |
| source_table | text | YES |
| source_story_key | text | YES |
| created_at | timestamp without time zone | YES |
| updated_at | timestamp without time zone | YES |

### `unified_tagged_post` — grain: 1 baris / tagged post (IG)

**Builder:** `sp_sync_unified_tagged_post()` · **PK/UNIQUE (conflict key):** `(post_id, platform)` · **Duplicate handling:** UPSERT.

Post yang men-tag brand (UGC). IG-only.

**Kolom & tipe (11 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| post_id | text | NO |
| platform | text | NO |
| post_date | timestamp with time zone | YES |
| caption | text | YES |
| post_type | text | YES |
| link_post | text | YES |
| like_count | integer | NO |
| comment_count | integer | NO |
| username | text | YES |
| updated_at | timestamp with time zone | NO |

### `unified_competitor_post` — grain: 1 baris / (social_account_id, post_id) kompetitor

**Builder:** `sp_sync_unified_competitor_post()` · **PK:** `(id)` surrogate · **UNIQUE:** `(social_account_id, post_id)` — **sekarang dipakai sebagai conflict target** (⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT ke UPSERT) · **Duplicate handling: UPSERT** (`ON CONFLICT (social_account_id, post_id) DO UPDATE`). Konsisten sekarang dengan konvensi Silver lain. **Trade-off:** post kompetitor yang sudah tidak dihasilkan sumber (dihapus/akun di-disconnect) tidak lagi otomatis hilang — beda dari perilaku TRUNCATE sebelumnya.

Detail lengkap + keterbatasan data per platform (`reach`/`comments`/`shares`/`saves` yang NULL per platform) ada di [§6.2](#62-layer-silver-kompetitor).

**Kolom & tipe (14 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| social_account_id | uuid | NO |
| platform | character varying | NO |
| post_id | character varying | NO |
| post_date | timestamp with time zone | YES |
| post_date_wib | date | YES |
| caption | text | YES |
| post_type | character varying | YES |
| like_count | integer | YES |
| comment_count | integer | YES |
| share_count | integer | YES |
| view_count | integer | YES |
| save_count | integer | YES |
| updated_at | timestamp with time zone | NO |

### `unified_competitor_profile_daily` — grain: 1 baris / (social_account_id, tanggal) kompetitor

**Builder:** `sp_sync_unified_competitor_profile_daily()` · **PK:** `(id)` surrogate · **UNIQUE:** `(social_account_id, metric_date)` — **sekarang dipakai sebagai conflict target** (⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT ke UPSERT) · **Duplicate handling: UPSERT** (`ON CONFLICT (social_account_id, metric_date) DO UPDATE`). Trade-off sama seperti `unified_competitor_post` di atas.

Lebih ramping dari `unified_profile` — nggak ada `lost_followers`, `net_growth`, `profile_reach`, dst. Detail di [§6.2](#62-layer-silver-kompetitor).

**Kolom & tipe (10 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| social_account_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| follower_count | integer | YES |
| following_count | integer | YES |
| post_count | integer | YES |
| followers_growth | integer | YES |
| like_count | integer | YES |
| updated_at | timestamp with time zone | NO |

---

## 3. Feature Layer (`feature`)

Output NLP, di antara Silver & Gold. **Tidak ada stored procedure di schema `feature`** (confirmed — query `pg_proc` return kosong) — 3 tabel di sini diisi langsung oleh kode Python (Dagster asset `comment_relevance_scores` dan `comment_sentiment_scores`, `psycopg2`/`execute_values`), bukan `CALL sp_xxx()`.

⚠️ **Koreksi dari versi dokumentasi sebelumnya** — nggak semua 3 tabel di sini UPSERT. Confirmed dari `feature_assets.py` (10 Jul 2026):

| Tabel | Asset Dagster | Duplicate handling | Kenapa |
|---|---|---|---|
| `comment_relevance_scores` | `comment_relevance_scores` | **INCREMENTAL** (`INSERT ... ON CONFLICT DO NOTHING`, ⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT) | Encode ulang SEMUA histori tiap run makin lambat seiring histori bertambah — sekarang cuma comment yang **belum** ada skornya yang di-fetch+encode (`LEFT JOIN ... WHERE s.comment_id IS NULL`, pola sama persis `comment_sentiment_scores`). Caption yang berubah setelah comment discore TIDAK memicu re-score otomatis. |
| `word_frequencies` | `comment_relevance_scores` (1 asset, 2 tabel) | **TRUNCATE + INSERT** (REPLACE penuh, TIDAK diubah) | Regex tokenize + Counter itu murah (bukan model inference) — tetap scan SEMUA comment tiap run supaya top-N kata per brand/platform akurat mencakup seluruh histori. |
| `comment_sentiment_scores` | `comment_sentiment_scores` | **UPSERT** (`ON CONFLICT (comment_id, platform) DO UPDATE`) | Inference transformer (IndoRoBERTa) jauh lebih berat daripada cosine similarity — re-score seluruh histori tiap run boros compute. Query fetch-nya sendiri cuma ambil comment yang **belum** ada skornya (`LEFT JOIN ... WHERE s.comment_id IS NULL`) — pola yang sejak 2026-09-04 juga dipakai `comment_relevance_scores`. |

Setelah `comment_relevance_scores`+`word_frequencies` selesai ditulis, asset ini **invalidate cache Redis** (`dash:{brand_id}:*`) untuk tiap brand yang datanya berubah. ⚠️ Ini satu-satunya asset di seluruh pipeline yang eksplisit invalidate Redis — asset Gold lain (termasuk `comment_sentiment_daily`/`_post`) nggak melakukan ini di kode yang ada, jadi kalau dashboard kelihatan nge-cache data sentiment yang basi, ini kemungkinan penyebabnya (bukan bug SP, tapi memang belum ada invalidation-nya).

### `comment_relevance_scores`

**PK:** `(comment_id, platform)` · **Duplicate handling: INCREMENTAL** (`ON CONFLICT DO NOTHING`, ⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT). · **Asset deps:** `unified_comment` + `unified_post` (butuh caption post induk).

Skor cosine similarity komentar vs caption post induk (SentenceTransformer, model `paraphrase-multilingual-MiniLM-L12-v2`, dimuat sekali per run lewat `setup_for_execution`). Butuh caption post induk (join `post_id`, `platform`). Comment tanpa teks (`comment_text IS NULL/''`) di-skip. Cuma comment yang **belum** ada baris di tabel ini yang di-encode tiap run — comment lama tidak di-re-score walau captionnya berubah belakangan.

**Kolom & tipe (5 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| comment_id | text | NO |
| platform | character varying | NO |
| brand_id | uuid | NO |
| relevance_score | numeric | NO |
| scored_at | timestamp with time zone | NO |

### `comment_sentiment_scores` ⚠️ belum ada di dokumentasi sebelumnya

**PK:** `(comment_id, platform)` · **Duplicate handling: UPSERT** (`ON CONFLICT (comment_id, platform) DO UPDATE`, dari Python). · **Asset deps:** `unified_comment` saja (nggak butuh caption/`unified_post`).

Skor sentimen per komentar dari model `w11wo/indonesian-roberta-base-sentiment-classifier` (HuggingFace `pipeline("sentiment-analysis")`, `truncation=True` biar komentar >512 token aman). `sentiment_label` ∈ {positive, neutral, negative}, `sentiment_score` = confidence dari model, `model_name` disimpan buat traceability kalau model diganti di masa depan. Comment tanpa teks di-skip (sama seperti relevance scorer).

**Kolom & tipe (7 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| comment_id | text | NO |
| platform | character varying | NO |
| brand_id | uuid | NO |
| sentiment_label | character varying | NO |
| sentiment_score | numeric | NO |
| model_name | text | NO |
| scored_at | timestamp without time zone | NO |

### `word_frequencies`

**PK:** `(brand_id, platform, word)` · **Duplicate handling: TRUNCATE + INSERT** (satu transaksi bareng `comment_relevance_scores`, asset Dagster yang sama). · **Asset deps:** `unified_comment` + `unified_post` (ikut asset `comment_relevance_scores`).

Top-N kata per brand (top_n=50, stopword ID+EN dibuang). `brand_id` di sini per-akun (sama seperti asalnya, `unified_comment`/`unified_post`).

**Kolom & tipe (5 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| word | text | NO |
| frequency | integer | NO |
| scored_at | timestamp with time zone | NO |

### `post_caption_sentiment_scores` / `tagged_post_caption_sentiment_scores` ⚠️ baru 2026-09-08

**Asset Dagster:** `post_caption_sentiment_scores` (deps `unified_post`) dan
`tagged_post_caption_sentiment_scores` (deps `unified_tagged_post`, IG-only UGC).
**Duplicate handling: UPSERT** (`ON CONFLICT (post_id, platform) DO UPDATE`, dari
Python — pola identik `comment_sentiment_scores`).

Sentimen caption POST/TAGGED POST sendiri (bukan komentar), model & pipeline sama
persis dengan `comment_sentiment_scores` — REUSE `compute_sentiment_scores()` apa
adanya lewat field-rename (`comment_id`←`post_id`, `comment_text`←`caption`) di
`feature_assets.py`, supaya jalur `comment_sentiment_scores` yang sudah production
tidak disentuh. Comment di tagged post **tidak** ikut discore (raw-nya nggak
pernah ada — overlap `unified_comment.post_id` vs `unified_tagged_post.post_id` = 0).

Kolom yang confirmed dari `INSERT` di kode (⚠️ tipe & kolom lengkap belum
diverifikasi ulang ke DB per baris ini, cuma dari SQL asset):
`post_id`, `platform`, `brand_id`, `sentiment_label`, `sentiment_score`, plus
kemungkinan `scored_at` (di-`SET ... scored_at = now()` saat UPDATE, sama pola
`comment_sentiment_scores`).

---

## 4. Layer Gold (`l2_gold`)

Agregat siap pakai. Tiap tabel dibangun 1 asset Dagster (`CALL sp_build_*`, kecuali `post_wordcloud` = Python, `v_campaign_posts` = VIEW). Grain/PK dan duplicate-handling di bawah confirmed via `pg_constraint` + definisi SP (10 Jul 2026) — beberapa beda dari yang tercatat sebelumnya, ditandai ⚠️.

### 4.1 `brand_metric_daily`

**Builder:** `sp_build_brand_metric_daily()` · **PK:** `(brand_id, account_id, platform, metric_date)` · **brand_id = umbrella** (JOIN `brand_social_accounts`) · **Duplicate handling:** UPSERT `ON CONFLICT (brand_id, account_id, platform, metric_date) DO UPDATE`.

Agregat harian channel & konten. Melayani mayoritas KPI Overview/Content/Audience.

- **Komponen additive:** post_count, engagement_sum, engagement_public_sum, reach_sum, views_sum, impressions_sum, likes_sum, comments_sum, shares_sum, saves_sum, reposts_sum, video_views_sum, new_followers_sum, lost_followers_sum, net_growth_sum.
- **Denominator ER:** reach_denom_sum, views_denom_sum, impressions_denom_sum, followers_denom_sum, er_denominator_sum.
- **Channel EOD/flow:** follower_count_eod (snapshot), profile_visit_sum, profile_reach_sum, `accounts_engaged_sum` (IG-only; KPI "IG Accounts Engaged" → FE filter `platform='instagram'`).
- **Ratio harian jadi (Hybrid):** er_reach_daily, er_views_daily, er_impressions_daily, er_followers_daily. Untuk rentang >1 hari, hitung dari komponen — jangan AVG kolom ini.

⚠️ **Kolom `er_*_daily` di atas HANYA untuk trend/window ER umum** (mis. chart "Engagement Over Time", ER biasa). **Jangan** dipakai untuk KPI **`Avg. ER Reach/Views/Followers`** — itu metrik beda, sumbernya `post_metric`, lihat catatan di bawah.

**Cara hitung `Avg. ER Reach/Views/Followers` (channel-level KPI, formula resmi `metriks_launch.xlsx`, keputusan 03 Jul 2026):**

```
Avg. ER Reach     = SUM(er_reach per post)     / TOTAL POST
Avg. ER Views     = SUM(er_views per post)     / TOTAL POST
Avg. ER Followers = SUM(er_followers per post) / TOTAL POST
```

Rata-rata sederhana ER **per post** (tiap post "1 suara", terlepas besar-kecil reach-nya) — beda dari ER window/trend di atas yang weighted by reach/views. **Sumber: `l2_gold.post_metric`, bukan `brand_metric_daily`**, karena granularity yang dibutuhkan per-post:

```sql
SELECT AVG(er_reach) AS avg_er_reach
FROM l2_gold.post_metric
WHERE brand_id = ? AND platform = ? AND post_date BETWEEN ? AND ?;
```

`AVG()` Postgres otomatis skip baris NULL (post dengan reach=0) dari pembilang *dan* `TOTAL POST`-nya — kalau launch spec maksud `TOTAL POST` = semua post tayang (termasuk reach=0), hasilnya beda dari `AVG()` biasa; belum dikonfirmasi karena belum ada kasus reach=0 di data. Berlaku per platform sesuai `engagement_rate_base`: IG & TikTok → Reach/Views/Followers (tanpa Impressions); FB → Reach/Impressions/Followers (tanpa Views).

**Kolom & tipe (33 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| account_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| post_count | bigint | NO |
| engagement_sum | bigint | NO |
| reach_sum | bigint | NO |
| views_sum | bigint | NO |
| impressions_sum | bigint | NO |
| likes_sum | bigint | NO |
| comments_sum | bigint | NO |
| shares_sum | bigint | NO |
| saves_sum | bigint | NO |
| video_views_sum | bigint | NO |
| er_denominator_sum | bigint | NO |
| new_followers_sum | bigint | NO |
| lost_followers_sum | bigint | NO |
| net_growth_sum | bigint | NO |
| built_at | timestamp without time zone | NO |
| reposts_sum | bigint | YES |
| engagement_public_sum | bigint | YES |
| reach_denom_sum | bigint | YES |
| views_denom_sum | bigint | YES |
| impressions_denom_sum | bigint | YES |
| followers_denom_sum | bigint | YES |
| follower_count_eod | bigint | YES |
| profile_visit_sum | bigint | YES |
| profile_reach_sum | bigint | YES |
| er_reach_daily | numeric | YES |
| er_views_daily | numeric | YES |
| er_impressions_daily | numeric | YES |
| er_followers_daily | numeric | YES |
| accounts_engaged_sum | bigint | YES |

### 4.2 `post_metric`

**Builder:** `sp_build_post_metric()` · **PK:** `(platform, brand_id, post_id, post_date)` · **brand_id = per-akun** (langsung dari `unified_post`, TIDAK di-roll-up) · **Duplicate handling:** UPSERT `ON CONFLICT (platform, brand_id, post_id, post_date) DO UPDATE`.

Proyeksi 1:1 dari `unified_post`, plus resolusi `content_pillar_id`. Kolom: identitas (termasuk `hashtag_list`, `link`, `cover_image`), `content_pillar` (text) + `content_pillar_id` (FK ke `dim_content_pillar.id`), `format`, is_campaign/is_boosted, metrik mentah (termasuk `follows` — New Follow from Content, dipatch 3 Jul 2026), engagement_owned, engagement_public, er_reach/views/impressions/followers, avg_watch_time, `duration_s`, completion_rate, reels_skip_rate. Melayani Top Posts, Post Type Performance, Reel Watch Time, TikTok scatter, Campaign Post Grid. (Untuk visual watch-time/scatter, FE filter `duration_s > 0`.)

⚠️ **Mekanisme `content_pillar_id` (hashtag matching)** — ini belum pernah terdokumentasi sebelumnya. Di dalam SP, resolusinya:

```sql
LEFT JOIN LATERAL (
    SELECT dcp.id
    FROM public.brand_social_accounts bsa
    JOIN l2_gold.dim_content_pillar dcp
      ON dcp.brand_id = bsa.brand_id
     AND dcp.hashtags && p.hashtag_list   -- overlap array hashtag
    WHERE bsa.social_account_id = p.brand_id
    ORDER BY dcp.id ASC                    -- tie-break: pillar id terkecil (dibuat paling awal)
    LIMIT 1
) pillar_match ON TRUE
```

Logikanya: cari pillar yang `hashtags`-nya overlap sama `hashtag_list` post, lewat SEMUA brand umbrella yang akun ini terhubung (kasus BNI — 1 akun bisa >1 brand). **Wajib `LATERAL ... LIMIT 1`, bukan `LEFT JOIN` biasa** — kalau plain JOIN, 1 akun yang mapped ke >1 brand bisa fan-out jadi >1 baris dengan `ON CONFLICT` key yang sama (`platform, brand_id, post_id, post_date` — brand_id di sini konstan karena per-akun) → `CardinalityViolation` (ini bug yang sama kayak yang pernah kefix di `sp_build_post_metric` sebelumnya). Kalau nggak ada hashtag yang match, `content_pillar_id` NULL — FE / SP lain fallback ke `content_pillar` (text label) apa adanya.

**Kolom & tipe (35 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| post_id | text | NO |
| post_date | timestamp without time zone | NO |
| post_type | text | YES |
| caption | text | YES |
| link | text | YES |
| cover_image | text | YES |
| content_pillar | text | YES |
| format | text | YES |
| is_campaign | boolean | YES |
| is_boosted | boolean | YES |
| likes | bigint | YES |
| comments | bigint | YES |
| shares | bigint | YES |
| saves | bigint | YES |
| reposts | bigint | YES |
| reach | bigint | YES |
| views | bigint | YES |
| impressions | bigint | YES |
| followers_on_post_day | bigint | YES |
| engagement_owned | bigint | YES |
| engagement_public | bigint | YES |
| er_reach | numeric | YES |
| er_views | numeric | YES |
| er_impressions | numeric | YES |
| er_followers | numeric | YES |
| avg_watch_time | numeric | YES |
| completion_rate | text | YES |
| reels_skip_rate | numeric | YES |
| built_at | timestamp without time zone | NO |
| duration_s | numeric | YES |
| follows | bigint | YES |
| hashtag_list | ARRAY | YES |
| content_pillar_id | bigint | YES |

### 4.3 `pillar_performance_daily`

**Builder:** `sp_build_pillar_performance()` · **PK:** `(brand_id, platform, metric_date, content_pillar)` · **brand_id = umbrella** · **Duplicate handling:** UPSERT.

post_count, engagement_sum, er_denominator_sum, reach_sum, views_sum, watch_time_sum. JOIN ke `dim_content_pillar` via (brand_id, content_pillar) — ⚠️ lihat catatan di 4.13, JOIN ini sekarang berbasis nama pillar, bukan lagi unique constraint. Content Pillars → Comparison; TikTok Deep → Watch Time by Pillar.

**Kolom & tipe (11 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| content_pillar | text | NO |
| post_count | bigint | NO |
| engagement_sum | bigint | NO |
| er_denominator_sum | bigint | NO |
| reach_sum | bigint | NO |
| views_sum | bigint | NO |
| watch_time_sum | numeric | NO |
| built_at | timestamp without time zone | NO |

### 4.4 `content_attribute_daily`

**Builder:** `sp_build_content_attribute_daily()` · **PK:** `(brand_id, platform, metric_date, content_tag)` ⚠️ **(sebelumnya tercatat cuma `brand × content_tag × tanggal` — sebenarnya ada `platform` juga di grain)** · **brand_id = umbrella** · **Duplicate handling:** UPSERT.

post_count, engagement_sum, er_denominator_sum. Overview → Content Attribute Breakdown.

**Kolom & tipe (8 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| content_tag | character varying | NO |
| post_count | bigint | NO |
| engagement_sum | bigint | NO |
| er_denominator_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.5 `comment_activity_daily` / `comment_activity_hourly`

**Builder:** `sp_build_comment_activity()` · **PK daily:** `(brand_id, platform, metric_date)` · **PK hourly:** `(brand_id, platform, metric_date, hour_of_day)` ⚠️ **(sebelumnya cuma "brand × hari/jam" — ada `platform` juga)** · **brand_id = umbrella** · **Duplicate handling:** UPSERT (keduanya).

Jumlah komentar (+ likes_sum, replies_sum di versi daily). Community → Comment Volume, Comment Activity by Hour.

**`comment_activity_daily` (7 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| comment_count | bigint | NO |
| likes_sum | bigint | NO |
| replies_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

**`comment_activity_hourly` (6 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| hour_of_day | smallint | NO |
| comment_count | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.6 `comment_sentiment_daily` / `comment_sentiment_post` ⚠️ belum ada di dokumentasi sebelumnya

**Builder:** `sp_build_comment_sentiment_daily()` + `sp_build_comment_sentiment_post()` · sumber: `feature.comment_sentiment_scores` JOIN `unified_comment`.

⚠️ **Grain `brand_id` beda antara 2 tabel ini — jangan disamakan:**
- `comment_sentiment_daily`: **brand_id = umbrella** (JOIN `brand_social_accounts`). **PK:** `(brand_id, platform, metric_date)`. UPSERT.
- `comment_sentiment_post`: **brand_id = per-akun** (langsung dari `unified_comment.brand_id`, TIDAK di-roll-up — sama pola kayak `post_metric`). **PK:** `(platform, post_id)`. UPSERT. `post_date` di sini resolved via `LEFT JOIN unified_post` (bisa NULL kalau post induk nggak ketemu).

Isi: total_comments, positive_count, neutral_count, negative_count, avg_sentiment_score. `comment_sentiment_post` tambahan `dominant_sentiment` (majority vote, tie-break ke 'neutral' kalau neutral>0, else NULL).

**`comment_sentiment_daily` (9 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| total_comments | bigint | NO |
| positive_count | bigint | NO |
| neutral_count | bigint | NO |
| negative_count | bigint | NO |
| avg_sentiment_score | numeric | YES |
| built_at | timestamp without time zone | NO |

**`comment_sentiment_post` (11 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| platform | character varying | NO |
| post_id | text | NO |
| brand_id | uuid | NO |
| post_date | date | YES |
| total_comments | bigint | NO |
| positive_count | bigint | NO |
| neutral_count | bigint | NO |
| negative_count | bigint | NO |
| avg_sentiment_score | numeric | YES |
| dominant_sentiment | character varying | YES |
| built_at | timestamp without time zone | NO |

### 4.7 `community_contributors`

**Builder:** `sp_build_community_contributors()` · **PK:** `(brand_id, platform, window_days, normalized_username)` · **brand_id = umbrella** · **Duplicate handling: UPSERT** (`ON CONFLICT ... DO UPDATE`, ⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT). **Trade-off:** ini leaderboard yang harusnya re-hitung total tiap run — user yang jatuh dari window (comment count/relevance turun, atau keluar window 7/30/90 hari) TIDAK lagi otomatis hilang dari tabel; `rank_in_window`/`composite_score`/`tier` lama bisa nyangkut basi sampai user itu muncul lagi di run berikutnya (yang akan menimpanya) atau dibersihkan manual.

comments_count, likes_received, replies_sum, avg_relevance, composite_score, tier (super_fan ≥70 / active ≥40 / casual), rank_in_window. composite = 50% volume-normalized + 50% avg_relevance. Melayani Community (leaderboard) & Audience (top contributors).

**Kolom & tipe (12 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| window_days | smallint | NO |
| normalized_username | text | NO |
| comments_count | bigint | NO |
| likes_received | bigint | NO |
| replies_sum | bigint | NO |
| avg_relevance | numeric | YES |
| composite_score | numeric | NO |
| tier | character varying | NO |
| rank_in_window | integer | NO |
| built_at | timestamp without time zone | NO |

### 4.8 `comment_relevance_distribution`

**Builder:** `sp_build_comment_relevance_distribution()` · **PK:** `(brand_id, platform, tier)` · **brand_id = umbrella** · **Duplicate handling: UPSERT** (`ON CONFLICT (brand_id, platform, tier) DO UPDATE`, ⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT). **Trade-off:** kalau sebuah (brand, platform, tier) sudah tidak punya komentar lagi di run terbaru, baris lama dengan `comment_count` basi tetap nyangkut (tidak ter-zero-kan) sampai combo itu muncul lagi.

comment_count. Tier: High >75, Mid 40–75, Low <40 (skala relevance 0–100). Simpan count; FE hitung % = count/SUM(count). Audience → Comment Relevance distribution. (Sample komentar per tier: FE JOIN `unified_comment ↔ comment_relevance_scores`.)

**Kolom & tipe (5 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| tier | text | NO |
| comment_count | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.9 `post_comment_timeline`

**Builder:** `sp_build_post_comment_timeline()` · **PK:** `(platform, post_id, bucket_date)` (nggak ada `brand_id` di grain — post_id + platform udah unik) · **Duplicate handling: UPSERT** (`ON CONFLICT (platform, post_id, bucket_date) DO UPDATE`, ⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT). **Trade-off:** bucket tanggal yang commentnya sudah hilang (dihapus dari sumber) tetap nyangkut dengan `comment_count` basi.

`bucket_date` (WIB, sumbu absolut), `days_since_post` (sumbu relatif = bucket_date − post_date), comment_count. Campaign Analysis → Comment timeline. Bucket pakai `comment_date` (WIB).

**Kolom & tipe (6 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| platform | character varying | NO |
| post_id | text | NO |
| bucket_date | date | NO |
| days_since_post | integer | YES |
| comment_count | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.10 `post_wordcloud`

**Builder:** Python (`compute_wordcloud_per_post`) · **PK:** `(post_id, platform, word)` · **Duplicate handling: TRUNCATE + INSERT.**

frequency (top-50 per post). Campaign Analysis → Word cloud.

**Kolom & tipe (4 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| post_id | text | NO |
| platform | text | NO |
| word | text | NO |
| frequency | integer | NO |

### 4.11 `audience_demographics_daily`

**Builder:** `sp_build_audience_demographics_daily()` · **PK:** `(platform, brand_id, audience_date, audience_type)` · **brand_id = per-akun** (langsung dari `unified_audience`, TIDAK di-roll-up ke umbrella) · **Duplicate handling:** UPSERT.

age_13_17…age_65_plus, gender_female/male/unknown (count). Audience → Age Distribution, Gender Split. IG-only. FE hitung % dalam breakdown masing-masing.

**Kolom & tipe (15 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| audience_date | date | NO |
| audience_type | character varying | NO |
| age_13_17 | numeric | YES |
| age_18_24 | numeric | YES |
| age_25_34 | numeric | YES |
| age_35_44 | numeric | YES |
| age_45_54 | numeric | YES |
| age_55_64 | numeric | YES |
| age_65_plus | numeric | YES |
| gender_female | numeric | YES |
| gender_male | numeric | YES |
| gender_unknown | numeric | YES |
| built_at | timestamp without time zone | NO |

### 4.12 `audience_geo_daily`

**Builder:** `sp_build_audience_geo_daily()` · **PK:** `(platform, brand_id, audience_date, audience_type, geo_level, geo_key)` · **brand_id = per-akun** · **Duplicate handling:** UPSERT.

geo_level ('city'/'country'), geo_key, audience_count. Hasil unnest jsonb (pakai `LATERAL` buat unnest `city_breakdown`/`country_breakdown`). Audience → Top Audience Cities (+ Countries). IG-only.

**Kolom & tipe (8 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| audience_date | date | NO |
| audience_type | character varying | NO |
| geo_level | text | NO |
| geo_key | text | NO |
| audience_count | numeric | YES |
| built_at | timestamp without time zone | NO |

### 4.13 `posting_time_heatmap`

**Builder:** `sp_build_posting_time_heatmap()` · **PK:** `(platform, brand_id, weekday, hour)` · **brand_id = per-akun** (TIDAK di-roll-up) · **Duplicate handling: UPSERT** (`ON CONFLICT (platform, brand_id, weekday, hour) DO UPDATE`, ⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT — dulu ini snapshot all-time yang sengaja di-TRUNCATE tiap build, lihat glosarium). **Trade-off:** kombinasi weekday×hour yang sudah tidak ada post lagi (mis. post-nya dihapus) tetap nyangkut dengan angka basi.

post_count, engagement_sum, reach_sum, views_sum, er_denominator_sum. weekday 0=Minggu..6=Sabtu (WIB). Overview → Best Posting Times. FE warnai pakai avg engagement atau ER dari komponen.

**Kolom & tipe (10 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| weekday | smallint | NO |
| hour | smallint | NO |
| post_count | bigint | NO |
| engagement_sum | bigint | NO |
| reach_sum | bigint | NO |
| views_sum | bigint | NO |
| er_denominator_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.14 `dim_content_pillar` ⚠️ grain berubah — koreksi dari dokumentasi sebelumnya

**Builder:** `sp_build_dim_content_pillar()` · **PK:** `(id)` — surrogate `bigserial`, **BUKAN** `(brand_id, content_pillar)`. Unique constraint lama di `(brand_id, content_pillar)` **sudah dihapus** (confirmed nggak ada di `pg_constraint`) — sekarang duplikat nama pillar per brand **dibolehkan**, dibedain by `id`. · **brand_id = umbrella** · **Duplicate handling: bukan `ON CONFLICT`, tapi `NOT EXISTS` guard:**

```sql
INSERT INTO l2_gold.dim_content_pillar (brand_id, content_pillar)
SELECT DISTINCT pp.brand_id, pp.content_pillar
FROM l2_gold.pillar_performance_daily pp
WHERE pp.content_pillar IS NOT NULL AND pp.content_pillar <> ''
  AND NOT EXISTS (
      SELECT 1 FROM l2_gold.dim_content_pillar dcp
      WHERE dcp.brand_id = pp.brand_id AND dcp.content_pillar = pp.content_pillar
  );
```

Efeknya sama kayak `ON CONFLICT DO NOTHING` (seed nama baru doang, nggak nimpa edit user), tapi lewat `NOT EXISTS` karena nggak ada unique index buat jadi conflict target lagi. Kolom `hashtags` (array) dipakai buat matching di `post_metric` (lihat 4.2). color, display_order, description, is_active, created_at, updated_at — read-write untuk app (FE CRUD lewat UI). Content Pillars → Define Pillars.

**Kolom & tipe (10 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| content_pillar | text | NO |
| color | text | YES |
| display_order | integer | YES |
| description | text | YES |
| is_active | boolean | NO |
| created_at | timestamp without time zone | NO |
| updated_at | timestamp without time zone | NO |
| id | bigint | NO |
| hashtags | ARRAY | NO |

### 4.15 Story & TikTok

**Builder:** `sp_build_story_funnel()` (isi 2 tabel sekaligus) / `sp_build_tiktok_churn()` · Semua UPSERT.

| Tabel | PK | brand_id | Fungsi |
|---|---|---|---|
| `story_metric_daily` | `(brand_id, platform, metric_date)` | umbrella | Story KPI, Retention Funnel, Over Time. IG-only. |
| `story_type_daily` | `(brand_id, platform, metric_date, story_type)` ⚠️ *(sebelumnya cuma "brand × story_type" — ada `metric_date` juga, ini tabel harian, bukan snapshot sekali)* | umbrella | Story Type Performance. |
| `tiktok_churn_daily` | `(brand_id, metric_date)` | umbrella | new/lost/net followers. TikTok-only (nggak ada kolom `platform` — implisit TikTok). |

**`story_metric_daily` (13 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| story_count | bigint | NO |
| reach_sum | bigint | NO |
| views_sum | bigint | NO |
| replies_sum | bigint | NO |
| taps_fwd_sum | bigint | NO |
| taps_back_sum | bigint | NO |
| exits_sum | bigint | NO |
| swipe_up_sum | bigint | NO |
| follows_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

**`story_type_daily` (8 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| story_type | character varying | NO |
| story_count | bigint | NO |
| reach_sum | bigint | NO |
| replies_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

**`tiktok_churn_daily` (8 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| new_followers | bigint | NO |
| lost_followers | bigint | NO |
| net_growth | bigint | NO |
| video_views_sum | bigint | NO |
| built_at | timestamp without time zone | NO |

### 4.16 Lain

| Tabel | Builder | PK | brand_id | Duplicate handling | Fungsi |
|---|---|---|---|---|---|
| `ugc_tagged_posts` | `sp_build_ugc_tagged_posts()` | `(post_id, platform, brand_id)` | umbrella | UPSERT (⚠️ diubah 2026-09-04 dari TRUNCATE+INSERT — post yang tag-nya dicabut/dihapus tetap nyangkut) | Tagged Posts (UGC). IG-only. |
| `v_campaign_posts` | VIEW (bukan tabel materialized) | on-demand | — | n/a (dihitung tiap query) | Campaign Analysis. |

**`ugc_tagged_posts` (11 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | NO |
| post_id | text | NO |
| platform | text | NO |
| username | text | YES |
| post_type | text | YES |
| caption | text | YES |
| like_count | integer | NO |
| comment_count | integer | NO |
| total_engagement | integer | NO |
| link_post | text | YES |
| post_date | timestamp with time zone | YES |

**`v_campaign_posts` (23 kolom, VIEW):**

| Kolom | Tipe | Null |
|---|---|---|
| brand_id | uuid | YES |
| account_id | uuid | YES |
| platform | character varying | YES |
| post_id | text | YES |
| post_date | date | YES |
| post_datetime | timestamp without time zone | YES |
| title | text | YES |
| caption | text | YES |
| link | text | YES |
| post_type | text | YES |
| format | text | YES |
| content_pillar | text | YES |
| is_campaign | boolean | YES |
| is_boosted | boolean | YES |
| reach | bigint | YES |
| views | bigint | YES |
| likes | bigint | YES |
| comments | bigint | YES |
| shares | bigint | YES |
| saves | bigint | YES |
| engagement | bigint | YES |
| engagement_rate | numeric | YES |
| engagement_rate_base | text | YES |

### 4.17 `audience_sentiment_monthly`, `comment_wordcloud_sentiment`, `ytd_performance` ⚠️ baru 2026-09-08

Belum ter-query ulang ke DB (⚠️ kolom & tipe di bawah cuma dari deskripsi asset
Dagster dan SQL `INSERT` di kode, per 2026-09-10 — beda dari tabel lain di
dokumen ini yang confirmed via `information_schema`/`pg_constraint`):

| Tabel | Builder | Grain | Duplicate handling | Fungsi |
|---|---|---|---|---|
| `audience_sentiment_monthly` | `sp_build_audience_sentiment_monthly()` | brand umbrella x platform x `source_type` (comment/post_caption/tagged_post_caption) x bulan | UPSERT (asumsi, konsisten pola SP lain — belum confirmed langsung) | Section "Audience Sentiment" di report. Persentase & MoM comparison dihitung di VIEW `l2_gold.v_audience_sentiment_mom` (LAG per partition), BUKAN di tabel ini — pola sama `v_campaign_posts`. |
| `comment_wordcloud_sentiment` | Python (`compute_wordcloud_per_sentiment_month`) | brand umbrella x platform x sentiment_label x bulan | TRUNCATE + INSERT | Word cloud di section Audience Sentiment. Kolom confirmed dari `INSERT`: `brand_id`, `platform`, `sentiment_label`, `period_month`, `word`, `frequency`. |
| `ytd_performance` | `sp_calculate_ytd_performance(ytd_id, platform_id, metrics_target)` | ytd_id x platform_id x metrics_target x metric_date | Full recompute (delete+reinsert) per kombinasi di dalam SP — idempotent, self-heals kalau `brand_metric_daily` di-backfill | Beda pola dari mart lain: butuh parameter per kombinasi (bukan `sp_build_*()` tanpa argumen). Asset Dagster cuma REFRESH kombinasi yang **sudah ada** & `ytd_setting.is_active` — kombinasi baru didaftarkan app saat user pertama kali pilih metric, bukan oleh asset ini. |

---

## 5. Peta Dashboard → Gold (per visual)

### Overview

| Visual | Sumber Gold | Catatan |
|---|---|---|
| Brand Header (Total followers) | `brand_metric_daily.follower_count_eod` | snapshot terbaru |
| 5 KPI Cards | `brand_metric_daily` | sum komponen per window; ER dari komponen. ⚠️ Kalau salah satu card = "Avg. ER Reach/Views/Followers" (`metriks_launch.xlsx`), sumbernya `post_metric` (AVG per-post) — **bukan** `brand_metric_daily`. Cek definisi pasti tiap card ke FE dev. |
| Engagement Over Time | `brand_metric_daily` | GROUP BY week |
| Platform Share (donut) | `brand_metric_daily.reach_sum` | GROUP BY platform |
| Brand Performance Matrix | `brand_metric_daily` | 1 baris / brand×platform |
| Content Attribute Breakdown | `content_attribute_daily` | grain brand × content_tag |
| Best Posting Times (heatmap) | `posting_time_heatmap` | weekday × hour, WIB |

### Content Overview

| Visual | Sumber Gold | Catatan |
|---|---|---|
| 4 KPI (Posts/Saves Rate/Completion/Link Clicks) | `brand_metric_daily` + `post_metric` | platform-specific |
| Post Type Performance | `post_metric` | GROUP BY format |
| Content Volume by Week | `brand_metric_daily.post_count` | GROUP BY week |
| Top Posts | `post_metric` | ranking by ER |
| TikTok Completion Distribution | `post_metric.completion_rate` | FE bucket |
| Reel Watch Time by Duration | `post_metric` (avg_watch_time + duration_s) | FE filter duration_s>0 |

### Audience Deep Dive

| Visual | Sumber Gold | Catatan |
|---|---|---|
| 4 KPI (Followers/IG Accounts Engaged/TK Profile Views/FB Visits) | `brand_metric_daily` (incl. `accounts_engaged_sum`) | IG Engaged filter platform=IG |
| Age Distribution | `audience_demographics_daily` | share dari komponen count |
| Gender Split by Platform | `audience_demographics_daily` | IG-only |
| Comment Relevance (distribusi) | `comment_relevance_distribution` | count per tier; FE hitung % |
| Comment Relevance (sample) | FE JOIN `unified_comment ↔ comment_relevance_scores` | per tier |
| Top Community Contributors | `community_contributors` | composite_score + tier |
| Top Audience Cities | `audience_geo_daily` | geo_level='city' |
| Follower Growth Trend | `brand_metric_daily.net_growth_sum` | GROUP BY week |
| UGC Tagged Posts | `ugc_tagged_posts` | — |

### Stories

| Visual | Sumber Gold |
|---|---|
| 4 KPI | `story_metric_daily` |
| Story Retention Funnel | `story_metric_daily` |
| Story Type Performance | `story_type_daily` |
| Story Performance Over Time | `story_metric_daily` |

### TikTok Deep

| Visual | Sumber Gold | Catatan |
|---|---|---|
| 5 KPI | `tiktok_churn_daily` + `brand_metric_daily` | — |
| Follower Churn (diverging bar) | `tiktok_churn_daily` | new/lost/net |
| Duration vs Completion (scatter) | `post_metric` (duration_s + completion_rate) | FE filter duration_s>0 |
| Avg Watch Time by Pillar | `pillar_performance_daily.watch_time_sum` | grain brand × pillar |

### Community

| Visual | Sumber Gold |
|---|---|
| 4 KPI | `comment_activity_daily` |
| Comment Volume by Platform | `comment_activity_daily` |
| Comment Activity by Hour | `comment_activity_hourly` |
| Top Commenters Leaderboard | `community_contributors` |

### Campaign Analysis

| Visual | Sumber Gold | Catatan |
|---|---|---|
| Post Selection Grid | `post_metric` (+ content_pillar) | filter by pillar/platform |
| Per-post contribution | `post_metric` | on-the-fly dari post terpilih |
| Comment timeline distribution | `post_comment_timeline` | sumbu absolut/relatif |
| Cleaned word cloud | `post_wordcloud` | post × word × frequency |

### Content Pillars

| Visual | Sumber Gold | Catatan |
|---|---|---|
| Define Pillars (form) | `dim_content_pillar` | user CRUD nama + warna |
| Comparison Output | `pillar_performance_daily` + `dim_content_pillar` | JOIN via (brand_id, content_pillar) |

---

## 6. Data Kompetitor (Brand vs Competitor)

Jalur data terpisah dari brand utama, khusus untuk report **Brand vs Competitor**. Scope sama (FB/IG/TikTok), tapi grain, semantik `brand_id`, dan keterbatasan data beda. Confirmed via `information_schema` (10 Jul 2026).

### 6.1 Alur & Perbedaan Kunci

```
l0_raw (fb/ig/tiktok_competitor_media + _snapshots, dari Apify)
  → l0_harmonization (SHARED — tabel sama dengan brand utama: facebook_post, instagram_post, dst)
  → l1_silver (unified_competitor_post, unified_competitor_profile_daily — dedicated)
  → l2_gold (competitor_post_metric, competitor_profile_metric_daily — dedicated)
  → dashboard (Brand vs Competitor)
```

- **Harmonization = shared, bukan dedicated.** Tidak ada `l0_harmonization.*_competitor_*`. Baris kompetitor masuk lewat `UNION ALL` (CTE `main_latest` + `competitor_latest`) di dalam SP harmonization yang sama dipakai brand utama (`sp_sync_facebook_post_from_raw()`, dst), difilter oleh `public.brand_competitors`. Konfirmasi: query `information_schema.tables ... WHERE table_name ILIKE '%competitor%'` cuma nunjukin tabel di `l0_raw`, `l1_silver`, `l2_gold`, `public` — nggak ada di `l0_harmonization`.
- **Silver & Gold = dedicated**, terpisah dari `unified_post`/`post_metric` milik brand utama. Satu post kompetitor bisa muncul di `post_metric` (apa adanya, label akun asli, tanpa filtering) **dan** `competitor_post_metric` (relabeled, di-scope ke brand client yang melacak) — dua-duanya valid, bukan duplikat bug. Menggabungkan jadi satu tabel tidak feasible: satu post kompetitor bisa dilacak >1 client (many-to-many), akan merusak agregasi `post_metric`.
- **Silver pakai `social_account_id`, bukan `brand_id`** — beda dari konvensi brand utama, karena satu akun kompetitor bisa dilacak oleh >1 brand client. Resolusi ke `brand_id` client baru terjadi di Gold, lewat JOIN `public.brand_competitors`.
- **Reach & impressions terstruktur tidak tersedia untuk kompetitor** di semua platform — keterbatasan scraping (Apify tidak dapat private insight akun orang lain).
- **`competitor_scheduler_config`** — singleton global, belum ada per-account targeting.

### 6.2 Layer Silver Kompetitor

Kolom & tipe lengkap `unified_competitor_post` dan `unified_competitor_profile_daily` ada di [§2](#2-layer-silver-l1_silver) (biar nggak dobel). Ringkasan:

| Tabel | Grain | Builder | Duplicate handling |
|---|---|---|---|
| `unified_competitor_post` | (social_account_id, post_id) | `sp_sync_unified_competitor_post()` | **UPSERT** (⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT) |
| `unified_competitor_profile_daily` | (social_account_id, metric_date) | `sp_sync_unified_competitor_profile_daily()` | **UPSERT** (⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT) |

⚠️ **Ketersediaan metrik per platform di `unified_competitor_post` beda-beda** (dari komentar di SP, confirmed 10 Jul 2026) — jangan asumsikan semua platform lengkap:

| Metrik | Facebook | Instagram | TikTok |
|---|---|---|---|
| like_count | ✅ (pakai `reactions`, bukan `likes` — konsisten sama konvensi brand utama) | ✅ | ✅ |
| comment_count | ❌ NULL (scraping FB kompetitor nggak dapet comment count sama sekali) | ✅ | ✅ |
| share_count | ✅ | ❌ NULL (nggak tersedia dari scraping publik) | ✅ |
| view_count | ❌ NULL | ✅ | ✅ |
| save_count | ❌ NULL (FB nggak punya konsep save) | ❌ NULL (nggak tersedia dari scraping publik) | ✅ |

→ **TikTok = satu-satunya platform kompetitor dengan data lengkap.** FB & IG kompetitor punya lubang metrik yang berbeda-beda — kalau bikin visual Brand vs Competitor yang gabung platform, ini bisa bikin angka misleading kalau nggak di-exclude/di-flag di FE.

### 6.3 Layer Gold Kompetitor

| Tabel | Grain (UNIQUE) | Builder | Duplicate handling |
|---|---|---|---|
| `competitor_post_metric` | (brand_id, competitor_social_account_id, post_id) | `sp_build_competitor_post_metric()` | **UPSERT** (⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT) |
| `competitor_profile_metric_daily` | (brand_id, competitor_social_account_id, metric_date) | `sp_build_competitor_profile_metric_daily()` | **UPSERT** (⚠️ diubah 2026-09-04, dulu TRUNCATE+INSERT) |

Keduanya resolve `brand_id` (= brand client yang melacak) lewat JOIN `public.brand_competitors` — bukan lewat `brand_social_accounts` kayak Gold brand utama.

⚠️ **Dependency order**: `competitor_post_metric` harus di-build **sebelum** `competitor_profile_metric_daily` (`sp_build_competitor_profile_metric_daily()` baca dari `competitor_post_metric` — bukan sekadar konvensi, memang dependency).

**`competitor_post_metric` (11 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| brand_id | uuid | NO |
| competitor_social_account_id | uuid | NO |
| platform | character varying | NO |
| post_id | character varying | NO |
| post_date_wib | date | YES |
| like_count | integer | YES |
| comment_count | integer | YES |
| share_count | integer | YES |
| view_count | integer | YES |
| save_count | integer | YES |

**`competitor_profile_metric_daily` (14 kolom):**

| Kolom | Tipe | Null |
|---|---|---|
| id | bigint | NO |
| brand_id | uuid | NO |
| competitor_social_account_id | uuid | NO |
| platform | character varying | NO |
| metric_date | date | NO |
| follower_count | integer | YES |
| following_count | integer | YES |
| followers_growth | integer | YES |
| post_count | integer | YES |
| like_count | integer | YES |
| comment_count | integer | YES |
| share_count | integer | YES |
| view_count | integer | YES |
| save_count | integer | YES |

### 6.4 Gap Diketahui

- Profile snapshot coverage 0% untuk `morigro.id` (FB) dan `telkomsel` (TikTok) — pre-existing, penyebab belum teridentifikasi, tidak terkait ke brand/akun kompetitor lain.
- Metrik reach-based (ER Reach, dll) tidak bisa dihasilkan untuk kompetitor — exclude dari UI kalau belum ada fallback.

### 6.5 Peta Dashboard → Gold (Brand vs Competitor)

| Visual | Sumber Gold | Catatan |
|---|---|---|
| Perbandingan metrik channel | `competitor_profile_metric_daily` | vs `brand_metric_daily` untuk brand sendiri |
| Perbandingan post-level | `competitor_post_metric` | vs `post_metric` untuk brand sendiri |
