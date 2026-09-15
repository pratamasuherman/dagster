# Dokumentasi Layer `l0_raw` & `l0_harmonization` — Autometric

> Dibuat: 10 Juli 2026. Disusun berdasarkan hasil query langsung ke `tsdb` (Tiger Cloud Singapore). Sesuai prinsip "cek realita DB dulu" — semua isi dokumen ini berasal dari struktur database aktual, bukan asumsi dari kode aplikasi.
>
> ⚠️ **Catatan 2026-09-10:** sejak 2026-08-19, `autometric/assets/harmonization_assets.py`
> menambahkan 3 procedure/asset baru yang mengubah beberapa temuan di bawah ini
> (lihat §5 dan §8) — TikTok comment & TikTok audience sekarang PUNYA jalur sync
> dari raw (sebelumnya dianggap anomali/tidak ada), dan ada 2 procedure housekeeping
> baru (gap-fill tanggal profile, koreksi pembulatan audience). Detail kolom
> tabel-tabel baru (`l0_raw.tt_comments`, `l0_harmonization.tiktok_audience`,
> `l0_extra.tt_profile_native`) **belum di-query ulang ke DB** untuk dokumen ini —
> yang di bawah cuma dari deskripsi asset Dagster, bukan `information_schema`
> langsung seperti sisa dokumen ini. Query ulang bagian 2 kalau butuh detail kolom
> pasti.

---

## 1. Gambaran Umum

Autometric pakai medallion architecture: `l0_raw → l0_harmonization → l1_silver → feature (NLP) → l2_gold`. Dokumen ini fokus ke dua layer paling awal.

| Layer | Peran | Analogi |
|---|---|---|
| **`l0_raw`** | Landing zone. Setiap kali data di-fetch dari Meta Graph API / TikTok API / Apify, hasilnya **di-INSERT langsung apa adanya** ke sini — tanpa dedup, tanpa transformasi. | Kayak nge-scan struk belanja dan nyimpen fotonya — satu foto per transaksi scan, meskipun barangnya sama, difoto berkali-kali tetap disimpan semua. |
| **`l0_harmonization`** | Normalisasi lintas platform. Data dari 3 platform (FB/IG/TikTok) × 2 sumber (brand sendiri vs kompetitor) × N raw table disatukan jadi **satu skema kolom yang seragam per jenis entitas** (profile, post, comment, dst), dengan **satu baris final per entitas** (bukan per fetch). | Kayak rekap struk itu jadi satu baris ringkasan per transaksi di buku kas — meskipun difoto 5x, yang dicatat cuma versi terakhir/terbaik. |

**Perbedaan kunci grain (butir data):**
- `l0_raw` = time-series snapshot. Bisa ada banyak baris per hari untuk akun & entitas yang sama (setiap kali pipeline fetch data).
- `l0_harmonization` = satu baris per (`brand_id`, natural key) — misal satu baris per post per brand, atau satu baris per brand per hari untuk data profile.
- `brand_id` di `l0_harmonization` **bukan** `brands.id` — melainkan `social_accounts.id` (per-akun/per-platform), sama seperti pattern `brand_id` di Silver yang udah didokumentasikan sebelumnya. Semua FK `brand_id` di layer ini reference ke `public.social_accounts(id)`.

---

## 2. Query Referensi

Query-query ini yang dipakai buat generate dokumentasi ini — simpan buat re-run kalau skema berubah.

### 2.1 Kolom & tipe data semua tabel
```sql
SELECT 
    c.table_schema, c.table_name, c.ordinal_position, c.column_name,
    c.data_type, c.udt_name, c.character_maximum_length,
    c.numeric_precision, c.numeric_scale, c.is_nullable, c.column_default
FROM information_schema.columns c
WHERE c.table_schema IN ('l0_raw', 'l0_harmonization')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
```

### 2.2 Jumlah baris per tabel
```sql
SELECT schemaname, relname AS table_name, n_live_tup AS estimated_rows
FROM pg_stat_user_tables
WHERE schemaname IN ('l0_raw', 'l0_harmonization')
ORDER BY schemaname, relname;
```

### 2.3 Daftar procedure/function
```sql
SELECT n.nspname AS schema_name, p.proname AS procedure_name, p.prokind,
    pg_get_function_identity_arguments(p.oid) AS arguments,
    pg_get_function_result(p.oid) AS return_type
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname IN ('l0_raw', 'l0_harmonization')
  AND p.prokind IN ('f','p')
ORDER BY n.nspname, p.proname;
```

### 2.4 Full definisi procedure
```sql
SELECT n.nspname AS schema_name, p.proname AS procedure_name,
    pg_get_functiondef(p.oid) AS definition
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname IN ('l0_raw', 'l0_harmonization')
  AND p.prokind IN ('f','p')
ORDER BY n.nspname, p.proname;
```

### 2.5 Constraint & FK (pakai `pg_constraint`, bukan `information_schema`)
```sql
SELECT con.conname AS constraint_name, con.contype,
    nsp.nspname AS schema_name, rel.relname AS table_name,
    pg_get_constraintdef(con.oid) AS definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
WHERE nsp.nspname IN ('l0_raw', 'l0_harmonization')
ORDER BY nsp.nspname, rel.relname;
```

---

## 3. Struktur `l0_raw`

16 tabel. Semua row punya `id UUID DEFAULT gen_random_uuid()` sebagai PK, dan `fetched_at TIMESTAMPTZ DEFAULT now()` yang jadi penanda kapan snapshot itu diambil.

**Pattern constraint yang penting:**
- Tabel *snapshot milik brand sendiri* (`fb_profile_snapshots`, `fb_post_snapshots`, `fb_comments`, `ig_profile_snapshots`, `ig_media_snapshots`, `ig_comments`, `ig_stories`, `ig_tagged_posts`, `tt_profile_snapshots`, `tt_video_snapshots`) — **TIDAK PUNYA UNIQUE constraint** di natural key (`social_account_id` + `post_id`/`media_id`/`video_id`). Artinya di level database, boleh ada snapshot ganda untuk entitas yang sama kapan aja — ini disengaja, karena raw memang didesain sebagai log append-only per fetch.
- Tabel *kompetitor* (`fb_competitor_media`, `ig_competitor_media`, `tiktok_competitor_media`) **PUNYA** `UNIQUE (social_account_id, post_id/media_id)` — jadi di raw layer sendiri, data kompetitor sifatnya upsert per entitas (bukan snapshot historis penuh).
- FK ke `social_accounts(id)`: tabel kompetitor pakai `ON DELETE CASCADE` (kalau akun kompetitor dihapus, semua raw datanya ikut kehapus). Tabel milik brand sendiri **tidak** ada `ON DELETE` eksplisit (default RESTRICT) — kalau ada raw data yang nempel, akun brand nggak bisa dihapus gitu aja.


### 3.1 Daftar tabel `l0_raw`

| Tabel | Baris | Sumber | Unique constraint | Grain |
|---|---|---|---|---|
| `fb_profile_snapshots` | 116 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `fb_post_snapshots` | 48 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `fb_comments` | 93 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `fb_competitor_snapshots` | 88 | Apify (kompetitor) | — | 1 snapshot per fetch |
| `fb_competitor_media` | 15 | Apify (kompetitor) | `(social_account_id, post_id)` | 1 baris per post (upsert) |
| `ig_profile_snapshots` | 116 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `ig_media_snapshots` | 61 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `ig_comments` | 1187 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `ig_stories` | 82 | Meta Graph API (brand sendiri) | — | 1 snapshot per fetch |
| `ig_tagged_posts` | 28 | Meta Graph API (UGC, brand sendiri) | — | 1 snapshot per fetch |
| `ig_competitor_snapshots` | 69 | Apify (kompetitor) | — | 1 snapshot per fetch |
| `ig_competitor_media` | 110 | Apify (kompetitor) | `(social_account_id, media_id)` | 1 baris per post (upsert) |
| `tt_profile_snapshots` | 96 | TikTok API (brand sendiri) | — | 1 snapshot per fetch |
| `tt_video_snapshots` | 62 | TikTok API (brand sendiri) | — | 1 snapshot per fetch |
| `tiktok_competitor_snapshots` | 88 | Apify (kompetitor) | — | 1 snapshot per fetch |
| `tiktok_competitor_media` | 68 | Apify (kompetitor) | `(social_account_id, post_id)` | 1 baris per post (upsert) |

⚠️ **Catatan**: Tidak ada tabel raw untuk **komentar TikTok** (`tiktok_comments` tidak eksis di `l0_raw`). Tapi `l0_harmonization.tiktok_comment` punya 110 baris data — lihat bagian 6 (Temuan & Anomali) buat detail.

⚠️ **Catatan**: `facebook_audience` dan `instagram_audience` di harmonization **tidak** punya tabel raw terpisah — datanya diambil dari kolom JSONB `demographics_age`, `demographics_city`, `demographics_country`, `demographics_gender` di dalam `fb_profile_snapshots` / `ig_profile_snapshots`, lalu di-pivot jadi baris-baris terpisah oleh procedure.

### 3.2 Detail kolom per tabel `l0_raw`

#### `l0_raw.fb_comments` (93 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| post_id | varchar(255) | NO |  |
| comment_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| link_post | text | YES |  |
| link_comment | text | YES |  |
| post_date | timestamp with time zone | YES |  |
| comment_time | timestamp with time zone | YES |  |
| comment_text | text | YES |  |
| comment_username | varchar(255) | YES |  |
| comment_user_id | varchar(255) | YES |  |
| likes_count | integer | NO | 0 |
| replies_count | integer | NO | 0 |
| reactions_count | integer | NO | 0 |
| has_attachment | boolean | NO | false |
| parent_id | varchar(255) | YES |  |

#### `l0_raw.fb_competitor_media` (15 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| post_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| post_date | timestamp with time zone | YES |  |
| caption | text | YES |  |
| url | text | YES |  |
| page_name | varchar(255) | YES |  |
| like_count | bigint | YES |  |
| share_count | bigint | YES |  |
| top_reactions_count | bigint | YES |  |
| media_count | integer | YES |  |
| media | text[] | YES |  |
| hashtags_list | text[] | YES |  |
| hashtags_count | integer | YES |  |

#### `l0_raw.fb_competitor_snapshots` (88 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| username | varchar(255) | YES |  |
| account_id | varchar(255) | YES |  |
| page_id | varchar(255) | YES |  |
| page_name | varchar(255) | YES |  |
| page_title | varchar(255) | YES |  |
| follower_count | bigint | YES |  |
| like_count | bigint | YES |  |
| rating_count | bigint | YES |  |
| email | varchar(255) | YES |  |
| creation_date | date | YES |  |
| categories | text[] | YES |  |
| info | text | YES |  |
| intro | text | YES |  |
| websites_link | text[] | YES |  |
| page_url | text | YES |  |
| profile_photo | text | YES |  |

#### `l0_raw.fb_post_snapshots` (48 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| post_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| posted_at | timestamp with time zone | YES |  |
| message | text | YES |  |
| story | text | YES |  |
| full_picture | text | YES |  |
| permalink_url | text | YES |  |
| post_type | varchar(50) | YES |  |
| reactions_count | integer | YES |  |
| likes_count | integer | YES |  |
| comments_count | integer | YES |  |
| shares_count | integer | NO | 0 |
| impressions | bigint | YES |  |
| reach | bigint | YES |  |
| clicks | bigint | YES |  |
| reactions_by_type | jsonb | YES |  |
| video_views | bigint | YES |  |

#### `l0_raw.fb_profile_snapshots` (116 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| page_id | varchar(255) | YES |  |
| name | varchar(255) | YES |  |
| about | text | YES |  |
| category | varchar(255) | YES |  |
| website | text | YES |  |
| fan_count | integer | YES |  |
| followers_count | integer | YES |  |
| cover_url | text | YES |  |
| avatar_url | text | YES |  |
| page_link | text | YES |  |
| page_follows | bigint | YES |  |
| page_daily_follows_unique | integer | YES |  |
| page_media_view | bigint | YES |  |
| page_total_media_view_unique | bigint | YES |  |
| page_video_views | bigint | YES |  |
| page_views_total | bigint | YES |  |
| content_interactions | bigint | YES |  |
| link_clicks | bigint | YES |  |
| profile_reach | bigint | YES |  |

#### `l0_raw.ig_comments` (1187 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| media_id | varchar(255) | NO |  |
| comment_id | varchar(255) | NO |  |
| link_post | text | YES |  |
| link_comment | text | YES |  |
| comment_time | timestamp with time zone | YES |  |
| comment_text | text | YES |  |
| comment_username | varchar(255) | YES |  |
| likes_count | integer | NO | 0 |
| replies_count | integer | NO | 0 |
| hidden | boolean | YES |  |
| parent_id | text | YES |  |
| created_at | timestamp with time zone | NO | now() |

#### `l0_raw.ig_competitor_media` (110 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| media_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| posted_at | timestamp with time zone | YES |  |
| caption | text | YES |  |
| media_type | varchar(50) | YES |  |
| permalink | text | YES |  |
| cover_image | text | YES |  |
| like_count | integer | YES |  |
| comment_count | integer | YES |  |
| view_count | integer | YES |  |
| shortcode | varchar(255) | YES |  |
| slide_count | integer | YES |  |
| video_duration | double precision | YES |  |
| hashtags_list | text[] | YES |  |
| hashtags_count | integer | YES |  |
| mentions | text[] | YES |  |
| is_collaborator | boolean | YES |  |
| is_sponsored | boolean | YES |  |
| is_comment_disabled | boolean | YES |  |
| music_title | varchar(255) | YES |  |
| music_author | varchar(255) | YES |  |
| is_pinned | boolean | YES |  |

#### `l0_raw.ig_competitor_snapshots` (69 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| username | varchar(255) | YES |  |
| full_name | varchar(255) | YES |  |
| biography | text | YES |  |
| is_verified | boolean | YES |  |
| follower_count | integer | YES |  |
| following_count | integer | YES |  |
| media_count | integer | YES |  |
| is_private | boolean | YES |  |
| is_business | boolean | YES |  |
| account_category | varchar(255) | YES |  |
| external_url | text | YES |  |
| bio_links | text[] | YES |  |

#### `l0_raw.ig_media_snapshots` (61 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| media_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| posted_at | timestamp with time zone | YES |  |
| caption | text | YES |  |
| media_type | varchar(50) | YES |  |
| permalink | text | YES |  |
| cover_image | text | YES |  |
| reach | integer | YES |  |
| saved | integer | YES |  |
| comments | integer | YES |  |
| shares | integer | YES |  |
| total_interactions | integer | YES |  |
| likes | integer | YES |  |
| views | integer | YES |  |
| reposts | integer | YES |  |
| follows | integer | YES |  |
| profile_visits | integer | YES |  |
| reel_avg_watch_time | numeric(10,4) | YES |  |
| reel_video_view_total_time | bigint | YES |  |
| video_duration | numeric(10,3) | YES |  |
| carousel_media_count | integer | YES |  |

#### `l0_raw.ig_profile_snapshots` (116 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| username | varchar(255) | YES |  |
| name | varchar(255) | YES |  |
| biography | text | YES |  |
| website | text | YES |  |
| followers_count | integer | YES |  |
| follows_count | integer | YES |  |
| media_count | integer | YES |  |
| accounts_engaged | integer | YES |  |
| comments | integer | YES |  |
| likes | integer | YES |  |
| profile_links_taps | integer | YES |  |
| reach | integer | YES |  |
| replies | integer | YES |  |
| reposts | integer | YES |  |
| saves | integer | YES |  |
| shares | integer | YES |  |
| total_interactions | integer | YES |  |
| views | integer | YES |  |
| follows_and_unfollows | jsonb | YES |  |
| demographics_age | jsonb | YES |  |
| demographics_city | jsonb | YES |  |
| demographics_country | jsonb | YES |  |
| demographics_gender | jsonb | YES |  |

#### `l0_raw.ig_stories` (82 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| media_id | varchar(255) | NO |  |
| posted_at | timestamp with time zone | YES |  |
| media_type | varchar(50) | YES |  |
| permalink | text | YES |  |
| media_url | text | YES |  |
| thumbnail_url | text | YES |  |
| username | text | YES |  |
| reach | integer | YES |  |
| replies | integer | YES |  |
| shares | integer | YES |  |
| follows | integer | YES |  |
| profile_visits | integer | YES |  |
| profile_activity | integer | YES |  |
| reposts | integer | YES |  |
| total_interactions | integer | YES |  |
| total_views | integer | YES |  |
| facebook_views | integer | YES |  |
| navigation | jsonb | YES |  |
| video_duration | numeric(10,3) | YES |  |
| fetched_at | timestamp with time zone | NO | now() |

#### `l0_raw.ig_tagged_posts` (28 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| media_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| posted_at | timestamp with time zone | YES |  |
| caption | text | YES |  |
| media_type | varchar(50) | YES |  |
| permalink | text | YES |  |
| tagged_by | varchar(255) | YES |  |
| like_count | integer | YES |  |
| comment_count | integer | YES |  |
| cover_image | text | YES |  |

#### `l0_raw.tiktok_competitor_media` (68 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| post_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| post_date | timestamp with time zone | YES |  |
| caption | text | YES |  |
| caption_language | varchar(100) | YES |  |
| media_type | varchar(100) | YES |  |
| slide_count | integer | YES |  |
| like_count | bigint | YES |  |
| comment_count | bigint | YES |  |
| play_count | bigint | YES |  |
| saved_count | bigint | YES |  |
| share_count | bigint | YES |  |
| video_duration | bigint | YES |  |
| hashtags_list | text[] | YES |  |
| hashtags_count | integer | YES |  |
| mentions | text[] | YES |  |
| url | text | YES |  |
| cover_image | text | YES |  |
| is_pinned | boolean | YES |  |
| is_sponsored | boolean | YES |  |
| is_ad | boolean | YES |  |
| music_title | varchar(255) | YES |  |
| music_author | varchar(255) | YES |  |

#### `l0_raw.tiktok_competitor_snapshots` (88 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| username | varchar(255) | YES |  |
| account_id | varchar(255) | YES |  |
| account_nickname | varchar(255) | YES |  |
| following_count | bigint | YES |  |
| follower_count | bigint | YES |  |
| video_count | bigint | YES |  |
| like_count | bigint | YES |  |
| is_verified | boolean | YES |  |
| bio_signature | varchar(255) | YES |  |
| bio_link | text | YES |  |
| is_private | boolean | YES |  |
| is_seller | boolean | YES |  |
| is_commerce_user | boolean | YES |  |
| commerce_user_category | varchar(255) | YES |  |
| avatar | text | YES |  |

#### `l0_raw.tt_profile_snapshots` (96 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| open_id | varchar(255) | YES |  |
| display_name | varchar(255) | YES |  |
| bio_description | text | YES |  |
| avatar_url | text | YES |  |
| is_verified | boolean | YES |  |
| follower_count | integer | YES |  |
| following_count | integer | YES |  |
| likes_count | bigint | YES |  |
| video_count | integer | YES |  |

#### `l0_raw.tt_video_snapshots` (62 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | uuid | NO | gen_random_uuid() |
| social_account_id | uuid | NO |  |
| video_id | varchar(255) | NO |  |
| fetched_at | timestamp with time zone | NO | now() |
| posted_at | timestamp with time zone | YES |  |
| title | text | YES |  |
| description | text | YES |  |
| duration | integer | YES |  |
| cover_image_url | text | YES |  |
| share_url | text | YES |  |
| like_count | bigint | YES |  |
| comment_count | bigint | YES |  |
| share_count | bigint | YES |  |
| view_count | bigint | YES |  |
| engagement_rate | numeric(8,4) | YES |  |

---

## 4. Struktur `l0_harmonization`

14 tabel. PK-nya `id INTEGER` (bukan UUID kayak raw), dan setiap tabel entitas (post/comment/profile/story/tagged_post/audience) punya **UNIQUE constraint di natural key** — ini yang bikin `ON CONFLICT` di procedure bisa jalan.

### 4.1 Daftar tabel `l0_harmonization`

| Tabel | Baris | Unique constraint | Grain |
|---|---|---|---|
| `facebook_profile` | 202 | `(brand_id, date)` | 1 baris per brand per hari |
| `facebook_post` | 52 | `(brand_id, post_id)` | 1 baris per post (living record) |
| `facebook_comment` | 93 | `(brand_id, post_id, comment_id)` | 1 baris per comment |
| `facebook_audience` | 0 | `(brand_id, date, audience_type, dimension_key, dimension_value)` | 1 baris per breakdown demografi per hari — **kosong, lihat bagian 6** |
| `instagram_profile` | 185 | `(brand_id, date)` | 1 baris per brand per hari |
| `instagram_post` | 171 | `(brand_id, post_id)` | 1 baris per post (living record) |
| `instagram_comment` | 1187 | `(brand_id, post_id, comment_id)` | 1 baris per comment |
| `instagram_story` | 82 | `(brand_id, story_id, date)` | 1 baris per story |
| `instagram_tagged_post` | 28 | `(brand_id, post_id)` | 1 baris per UGC post |
| `instagram_audience` | 4440 | `(brand_id, date, audience_type, dimension_key, dimension_value)` | 1 baris per breakdown demografi per hari |
| `tiktok_profile` | 182 | `(brand_id, date)` | 1 baris per brand per hari |
| `tiktok_post` | 128 | `(brand_id, post_id)` | 1 baris per post (living record) |
| `tiktok_comment` | 110 | `(brand_id, post_id, comment_id)` | 1 baris per comment — **tidak ada procedure sync-nya, lihat bagian 6** |
| `sync_log` | 6 | — | log eksekusi tiap procedure |

Semua `brand_id` di tabel-tabel ini FK ke `public.social_accounts(id)`.

### 4.2 Detail kolom per tabel `l0_harmonization`

#### `l0_harmonization.facebook_audience` (0 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| date | date | NO |  |
| audience_type | text | NO |  |
| dimension_key | text | NO |  |
| dimension_value | text | NO |  |
| value | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.facebook_comment` (93 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| comment_id | text | NO |  |
| link_post | text | YES |  |
| link_comment | text | YES |  |
| comment_time | timestamp with time zone | YES |  |
| comment_text | text | YES |  |
| comment_username | text | YES |  |
| likes_count | integer | YES | 0 |
| replies_count | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.facebook_post` (52 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| caption | text | YES |  |
| link | text | YES |  |
| post_type | text | YES |  |
| reach | integer | YES | 0 |
| impressions | integer | YES | 0 |
| reactions | integer | YES | 0 |
| likes | integer | YES | 0 |
| comments | integer | YES | 0 |
| shares | integer | YES | 0 |
| link_click | integer | YES | 0 |
| video_views | integer | YES | 0 |
| cover_image | text | YES |  |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |
| hashtag_list | text[] | YES |  |

#### `l0_harmonization.facebook_profile` (202 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| date | date | NO |  |
| profile_photo | text | YES |  |
| follower_count | integer | YES | 0 |
| page_like_count | integer | YES | 0 |
| follows_increase | integer | YES |  |
| content_interactions | integer | YES | 0 |
| link_clicks | integer | YES | 0 |
| profile_reach | integer | YES | 0 |
| content_views | integer | YES | 0 |
| profile_visit | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |
| new_followers | integer | YES |  |

#### `l0_harmonization.instagram_audience` (4440 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| date | date | NO |  |
| audience_type | text | NO |  |
| dimension_key | text | NO |  |
| dimension_value | text | NO |  |
| value | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.instagram_comment` (1187 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| comment_id | text | NO |  |
| link_post | text | YES |  |
| link_comment | text | YES |  |
| comment_time | timestamp with time zone | YES |  |
| comment_text | text | YES |  |
| comment_username | text | YES |  |
| likes_count | integer | YES | 0 |
| replies_count | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.instagram_post` (171 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| caption | text | YES |  |
| link | text | YES |  |
| post_type | text | YES |  |
| duration_s | numeric(10,3) | YES |  |
| slide_count | integer | YES |  |
| views | integer | YES | 0 |
| reach | integer | YES | 0 |
| likes | integer | YES | 0 |
| comments | integer | YES | 0 |
| shares | integer | YES | 0 |
| saves | integer | YES | 0 |
| repost | integer | YES | 0 |
| total_interactions | integer | YES | 0 |
| follows | integer | YES | 0 |
| profile_visits | integer | YES | 0 |
| reel_avg_watch_time | numeric(10,4) | YES | 0 |
| reel_video_view_total_time | bigint | YES | 0 |
| cover_image | text | YES |  |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |
| hashtag_list | text[] | YES |  |

#### `l0_harmonization.instagram_profile` (185 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| bio | text | YES |  |
| website | text | YES |  |
| date | date | NO |  |
| follower_count | integer | YES | 0 |
| following_count | integer | YES | 0 |
| account_total_post_count | integer | YES | 0 |
| followers_growth | integer | YES | 0 |
| profile_reach | integer | YES | 0 |
| profile_visit | integer | YES |  |
| content_views | integer | YES | 0 |
| profile_link_taps | integer | YES | 0 |
| likes | integer | YES | 0 |
| comments | integer | YES | 0 |
| shares | integer | YES | 0 |
| saves | integer | YES | 0 |
| replies | integer | YES | 0 |
| repost | integer | YES | 0 |
| total_interactions | integer | YES | 0 |
| accounts_engaged | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.instagram_story` (82 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| story_id | text | NO |  |
| date | timestamp with time zone | YES |  |
| link | text | YES |  |
| story_type | text | YES |  |
| reach | integer | YES |  |
| views | integer | YES |  |
| reposts | integer | YES |  |
| replies | integer | YES |  |
| shares | integer | YES |  |
| taps_forward | integer | YES |  |
| taps_back | integer | YES |  |
| exits | integer | YES |  |
| swipe_up | integer | YES |  |
| follows | integer | YES |  |
| total_interactions | integer | YES |  |
| profile_activity | integer | YES |  |
| profile_visit | integer | YES |  |
| link_video_story | text | YES |  |
| cover_image | text | YES |  |
| created_at | timestamp with time zone | NO | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.instagram_tagged_post` (28 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| caption | text | YES |  |
| post_type | text | YES |  |
| link_post | text | YES |  |
| like_count | integer | YES | 0 |
| comment_count | integer | YES | 0 |
| username | text | YES |  |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.sync_log` (6 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| log_id | bigint | NO | nextval('l0_harmonization.sync_log_log_id_seq'::regclass) |
| procedure_name | text | YES |  |
| status | text | YES |  |
| message | text | YES |  |
| executed_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.tiktok_comment` (110 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| comment_id | text | NO |  |
| link_post | text | YES |  |
| link_comment | text | YES |  |
| comment_time | timestamp with time zone | YES |  |
| comment_text | text | YES |  |
| comment_username | text | YES |  |
| likes_count | integer | YES | 0 |
| replies_count | integer | YES | 0 |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |

#### `l0_harmonization.tiktok_post` (128 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| post_id | text | NO |  |
| post_date | timestamp with time zone | YES |  |
| title | text | YES |  |
| caption | text | YES |  |
| link | text | YES |  |
| cover_image | text | YES |  |
| post_type | text | YES |  |
| slide_count | integer | YES |  |
| video_duration | integer | YES |  |
| views | integer | YES | 0 |
| likes | integer | YES | 0 |
| comments | integer | YES | 0 |
| shares | integer | YES | 0 |
| saves | integer | YES |  |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |
| hashtag_list | text[] | YES |  |

#### `l0_harmonization.tiktok_profile` (182 baris)

| Kolom | Tipe | Nullable | Default |
|---|---|---|---|
| id | integer | NO |  |
| brand_id | uuid | NO |  |
| date | date | NO |  |
| bio | text | YES |  |
| follower_count | integer | YES | 0 |
| following_count | integer | YES | 0 |
| account_total_post_count | integer | YES | 0 |
| video_views | integer | YES |  |
| profile_reach | integer | YES |  |
| profile_views | integer | YES |  |
| likes | integer | YES | 0 |
| comments | integer | YES |  |
| shares | integer | YES |  |
| net_growth | integer | YES |  |
| new_followers | integer | YES |  |
| lost_followers | integer | YES |  |
| is_verified | boolean | YES |  |
| created_at | timestamp without time zone | YES | (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'::text) |
| followers_growth | integer | YES | 0 |

---

## 5. Procedure Sinkronisasi (`l0_raw → l0_harmonization`)

Ada **14 objek** per query 10 Juli 2026 (1 function helper + 1 orchestrator + 12
procedure sync per entitas) — ⚠️ **+5 objek baru sejak 2026-08-19** (dikonfirmasi
dari `autometric/assets/harmonization_assets.py`, belum di-query ulang detail
definisinya ke DB untuk dokumen ini):

| Procedure baru (2026-08-19) | Fungsi (dari deskripsi asset Dagster) |
|---|---|
| `sp_sync_tiktok_comment_from_raw()` | Dari `l0_raw.tt_comments` (tabel raw BARU — sebelumnya tidak ada, lihat §8 poin 1 yang sekarang resolved) → `l0_harmonization.tiktok_comment`. |
| `sp_sync_tiktok_audience_from_raw()` | Sumber TikTok audience (raw TikTok tidak API — kemungkinan `l0_extra`, belum diverifikasi) → `l0_harmonization.tiktok_audience` (tabel BARU, tidak ada di tabel §4.1). |
| `sp_sync_tiktok_profile_native_from_extra()` | Dari `l0_extra.tt_profile_native` → UPDATE kolom `video_views`, `profile_reach`, `profile_views`, `comments`, `shares`, `net_growth`, `new_followers`, `lost_followers` di `l0_harmonization.tiktok_profile` (sebelumnya NULL terus). HARUS jalan setelah `sp_sync_tiktok_profile_from_raw` (match `brand_id`+`date`). |
| `sp_gapfill_profile_dates()` | Isi baris kosong (gap tanggal) profile snapshot dari tanggal akun didaftarkan sampai hari ini, untuk `facebook_profile`/`instagram_profile`/`tiktok_profile` sekaligus. LOCF untuk state field, 0 untuk flow field. MineralQUA + kompetitornya sengaja di-exclude (data demo/sintetis). |
| `sp_normalize_audience_percentage_rounding()` | Koreksi drift pembulatan pada `instagram_audience`/`tiktok_audience`: kalau total value per grup (brand_id, date, audience_type, dimension_key) 95–105 → dikoreksi ke tepat 100 (largest-remainder). Format absolut (jauh dari 100) tidak disentuh. |

Kelima procedure baru ini dipanggil lewat asset Dagster terpisah
(`harmonized_tiktok_profile_native`, `gapfilled_profile_dates`,
`normalized_audience_percentage` — lihat `dokumentasi_dagster_autometric.md` §3.2),
BUKAN lewat `sp_sync_all_from_raw()` orchestrator lama; belum dikonfirmasi apakah
orchestrator itu sendiri sudah diupdate untuk memanggilnya juga.

Daftar 14 objek asli (10 Juli 2026):

| Procedure/Function | Fungsi |
|---|---|
| `fn_extract_hashtags(text)` | Function (bukan procedure). Ekstrak semua `#hashtag` dari caption pakai regex, `lower()`-kan, dedup, balikin `text[]`. Kalau nggak ada hashtag, balikin array kosong `{}` (bukan NULL). |
| `sp_sync_all_from_raw()` | Orchestrator — manggil semua procedure di bawah secara berurutan (FB → IG → TikTok), lalu tulis hasilnya ke `sync_log`. Kalau ada error di tengah, exception ditangkap, dicatat ke `sync_log` sebagai `FAILED`, lalu di-`RAISE` ulang (supaya Dagster tahu run-nya gagal). |
| `sp_sync_facebook_profile_from_raw()` | Dari `fb_profile_snapshots` (main) + `fb_competitor_snapshots` (kompetitor) → `facebook_profile`. |
| `sp_sync_facebook_post_from_raw()` | Dari `fb_post_snapshots` (main) + `fb_competitor_media` (kompetitor) → `facebook_post`. |
| `sp_sync_facebook_comment_from_raw()` | Dari `fb_comments` → `facebook_comment`. |
| `sp_sync_facebook_audience_from_raw()` | Dari kolom `demographics_*` di `fb_profile_snapshots` → `facebook_audience`. **Ada di kode tapi baris pemanggilannya di-comment-out di `sp_sync_all_from_raw()`** — nggak jalan otomatis. |
| `sp_sync_instagram_profile_from_raw()` | Dari `ig_profile_snapshots` (main) + `ig_competitor_snapshots` (kompetitor) → `instagram_profile`. |
| `sp_sync_instagram_post_from_raw()` | Dari `ig_media_snapshots` (main) + `ig_competitor_media` (kompetitor) → `instagram_post`. |
| `sp_sync_instagram_comment_from_raw()` | Dari `ig_comments` → `instagram_comment`. Hanya proses akun **main brand** (tidak ada versi kompetitor). |
| `sp_sync_instagram_story_from_raw()` | Dari `ig_stories` → `instagram_story`. Hanya main brand. |
| `sp_sync_instagram_tagged_post_from_raw()` | Dari `ig_tagged_posts` → `instagram_tagged_post`. Hanya main brand. |
| `sp_sync_instagram_audience_from_raw()` | Dari kolom `demographics_*` di `ig_profile_snapshots` → `instagram_audience`. Aktif dipanggil di orchestrator. |
| `sp_sync_tiktok_profile_from_raw()` | Dari `tt_profile_snapshots` (main) + `tiktok_competitor_snapshots` (kompetitor) → `tiktok_profile`. |
| `sp_sync_tiktok_post_from_raw()` | Dari `tt_video_snapshots` (main) + `tiktok_competitor_media` (kompetitor) → `tiktok_post`. |

**Pola umum tiap procedure** (kecuali audience & comment-only):
1. Tentuin dulu mana `social_account_id` yang termasuk *main account* (via `public.brand_social_accounts`, filter `platforms.key`) dan mana yang *competitor account* (via `public.brand_competitors`) — dengan `NOT EXISTS` supaya akun yang udah jadi main brand nggak double-diproses sebagai kompetitor.
2. Ambil baris raw ter-relevan per entitas (dedup, lihat bagian 7).
3. `UNION ALL` main + competitor jadi satu `source_data`.
4. `INSERT ... ON CONFLICT (natural key) DO UPDATE ... WHERE (...) IS DISTINCT FROM (...)` — upsert idempotent (lihat bagian 7).
5. Khusus tabel profile (FB, IG, TikTok) — ada langkah tambahan setelah upsert: hitung ulang `follows_increase`/`followers_growth` pakai `LAG()` window function per `brand_id ORDER BY date`.

---

## 6. Penanganan NULL

Ada 3 pola berbeda tergantung jenis kolom:

### 6.1 Kolom metrik/angka → dipaksa jadi `0`, bukan NULL
Hampir semua kolom numerik (reach, likes, comments, views, dst) di-wrap `COALESCE(kolom_raw, 0)::TIPE` waktu insert ke harmonization. Ini konsisten sama `column_default = '0'` yang muncul di skema harmonization untuk kolom-kolom ini.

```sql
COALESCE(r.reach, 0)::INTEGER AS reach,
COALESCE(r.likes_count, 0)::INTEGER AS likes,
```

Efeknya: kalau API balikin NULL/nggak ada value buat suatu metrik (misal metrik itu belum kebuka izinnya, atau memang 0), di harmonization tetap kebaca `0`, **bukan NULL**. Ini best-practice buat downstream aggregation (SUM, AVG nggak keganggu NULL), tapi konsekuensinya: **secara data, kamu nggak bisa bedain "metriknya beneran 0" vs "datanya nggak ada/API-nya nggak return"** di level harmonization. Kalau butuh bedain itu, harus cek raw langsung (raw kolom-kolom metrik mayoritas nullable tanpa default).

### 6.2 Kolom teks/deskriptif → dibiarkan NULL apa adanya
Caption, link, cover_image, comment_text, username, dst — **tidak** di-COALESCE. Kalau raw-nya NULL, harmonization-nya juga NULL. Ini masuk akal karena nggak ada "nilai default" yang masuk akal buat teks/link.

### 6.3 Kolom array (`hashtag_list`) → fungsi helper jamin nggak pernah NULL
`fn_extract_hashtags()` pakai `COALESCE(array_agg(...), '{}'::text[])` di dalam function-nya sendiri — jadi walaupun caption kosong/nggak ada hashtag, hasilnya array kosong `{}`, bukan NULL.

### 6.4 Kasus khusus: `instagram_story` — `NULLIF(...,'') ` sebelum `COALESCE`
```sql
COALESCE(NULLIF(r.reach::TEXT, ''), '0')::INTEGER AS reach,
COALESCE(NULLIF(r.navigation ->> 'taps_forward', ''), '0')::INTEGER AS taps_forward,
```
Ini beda dari pattern lain — perlu `NULLIF(..., '')` dulu karena sumbernya juga narik dari JSONB (`navigation ->> 'key'`), yang bisa balikin **string kosong** (bukan NULL literal) kalau key-nya ada tapi valuenya kosong. Kalau langsung `COALESCE(x, 0)` tanpa `NULLIF`, string kosong `''` akan gagal di-cast ke INTEGER (error), bukan kejadi 0. Pattern ini nggak dipakai di procedure lain — worth diinget kalau nanti nemu bug serupa (cast error) di kolom yang ambil dari JSONB.

### 6.5 Filter join-key wajib NOT NULL
Semua procedure nge-filter raw data di awal dengan:
```sql
WHERE raw_data.social_account_id IS NOT NULL
  AND raw_data.post_id IS NOT NULL   -- (atau media_id/video_id/comment_id sesuai tabel)
```
Row raw yang key identifikasinya NULL **otomatis di-skip**, nggak pernah sampai ke harmonization. Ini karena key itu dipakai buat `DISTINCT ON` dan `ON CONFLICT` — kalau NULL, dedup & upsert-nya nggak bisa jalan benar.

### 6.6 Growth metrics (`follows_increase`, `followers_growth`) → `LAG()` + `COALESCE(...,0)`
```sql
COALESCE(
    follower_count - LAG(follower_count) OVER (PARTITION BY brand_id ORDER BY date),
    0
)::INTEGER AS new_followers_growth
```
Baris pertama per `brand_id` nggak punya "hari sebelumnya" buat dibandingin — `LAG()` balikin NULL, dan `COALESCE` men-default-kannya jadi `0`. Artinya **hari pertama data historis brand manapun, growth-nya otomatis kebaca 0** (bukan representasi growth asli, karena memang nggak ada baseline).

---

## 7. Penanganan Duplicate

Ada 3 lapis dedup yang jalan berurutan:

### 7.1 Lapis 1 — Dedup dalam satu raw table (`DISTINCT ON`)
Karena raw table nggak punya unique constraint, satu entitas (misal satu post) bisa punya banyak baris snapshot (di-fetch berkali-kali). Tiap procedure pakai `DISTINCT ON (natural_key) ... ORDER BY natural_key, [priority], fetched_at DESC, id DESC` buat ambil **cuma 1 baris "terbaik"** per key.

- Untuk entitas **per-hari** (profile, audience) → key-nya `(social_account_id, tanggal_dari_fetched_at)`, jadi kalau di-fetch 3x dalam sehari, cuma snapshot **terakhir hari itu** yang dipakai.
- Untuk entitas **living record** (post, comment, story, tagged post) → key-nya `(social_account_id, natural_id)` tanpa tanggal, jadi snapshot **paling baru dari kapanpun** yang menang, meng-overwrite versi lama.

### 7.2 Lapis 2 — Konflik main vs competitor (kalau akun sama terdaftar di dua peran)
CTE competitor selalu punya klausa:
```sql
AND NOT EXISTS (
    SELECT 1 FROM tiktok_main_accounts ma
    WHERE ma.social_account_id = bc.social_account_id
)
```
Jadi kalau satu `social_account_id` udah terdaftar sebagai main brand account, dia otomatis dikeluarkan dari jalur competitor — nggak diproses dobel.

Khusus `sp_sync_facebook_post_from_raw`, ada lapis dedup tambahan (`final_data_dedup`) yang eksplisit nge-rank `source_priority` (main = 1 menang atas competitor = 2) sebagai jaring pengaman ekstra kalau kombinasi `(brand_id, post_id)` yang sama muncul dari kedua sumber.

### 7.3 Lapis 3 — Idempotency antar-run (`ON CONFLICT ... DO UPDATE ... WHERE IS DISTINCT FROM`)
Semua insert ke harmonization pakai pattern:
```sql
ON CONFLICT (brand_id, post_id) DO UPDATE SET
    kolom_a = EXCLUDED.kolom_a, ...
WHERE (tgt.kolom_a, ...) IS DISTINCT FROM (EXCLUDED.kolom_a, ...);
```
Efeknya: kalau procedure di-run ulang (misal scheduled run harian) dan data sumbernya nggak berubah, row target **nggak ke-touch sama sekali** — nggak ada write yang sia-sia, dan `id` PK-nya tetap sama (nggak pernah delete-insert ulang). Ini penting karena Gold layer nge-JOIN balik ke sini pakai `id`/`brand_id`+key, jadi stabilitas `id` itu krusial.

⚠️ **Inkonsistensi kecil yang perlu dicatat**: pada `DO UPDATE`, kolom `created_at` **tidak** ikut di-update di procedure FB & TikTok (tetap nyimpen waktu insert pertama) — tapi di procedure IG (`comment`, `story`, `tagged_post`, `audience`) `created_at` **di-set ulang ke `NOW()`** tiap kali ada perubahan value. Kalau ada logic downstream yang mengandalkan `created_at` sebagai "kapan data ini pertama masuk", perilakunya beda antara FB/TikTok vs IG untuk tabel-tabel tersebut.

### 7.4 Kasus khusus — `audience` tables pakai reconciliation, bukan cuma upsert
`sp_sync_facebook_audience_from_raw` dan `sp_sync_instagram_audience_from_raw` beda dari yang lain: setelah upsert, ada langkah **DELETE** tambahan yang membuang baris `(brand_id, date)` lama yang kombinasi `audience_type/dimension_key/dimension_value`-nya **sudah tidak muncul lagi** di snapshot terbaru:
```sql
DELETE FROM l0_harmonization.instagram_audience tgt
USING affected_dates ad
WHERE tgt.brand_id = ad.brand_id AND tgt.date = ad.date
  AND NOT EXISTS (SELECT 1 FROM audience_rows ar WHERE ...);
```
Ini masuk akal karena breakdown demografi bisa berubah struktur antar snapshot (misal suatu rentang usia hilang dari hasil API). Tabel lain (post/comment/profile) nggak pernah delete — cuma insert/update.

### 7.5 Ringkasan alur dedup (post, sebagai contoh paling lengkap)
```
raw snapshots (banyak baris per fetch)
   │  DISTINCT ON (social_account_id, post_id) ORDER BY fetched_at DESC, id DESC
   ▼
1 baris terbaru per akun × post   [main]     1 baris terbaru per akun × post   [competitor]
   │                                                   │
   └──────────────────── UNION ALL ───────────────────┘
                          │
      DISTINCT ON (brand_id, post_id) ORDER BY source_priority, fetched_at DESC
                          ▼
              1 baris final per (brand_id, post_id)
                          │
        INSERT ... ON CONFLICT DO UPDATE WHERE IS DISTINCT FROM
                          ▼
              l0_harmonization.facebook_post (idempotent)
```

---

## 8. Temuan & Anomali (perlu dicek, belum diasumsikan penyebabnya)

Sesuai prinsip "cek realita dulu" — ini fakta dari DB, bukan tebakan gue soal penyebabnya. Perlu diverifikasi manual:

1. ~~**`tiktok_comment` punya 110 baris data, tapi tidak ada procedure/tabel raw sumbernya.**~~
   ⚠️ **RESOLVED 2026-08-19** — `sp_sync_tiktok_comment_from_raw()` sekarang ada
   (dikonfirmasi dari `harmonization_assets.py`: komentar kode eksplisit bilang
   "`sp_sync_tiktok_comment_from_raw` dibuat 2026-08-19, `l0_raw.tt_comments`
   sekarang ada"). Jadi TikTok comment sekarang punya jalur sync reguler dari raw,
   bukan lagi data misterius/tidak-refreshable. Baris raw & harmonization untuk
   sebelum 2026-08-19 kemungkinan tetap dari jalur lama (manual/backfill) yang
   disebut di catatan asli di bawah — belum diverifikasi, tapi tidak lagi relevan
   untuk data baru yang masuk setelah tanggal itu. Detail kolom `l0_raw.tt_comments`
   belum di-query ulang untuk dokumen ini (lihat catatan header).
   *(Catatan asli, sebelum 2026-08-19, untuk konteks histori):* tidak ada procedure
   `sp_sync_tiktok_comment_from_raw` maupun tabel raw TikTok comment — 110 baris
   yang ada kemungkinan masuk lewat insert manual/backfill lama.

2. **`sp_sync_facebook_audience_from_raw()` ada di database, tapi baris pemanggilannya di-comment di `sp_sync_all_from_raw()`:**
   ```sql
   -- CALL l0_harmonization.sp_sync_facebook_audience_from_raw();
   ```
   Konsisten dengan `facebook_audience` yang punya **0 baris**. Kalau memang FB audience/demographic breakdown mau dipakai, procedure-nya harus di-uncomment dan dites — kemungkinan sengaja dimatikan karena ada masalah data (misal struktur JSON demographic dari FB beda dari IG dan belum divalidasi).

3. **`facebook_profile.follows_increase` dan `facebook_profile.new_followers` sama-sama nullable tanpa default `0`** (beda dari kolom metrik lain di tabel yang sama yang defaultnya `0`). `follows_increase` emang sengaja nullable-nya nggak masalah karena selalu di-recompute lewat `LAG()`, tapi `new_followers` (asalnya dari `page_daily_follows_unique`) juga nullable — worth dicek apakah ini konsisten sama harapan Gold layer.

4. **Kolom `l0_raw.ig_comments.hidden`** ada tapi nggak dipakai sama sekali di `sp_sync_instagram_comment_from_raw()` — komentar yang di-hide di IG tetap ikut ke-sync ke harmonization tanpa dibedakan. Kalau dashboard butuh nge-exclude hidden comments, filter ini belum ada.

5. **`fb_comments.comment_user_id`** dan **`fb_comments.parent_id`** ada di raw tapi nggak dipakai di procedure sync — begitu juga `ig_comments.parent_id` dan `ig_comments.hidden`. Kolom-kolom ini "hilang" begitu masuk ke harmonization (nggak ada padanannya di `facebook_comment`/`instagram_comment`). Kalau butuh reply-threading atau hidden-comment filtering di Gold, sumbernya masih ada di raw tapi belum di-propagate.

---

## 9. Referensi Cepat — Mapping Raw → Harmonization

| Harmonization | Raw (main) | Raw (competitor) | Procedure |
|---|---|---|---|
| `facebook_profile` | `fb_profile_snapshots` | `fb_competitor_snapshots` | `sp_sync_facebook_profile_from_raw` |
| `facebook_post` | `fb_post_snapshots` | `fb_competitor_media` | `sp_sync_facebook_post_from_raw` |
| `facebook_comment` | `fb_comments` | *(via `valid_accounts`, gabung main+competitor)* | `sp_sync_facebook_comment_from_raw` |
| `facebook_audience` | `fb_profile_snapshots.demographics_*` | *(termasuk competitor via `brand_competitors`)* | `sp_sync_facebook_audience_from_raw` ⚠️ tidak aktif |
| `instagram_profile` | `ig_profile_snapshots` | `ig_competitor_snapshots` | `sp_sync_instagram_profile_from_raw` |
| `instagram_post` | `ig_media_snapshots` | `ig_competitor_media` | `sp_sync_instagram_post_from_raw` |
| `instagram_comment` | `ig_comments` | — (main only) | `sp_sync_instagram_comment_from_raw` |
| `instagram_story` | `ig_stories` | — (main only) | `sp_sync_instagram_story_from_raw` |
| `instagram_tagged_post` | `ig_tagged_posts` | — (main only) | `sp_sync_instagram_tagged_post_from_raw` |
| `instagram_audience` | `ig_profile_snapshots.demographics_*` | — (main only) | `sp_sync_instagram_audience_from_raw` |
| `tiktok_profile` | `tt_profile_snapshots` | `tiktok_competitor_snapshots` | `sp_sync_tiktok_profile_from_raw` |
| `tiktok_profile` (kolom native, ⚠️ baru 2026-08-19) | `l0_extra.tt_profile_native` | — | `sp_sync_tiktok_profile_native_from_extra` (UPDATE ke row yang sudah dibuat, jalan setelahnya) |
| `tiktok_post` | `tt_video_snapshots` | `tiktok_competitor_media` | `sp_sync_tiktok_post_from_raw` |
| `tiktok_comment` ⚠️ resolved 2026-08-19 | `l0_raw.tt_comments` (tabel baru) | belum diverifikasi | `sp_sync_tiktok_comment_from_raw` (dibuat 2026-08-19) |
| `tiktok_audience` ⚠️ baru 2026-08-19, tabel tidak ada di §4.1 | belum diverifikasi | belum diverifikasi | `sp_sync_tiktok_audience_from_raw` (dibuat 2026-08-19) |
