"""
Logika scoring NLP murni (tanpa Dagster), dipanggil oleh feature_assets.py.

Langkah 14 — dua fungsi:
  1. compute_relevance_scores(): cosine similarity comment_text vs caption post induk,
     diskalakan ke 0-100.
  2. compute_word_frequencies(): top-N kata per (brand, platform), buang stopword ID + EN.

Catatan desain:
  - Join comment->post pakai (post_id, platform), BUKAN post_id saja, karena post_id
    bisa tabrakan antar platform.
  - Caption NULL/kosong -> comment-nya di-SKIP (bukan diberi skor 0), karena tidak ada
    pembanding. Skor 0 menyesatkan (seolah "tidak relevan").
  - brand_id adalah UUID; di sini diperlakukan sebagai string.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Iterable

# Model multilingual; util cosine dari sentence-transformers (import lazy di
# dalam fungsi supaya modul ini tidak memicu load torch saat Dagster
# membaca definitions).


# --- Stopword ID + EN -----------------------------------------------------
# Daftar inti; cukup untuk word cloud. Bisa diperluas kapan saja.
_STOPWORDS_ID = {
    "yang", "untuk", "dengan", "pada", "dari", "dan", "di", "ke", "ini", "itu",
    "atau", "juga", "akan", "ada", "tidak", "sudah", "saya", "kamu", "aku",
    "dia", "kami", "kita", "mereka", "nya", "ya", "ga", "gak", "nggak", "deh",
    "dong", "sih", "aja", "kan", "kok", "yg", "utk", "dgn", "jadi", "bisa",
    "karena", "kalau", "kalo", "biar", "lagi", "banget", "bgt", "udah", "blm",
    "belum", "tapi", "tp", "buat", "punya", "mau", "lebih", "masih", "apa",
}
_STOPWORDS_EN = {
    "the", "a", "an", "and", "or", "but", "is", "are", "was", "were", "be",
    "been", "to", "of", "in", "on", "at", "for", "with", "by", "this", "that",
    "it", "its", "as", "from", "you", "your", "i", "we", "they", "he", "she",
    "my", "me", "so", "if", "not", "no", "yes", "do", "did", "have", "has",
    "will", "can", "just", "really", "very", "too", "also", "more", "all",
}
STOPWORDS = _STOPWORDS_ID | _STOPWORDS_EN

# Token = rangkaian huruf/angka, minimal 3 char. Buang emoji, tanda baca, mention/hashtag symbol.
_TOKEN_RE = re.compile(r"[a-zA-Z\u00C0-\u024F0-9]{3,}")


def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    tokens = _TOKEN_RE.findall(text.lower())
    return [t for t in tokens if t not in STOPWORDS and not t.isdigit()]


def compute_relevance_scores(
    comments: list[dict],
    captions_by_post: dict[tuple[str, str], str],
    model,
) -> list[dict]:
    """
    Hitung relevance score tiap comment terhadap caption post induknya.

    Args:
        comments: list dict, tiap dict minimal punya:
            comment_id (str), platform (str), brand_id (str),
            post_id (str), comment_text (str)
        captions_by_post: mapping (post_id, platform) -> caption text.
        model: SentenceTransformer (dari SentenceTransformerResource.model).

    Returns:
        list dict siap di-insert ke feature.comment_relevance_scores:
            comment_id, platform, brand_id, relevance_score (0-100 float).
        Comment yang caption-nya tidak ada / kosong / comment_text kosong di-SKIP.
    """
    from sentence_transformers import util

    # Saring comment yang punya pasangan caption valid.
    valid = []
    for c in comments:
        ctext = (c.get("comment_text") or "").strip()
        if not ctext:
            continue
        caption = captions_by_post.get((c.get("post_id"), c.get("platform")))
        if not caption or not caption.strip():
            continue
        valid.append((c, ctext, caption.strip()))

    if not valid:
        return []

    comment_texts = [v[1] for v in valid]
    caption_texts = [v[2] for v in valid]

    # Encode batch sekaligus (lebih cepat dari satu-satu).
    emb_comments = model.encode(comment_texts, convert_to_tensor=True, show_progress_bar=False)
    emb_captions = model.encode(caption_texts, convert_to_tensor=True, show_progress_bar=False)

    results = []
    for i, (c, _, _) in enumerate(valid):
        cos = float(util.cos_sim(emb_comments[i], emb_captions[i]).item())
        # cosine [-1,1] -> clamp ke [0,1] -> skala 0-100.
        score = max(0.0, min(1.0, cos)) * 100.0
        results.append({
            "comment_id": c["comment_id"],
            "platform": c["platform"],
            "brand_id": c["brand_id"],
            "relevance_score": round(score, 2),
        })
    return results


def compute_wordcloud_per_post(
    comments: Iterable[dict],
    top_n: int = 50,
) -> list[dict]:
    """
    Hitung top-N kata per (post_id, platform) dari comment_text.
    Dipakai untuk word cloud level POST (halaman Campaign Analysis).

    Beda dari compute_word_frequencies (yang grain-nya brand+platform):
    fungsi ini grain-nya post+platform, jadi tiap post punya word cloud sendiri.
    Pembersihan teks (stopword ID+EN, emoji, token < 3 char, angka) memakai
    _tokenize yang sama -> logika bersih-bersih tidak diduplikasi.

    Args:
        comments: iterable dict, tiap dict minimal punya:
            post_id (str), platform (str), comment_text (str).
        top_n: berapa kata teratas disimpan per post (default 50).
            Disimpan agak besar supaya developer bebas set top-N lebih kecil
            di UI (ORDER BY frequency DESC LIMIT n), tanpa tabel membengkak
            oleh ekor kata langka.

    Returns:
        list dict siap di-insert ke l2_gold.post_wordcloud:
            post_id, platform, word, frequency.
        Post tanpa token valid (komentar kosong / semua stopword) di-skip.
    """
    counters: dict[tuple[str, str], Counter] = {}
    for c in comments:
        key = (c.get("post_id"), c.get("platform"))
        if key[0] is None or key[1] is None:
            continue
        counters.setdefault(key, Counter()).update(_tokenize(c.get("comment_text") or ""))

    rows = []
    for (post_id, platform), counter in counters.items():
        for word, freq in counter.most_common(top_n):
            rows.append({
                "post_id": post_id,
                "platform": platform,
                "word": word,
                "frequency": freq,
            })
    return rows


def compute_wordcloud_per_sentiment_month(
    comments: Iterable[dict],
    top_n: int = 50,
) -> list[dict]:
    """
    Hitung top-N kata per (brand_id, platform, sentiment_label, period_month)
    dari comment_text. Dipakai untuk word cloud di section Audience Sentiment
    (l2_gold.comment_wordcloud_sentiment) -- beda dari compute_wordcloud_per_post
    (grain post+platform) dan compute_word_frequencies (grain brand+platform,
    tanpa split sentiment/bulan). Reuse _tokenize yang sama, logika bersih-bersih
    tidak diduplikasi.

    Args:
        comments: iterable dict, tiap dict minimal punya:
            brand_id (str), platform (str), sentiment_label (str),
            period_month (date), comment_text (str).
        top_n: berapa kata teratas disimpan per grup (default 50).

    Returns:
        list dict siap di-insert ke l2_gold.comment_wordcloud_sentiment:
            brand_id, platform, sentiment_label, period_month, word, frequency.
        Grup tanpa token valid (komentar kosong / semua stopword) di-skip.
    """
    counters: dict[tuple, Counter] = {}
    for c in comments:
        key = (c.get("brand_id"), c.get("platform"), c.get("sentiment_label"), c.get("period_month"))
        if any(k is None for k in key):
            continue
        counters.setdefault(key, Counter()).update(_tokenize(c.get("comment_text") or ""))

    rows = []
    for (brand_id, platform, sentiment_label, period_month), counter in counters.items():
        for word, freq in counter.most_common(top_n):
            rows.append({
                "brand_id": brand_id,
                "platform": platform,
                "sentiment_label": sentiment_label,
                "period_month": period_month,
                "word": word,
                "frequency": freq,
            })
    return rows


def compute_word_frequencies(
    comments: Iterable[dict],
    top_n: int = 50,
) -> list[dict]:
    """
    Hitung top-N kata per (brand_id, platform) dari comment_text.
    Stopword ID + EN dibuang, token < 3 char dibuang, angka murni dibuang.

    Returns:
        list dict siap di-insert ke feature.word_frequencies:
            brand_id, platform, word, frequency.
    """
    # Counter per (brand_id, platform).
    counters: dict[tuple[str, str], Counter] = {}
    for c in comments:
        key = (c.get("brand_id"), c.get("platform"))
        if key[0] is None or key[1] is None:
            continue
        counters.setdefault(key, Counter()).update(_tokenize(c.get("comment_text") or ""))

    rows = []
    for (brand_id, platform), counter in counters.items():
        for word, freq in counter.most_common(top_n):
            rows.append({
                "brand_id": brand_id,
                "platform": platform,
                "word": word,
                "frequency": freq,
            })
    return rows