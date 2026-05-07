from __future__ import annotations

import re

_URL_RE      = re.compile(r'https?://\S+|www\.\S+', re.IGNORECASE)
_HTML_RE     = re.compile(r'<[^>]+>')
_NONPRINT_RE = re.compile(r'[^\x20-\x7E]')   # keep printable ASCII only
_MULTI_WS_RE = re.compile(r'\s+')


def clean(text: str | None) -> str | None:
    """Clean a raw text field for NLP consumption.

    Steps (in order):
      1. Remove URLs
      2. Strip HTML/XML tags
      3. Drop non-printable characters
      4. Collapse whitespace
      5. Strip leading/trailing whitespace

    Returns None if nothing remains after cleaning.
    """
    if not text:
        return None
    text = _URL_RE.sub(' ', text)
    text = _HTML_RE.sub(' ', text)
    text = _NONPRINT_RE.sub(' ', text)
    text = _MULTI_WS_RE.sub(' ', text).strip()
    return text or None


def label_confidence(text: str | None, keywords_hit: list) -> float:
    """Keyword density in text — proxy for label reliability.

    Returns a score in [0.0, 1.0]:
      0.0 — no text or no matching keywords
      1.0 — every word in the text is a disaster keyword term

    Use to flag or down-weight low-confidence training samples.
    """
    if not text or not keywords_hit:
        return 0.0
    word_count = len(text.split())
    if word_count == 0:
        return 0.0
    keyword_words = sum(len(kw.split()) for kw in keywords_hit)
    return min(1.0, round(keyword_words / word_count, 4))
