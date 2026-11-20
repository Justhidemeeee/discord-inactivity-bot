"""Detect names that impersonate a protected member.

Names are normalized before matching so common disguises don't slip
through: case, accents and fancy Unicode letters (NFKD), leetspeak digits,
and separators ("J.u_s-t h1de me" -> "justhideme").

Long terms match anywhere in the collapsed name. Short terms (3 chars or
fewer, like "jhm") only match as a separate word, so "ajhmo" isn't flagged.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Optional

_LEET = str.maketrans({"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t",
                       "@": "a", "$": "s", "!": "i", "|": "i"})


def _fold(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower().translate(_LEET)


def _collapse(text: str) -> str:
    return re.sub(r"[^a-z]", "", _fold(text))


def _words(text: str) -> set[str]:
    return set(re.split(r"[^a-z]+", _fold(text))) - {""}


def impersonates(names: Iterable[Optional[str]], terms: Iterable[str]) -> bool:
    """True if any of `names` contains any of `terms` (after normalization)."""
    names = [n for n in names if n]
    for term in terms:
        t = _collapse(term)
        if not t:
            continue
        for name in names:
            if len(t) <= 3:
                if t in _words(name):
                    return True
            elif t in _collapse(name):
                return True
    return False
