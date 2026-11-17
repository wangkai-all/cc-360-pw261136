"""Versioned source profiles.

A *version* is a named, immutable description of how one upstream version of a
source behaves.  Two versions of the same source can share an identity scheme
(record ids, content hashes) while disagreeing about ordering, page envelope or
revision semantics.  Nothing in the rest of the codebase may assume an implicit
"current" version: a cursor, a scan and a fingerprint all carry the version they
were produced under.

`ordering` is spelled as an explicit convention, never inferred from a version
number:

  key_ascending   the sort key is the upstream record id, ascending.  A record's
                  position is stable for its lifetime.
  key_descending  same key, descending.
  updated_desc    the sort key is the record's ``updated_at`` descending and is
                  rewritten every time the record is revised.

The upgrade described in this project is ``key_ascending -> updated_desc``: the
numeric ids stay valid requests, so an old cursor still *resolves*, but it no
longer names a position in the new order.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple

KEY_ASCENDING = "key_ascending"
KEY_DESCENDING = "key_descending"
UPDATED_DESC = "updated_desc"
ORDERINGS = (KEY_ASCENDING, KEY_DESCENDING, UPDATED_DESC)


class ProfileError(ValueError):
    """Raised when two profiles are asked to work together but cannot."""


@dataclass(frozen=True)
class SourceProfile:
    source_id: str
    version: str
    ordering: str
    page_style: str = "offset"          # cursor is an absolute offset
    page_size: int = 3
    # "frozen" | "revisable" | None.  None derives from the ordering: an
    # updated_desc source is by definition revisable.  An explicit value that
    # contradicts the ordering is rejected below rather than silently honoured.
    revisions: Optional[str] = None
    probe_offset: int = 0
    probe_limit: int = 6
    diameter_limit: int = 400           # bounded probe; never walk the whole source

    def __post_init__(self) -> None:
        if not self.source_id or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{1,40}", self.source_id):
            raise ProfileError(f"bad source id {self.source_id!r}")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,30}", self.version):
            raise ProfileError(f"bad version {self.version!r}")
        if self.ordering not in ORDERINGS:
            raise ProfileError(f"unknown ordering {self.ordering!r}")
        if self.page_style != "offset":
            raise ProfileError(f"unsupported page_style {self.page_style!r}")
        if not 1 <= self.page_size <= 100:
            raise ProfileError("page_size out of range")
        if self.revisions is None:
            object.__setattr__(
                self, "revisions",
                "revisable" if self.ordering == UPDATED_DESC else "frozen")
        if self.revisions not in ("frozen", "revisable"):
            raise ProfileError(f"unknown revision policy {self.revisions!r}")
        if self.ordering == UPDATED_DESC and self.revisions == "frozen":
            raise ProfileError(
                "updated_desc ordering cannot be 'frozen': the sort key is the "
                "revision timestamp, so revisions necessarily move records")
        if self.ordering != UPDATED_DESC and self.revisions == "revisable":
            raise ProfileError(
                f"{self.ordering} cannot be 'revisable': the sort key is the id, "
                "which a revision does not change")
        if not 0 <= self.probe_offset <= 10000:
            raise ProfileError("probe_offset out of range")
        if not 1 <= self.probe_limit <= 10:
            raise ProfileError("probe_limit out of range")
        if not 20 <= self.diameter_limit <= 10000:
            raise ProfileError("diameter_limit out of range")

    @property
    def sort_key_label(self) -> str:
        return {
            KEY_ASCENDING: "id ascending",
            KEY_DESCENDING: "id descending",
            UPDATED_DESC: "updated_at descending",
        }[self.ordering]

    def matches(self, other: Mapping[str, object]) -> Tuple[str, ...]:
        """Fields that make a cursor's recorded ordering unusable under *other*.

        Deliberately narrow.  A changed ``page_size`` yields different page
        boundaries but the same position semantics, so it does not invalidate a
        recorded ordering; a changed ``ordering`` or ``page_style`` does.
        """
        out = []
        for field in ("ordering", "page_style"):
            if getattr(self, field) != other.get(field):
                out.append(field)
        return tuple(out)

    def same_behaviour_as(self, other: "SourceProfile") -> bool:
        return (self.ordering, self.page_style, self.revisions) == (
            other.ordering, other.page_style, other.revisions)


def profiles_registry(profiles) -> Dict[Tuple[str, str], SourceProfile]:
    reg: Dict[Tuple[str, str], SourceProfile] = {}
    for p in profiles:
        key = (p.source_id, p.version)
        if key in reg:
            raise ProfileError(f"duplicate profile {key}")
        reg[key] = p
    return reg


def lookup(registry: Mapping[Tuple[str, str], SourceProfile], source_id: str, version: str) -> SourceProfile:
    try:
        return registry[(source_id, version)]
    except KeyError as exc:
        raise ProfileError(f"unknown source profile {source_id}@{version}") from exc
