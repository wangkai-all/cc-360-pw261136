"""Version-switchable ingestion: ordering probes, cursor safety, verification.

Nothing here trusts a version string or a record total.  See ``probe.py`` for the
ordering evidence rule and ``verify.py`` for why closure is per id, not per count.
"""

__all__ = ["versions", "upstream", "journal", "probe", "harvester",
           "collector", "verify", "scenario", "demo", "server"]
__version__ = "1.0.0"
