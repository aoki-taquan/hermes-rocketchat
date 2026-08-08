"""Rocket.Chat gateway plugin for Hermes Agent."""
try:
    # Normal path: Hermes loads this as a package (``hermes_plugins.…``).
    from .adapter import register
except ImportError:  # pragma: no cover
    # Imported as a top-level module (e.g. by a test collector that treats
    # the repo root as a package). Fall back to an absolute import.
    import os
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from adapter import register

__all__ = ["register"]
