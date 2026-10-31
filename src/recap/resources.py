"""Locate versioned contracts in a source checkout or a regular wheel installation."""

import sys
from pathlib import Path


def resource_directory(name):
    checkout = Path(__file__).resolve().parents[2] / name
    if checkout.is_dir():
        return checkout
    installed = Path(sys.prefix) / "share" / "recap" / name
    if not installed.is_dir():
        raise FileNotFoundError(f"Missing installed RECAP resource directory: {name}")
    return installed
