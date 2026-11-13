"""The package version must follow the add-on version."""

from pathlib import Path
import re

from goes_timelapse import __version__


def test_package_version_matches_addon_config() -> None:
    """__version__ (the downloader User-Agent) equals config.yaml's version."""
    config = Path(__file__).parents[1] / "goes_timelapse" / "config.yaml"
    match = re.search(
        r'^version: "(.+)"$', config.read_text(encoding="utf-8"), re.MULTILINE
    )
    assert match is not None
    assert match.group(1) == __version__
