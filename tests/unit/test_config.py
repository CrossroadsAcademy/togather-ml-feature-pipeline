"""Test configuration loading."""

from src.utils.config import settings


def test_settings_load() -> None:
    """Test settings load successfully."""
    assert settings.app_name is not None
