"""Tests standard tap features using the built-in SDK tests library."""

from hotglue_singer_sdk.testing import get_standard_tap_tests
from tap_klaviyo.tap import TapKlaviyo


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(sample_config):
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapKlaviyo, config=sample_config)
    for test in tests:
        test()
