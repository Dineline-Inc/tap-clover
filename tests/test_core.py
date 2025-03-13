"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_clover.tap import TapClover

SAMPLE_CONFIG = {
  "is_sandbox": True,
  "merchant_id": "",
  "region": "us",
  "api_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxx"
}


# Run standard built-in tap tests from the SDK:
TestTapClover = get_tap_test_class(
    tap_class=TapClover,
    config=SAMPLE_CONFIG,
)
