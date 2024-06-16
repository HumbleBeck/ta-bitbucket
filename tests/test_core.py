"""Tests standard tap features using the built-in SDK tests library."""

import json

from singer_sdk.testing import get_tap_test_class

from tap_bitbucket.tap import TapBitbucket



with open(".secrets/config.json", mode="r", encoding="UTF-8") as f:
    SAMPLE_CONFIG = json.loads(f.read())


    # Run standard built-in tap tests from the SDK:
    TestTapBitbucket = get_tap_test_class(
        tap_class=TapBitbucket,
        config=SAMPLE_CONFIG,
    )

