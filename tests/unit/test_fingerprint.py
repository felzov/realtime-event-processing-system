"""The event fingerprint (ADR 0004, decision 13; phase 5 criterion 5)."""

import json
from typing import Any

import pytest

from event_pipeline.domain.events import parse_event
from event_pipeline.domain.fingerprint import FINGERPRINT_BYTES, fingerprint
from tests.samples import encode, valid_event


def fingerprint_of(body: dict[str, Any]) -> bytes:
    return fingerprint(parse_event(encode(body)))


def test_is_a_sha256() -> None:
    assert FINGERPRINT_BYTES == 32
    assert len(fingerprint_of(valid_event())) == FINGERPRINT_BYTES


# The sample event's fingerprint. Fingerprints are stored: if this value changes, every event
# recorded before the change looks like a conflict when redelivered (domain/fingerprint.py).
# Change it only together with a decision on the stored ones, never to make this test pass.
SAMPLE_FINGERPRINT = "c4f71be1d5dca21daf89ad811299e8ff992f7820eefb34a7d258b8c3fc5a0973"


def test_the_canonical_form_does_not_change_by_accident() -> None:
    assert fingerprint_of(valid_event()).hex() == SAMPLE_FINGERPRINT


@pytest.mark.parametrize(
    "occurred_at",
    [
        "2026-11-18T10:00:30Z",  # the sample's 12:00:30+02:00 in UTC
        "2026-11-18T10:00:30.000Z",
        "2026-11-18T10:00:30.000000+00:00",
        "2026-11-18T05:00:30-05:00",
        "2026-11-18t10:00:30z",
    ],
)
def test_the_same_instant_spelled_differently_has_the_same_fingerprint(occurred_at: str) -> None:
    body = valid_event()
    body["occurred_at"] = occurred_at

    assert fingerprint_of(body) == fingerprint_of(valid_event())


def test_key_order_and_whitespace_do_not_matter() -> None:
    body = valid_event()
    reordered = dict(reversed(list(body.items())))
    reordered["payload"] = dict(reversed(list(body["payload"].items())))
    raw = json.dumps(reordered, indent=4).encode()

    assert fingerprint(parse_event(raw)) == fingerprint_of(body)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("occurred_at",), "2026-11-18T10:00:30.000001Z"),
        (("source",), "web-shop.refunds"),
        (("payload", "order_id"), "ord-other"),
        (("payload", "store_id"), "store-other"),
        (("payload", "amount_minor"), 2600),
        (("payload", "currency"), "USD"),
        (("event_id",), "00000000-0000-4000-8000-000000000000"),
    ],
    ids=lambda v: ".".join(v) if isinstance(v, tuple) else None,
)
def test_any_other_value_changes_the_fingerprint(path: tuple[str, ...], value: Any) -> None:
    body = valid_event()
    target = body
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    assert fingerprint_of(body) != fingerprint_of(valid_event())
