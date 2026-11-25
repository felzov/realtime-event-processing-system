"""What makes two copies of an event "the same event" (ADR 0004, decision 13).

The fingerprint is a SHA-256 of the parsed event as canonical JSON: sorted keys, no whitespace,
`occurred_at` as a UTC instant with microseconds. So two copies that differ only in how they were
written (key order, spacing, the UTC offset of `occurred_at`, `.000` fractions) have the same
fingerprint, and two copies that differ in any value of the parsed event do not (instants to the
microsecond: the parser keeps no more). `received_at` is a Kafka header set by ingest, not part
of the event, so a client retry of the same event matches.

The fingerprint covers the parsed `schema_version` 1 fields: a future version 2 copy of the same
event would hash differently and be reported as a conflict.

The canonical form is stored data: every fingerprint in processed_events was made with it. Changing
it (another time format, separators, fields) makes every event recorded before the change look like
a conflict when it is redelivered or replayed. tests/unit/test_fingerprint.py pins one known value
so that such a change cannot happen by accident.
"""

import hashlib
import json
from datetime import UTC
from typing import Final

from event_pipeline.domain.events import ORDER_PLACED, OrderPlacedEvent

FINGERPRINT_BYTES: Final = hashlib.sha256().digest_size  # 32


def fingerprint(event: OrderPlacedEvent) -> bytes:
    """The SHA-256 of `event` as canonical JSON."""
    canonical = {
        "event_id": str(event.event_id),
        "event_type": ORDER_PLACED,
        "schema_version": event.schema_version,
        # Always six fraction digits and Z: one spelling per instant.
        "occurred_at": event.occurred_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "source": event.source,
        "payload": event.payload.model_dump(mode="json"),
    }
    text = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).digest()
