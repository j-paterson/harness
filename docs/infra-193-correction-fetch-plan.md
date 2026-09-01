# INFRA-193 — one supported, validated correction fetch

Assignment for the **admitted MVP slice only**: the supported
correction-fetch operation and its receiver-side count/integrity
validation, per the issue's 2026-09-01 recurrence section. The broader
merger / MCP / parallel-review backlog in that issue is explicitly NOT
in this pass.

## The recurrence, in this system

Correction `9944530cd4d64f74ad528a56ac9a6c34` for INFRA-220 was stored
durably with **four** findings and delivered intact through
`hermes-control`. The receiving Fable lead read `packets_json` with a
raw SQLite query piped through `cut -c1-2400`, reported **three**, and
delegated a rework brief that omitted the fourth entirely. The transport
was never at fault. The receiver truncated an authoritative payload
after delivery, then confirmed and delegated on the truncated view.

That is the same shape as the original Polysizer incident (findings 5–8
arrived, the Critical plus 1–4 were silently lost), reproduced by the
intake path rather than the wire.

## Why the current design permits it

- `LeadCorrectionOutbox.get(correction_id)` returns the whole record,
  but nothing requires a confirming lead to have called it. Any reader
  can look at `lead_corrections.packets_json` however it likes.
- `acknowledge(correction_id)` takes the id ALONE. It cannot tell a
  confirmation informed by the complete packet from one informed by a
  truncated glance, so nothing stands between a partial read and a
  durable confirmation.

The gap is not a missing store; it is a missing *obligation* at
confirmation.

## The change

**1. One supported fetch that is the intake path.**
A `fetch_correction` operation on the existing `hermes-command`
surface returns the COMPLETE durable record — every packet, every
field, untruncated — together with its integrity metadata:
`declared_count` (the durable number of packets) and `payload_sha256`
(over the canonically serialized packets). It validates the stored row
on the way out and fails closed on a malformed or unparseable payload.

**2. Confirmation requires proof of a complete read.**
`ack_correction` additionally requires `observed_count` and
`payload_sha256` from the caller, and refuses unless BOTH match the
durable metadata exactly. A caller who read three of four findings
cannot produce the right count; a caller who read a stale or altered
payload cannot produce the right hash. Refusal is fail-closed with zero
durable writes, leaving the correction `pending` and retryable.

This is the whole enforcement: confirmation stops being an assertion of
intent and becomes an assertion of *what was actually read*.

**3. Display truncation stays legal, after validation.**
Nothing forbids showing a shortened summary. The rule is only that the
authoritative intake — the thing confirmation and delegation rest on —
must be the validated complete fetch.

## Deliberately not in this slice

No new table, transport, envelope, or protocol. The existing correction
store, intake offer, and confirmation state are reused as they are. No
MCP surface, no merger coordinator, no parallel review lanes.

## Tests

- a correction whose payload is LARGER than terminal display limits is
  fetched complete, and every finding appears in the fetched document;
- confirmation with an `observed_count` lower than the declared count is
  REFUSED with zero durable writes and the correction stays `pending`
  (the exact 3-of-4 recurrence);
- confirmation with a mismatched `payload_sha256` is refused likewise;
- confirmation with the exact count and hash succeeds and is idempotent
  on replay;
- a malformed or unparseable stored payload fails the fetch closed
  rather than returning a partial document.
