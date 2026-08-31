"""Pure fixed-width rendering of dashboard snapshots.

Functions here perform no I/O and read no clocks: every timestamp on
the dashboard was injected into the snapshot or the failure fact by the
caller, so rendering the same input always yields the same lines and
always the same number of them (a stable pane height).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from hermes_orchestrator.dashboard_sources import (
    CapacityFact,
    CodexFact,
    DashboardSnapshot,
    IdleFact,
    ProfileLeaseFact,
    ProfileUsage,
    ResourceFact,
    TaskFact,
    TransitionFact,
    UsageWindows,
    WorkerFact,
)


@dataclass(frozen=True, slots=True)
class TickFailure:
    """A recorded refresh-tick failure shown on the next success."""

    at: str
    reason: str


def render_dashboard(
    snapshot: DashboardSnapshot,
    *,
    width: int = 80,
    failure: TickFailure | None = None,
) -> tuple[str, ...]:
    """Render one snapshot to a fixed-width, fixed-height block."""

    leases = {lease.profile_alias: lease for lease in snapshot.leases}
    lines = [
        f"HERMES ORCHESTRATOR {snapshot.generated_at}",
        *(
            _profile_line(usage, leases.get(usage.profile_alias))
            for usage in snapshot.usage
        ),
        _codex_line(snapshot.codex),
        _status_line(failure),
    ]
    return tuple(_fit(line, width) for line in lines)


def _profile_line(usage: ProfileUsage, lease: ProfileLeaseFact | None) -> str:
    if lease is None:
        lease_text = "no lease"
    else:
        lease_text = f"{lease.project_key}/{lease.state}"
        if lease.cooldown_until is not None:
            lease_text += f" cooldown until {lease.cooldown_until}"
    return (
        f"{usage.profile_alias:<8} "
        f"fable {usage.fable_tokens:>12} "
        f"overall {usage.overall_tokens:>12}  {lease_text}"
    )


def _codex_line(codex: CodexFact) -> str:
    if not codex.available:
        return f"codex    unavailable since {codex.unavailable_since}"
    primary = _percent(codex.primary_used_percent)
    secondary = _percent(codex.secondary_used_percent)
    reached = " limit reached" if codex.reached else ""
    return f"codex    primary {primary} secondary {secondary}{reached}"


def _status_line(failure: TickFailure | None) -> str:
    if failure is None:
        return "last tick ok"
    return f"last tick failed at {failure.at} ({failure.reason})"


def _percent(value: int | None) -> str:
    return "?" if value is None else f"{value}%"


def _fit(line: str, width: int) -> str:
    return line[:width].ljust(width)


# ---------------------------------------------------------------------------
# INFRA-209 (requirements reread + narrow-width follow-up): the single
# continuously updated frame for the left pane — header, Work, Capacity,
# (Detail), System, Attention.
#
# render_frame is pure and deterministic: every timestamp compared here
# is either injected as `now` or already frozen on the snapshot, so
# equal inputs always render equal output. Styling is applied last, as
# an invisible (zero visible-width) overlay on already width-fitted
# plain text, so `color=True` never changes any line's visible length.
# Two private sentinel marker pairs stand in for "bold this span" and
# "dim this span" during construction; `_finalize` either turns them
# into real SGR codes (color=True) or strips them (color=False) —
# `visible_length`/`_fit_content` treat them as zero-width either way,
# so fitting never has to know whether a span will end up styled.
#
# NO MID-TOKEN TRUNCATION: sections build width-adaptive content first
# (Capacity/System switch to a two-line-per-row layout below 72 cols;
# Capacity drops fields in a fixed priority; Work's per-project `last:`
# line wraps onto a second indented line, or drops its tail with a
# trailing ellipsis when there is no room to wrap). Whatever residual
# overflow remains after that is handled once, uniformly, by
# `_fit_content` in `_finalize`: it never cuts inside a word — it backs
# off to the last space and appends a single `…` — so nothing anywhere
# on the frame is ever truncated mid-token.
# ---------------------------------------------------------------------------

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_BOLD_OPEN, _BOLD_CLOSE = "\x00B", "\x00b"
_DIM_OPEN, _DIM_CLOSE = "\x00D", "\x00d"
_MARKER_RE = re.compile("[\x00][BbDd]")
_INVISIBLE_RE = re.compile(r"\x1b\[[0-9;]*m|[\x00][BbDd]")

_GREEN = "\x1b[32m"
_AMBER = "\x1b[33m"
_RED = "\x1b[31m"
_RESET = "\x1b[0m"
_MARKER_CODES = {
    _BOLD_OPEN: "\x1b[1m",
    _BOLD_CLOSE: _RESET,
    _DIM_OPEN: "\x1b[2m",
    _DIM_CLOSE: _RESET,
}

# The exact whole words this renderer ever colors, and their color —
# green=healthy/confirmed, amber=warning/stale/near-cap, red=blocked/
# capped/error (the operator palette). No other text is ever wrapped.
# "Paused" is deliberately NOT here: it is dimmed (a marker span), not
# given a semantic color (INFRA-209 narrow-width follow-up point 4).
_COLOR_WORDS = {
    "green": _GREEN,
    "yellow": _AMBER,
    "red": _RED,
    "available": _GREEN,
    "merged": _GREEN,
    "settled": _GREEN,
    "approved": _GREEN,
    "stale": _AMBER,
    "corrections": _AMBER,
    "capped": _RED,
    "Blocked": _RED,
    "unavailable": _RED,
}
_COLOR_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(word) for word in _COLOR_WORDS) + r")\b"
)

_RESOURCE_STALE_SECONDS = 90
# The merged cache+observation staleness bound for Capacity (distinct
# from CAPACITY_EVIDENCE_FRESHNESS, which bounded the pre-cache,
# observation-only design and is no longer read here).
_CACHE_STALE_BOUND = timedelta(hours=6)
_NEAR_CAP_REMAINING = 15  # near cap when any unexpired window's remaining% <= this
_NARROW_WIDTH = 72  # below this, Capacity and System switch to two-line rows

_REVIEW_STATE_LABELS = {
    "approved": "approved",
    "corrections_required": "corrections",
    "merged": "merged",
    "blocked": "blocked",
    "reconciliation_required": "reconciliation",
    "superseded": "superseded",
    "qa_rejected": "qa_rejected",
}
_CAPACITY_SOURCE_LABELS = {
    "provider_limit": "provider",
    "operator_attestation": "operator",
}


def visible_length(text: str) -> int:
    """Return the length of text with ANSI/marker sequences stripped."""

    return len(_INVISIBLE_RE.sub("", text))


def render_frame(
    snapshot: DashboardSnapshot,
    *,
    width: int,
    height: int,
    now: datetime,
    failure: TickFailure | None = None,
    color: bool = False,
    detail: bool = False,
) -> tuple[str, ...]:
    """Render one continuously updated frame, redrawn in place.

    Returns exactly `height` lines, each fitted to `width` by visible
    length. Sections collapse to fit a small pane: Work is trimmed
    first (down to a `+N more` row), then Detail (when requested),
    then Capacity, then System are dropped whole if there still is not
    enough room, keeping the header, Attention, and the last-tick line
    as the floor.
    """

    width = max(int(width), 1)
    height = max(int(height), 1)

    leases_by_alias = {lease.profile_alias: lease for lease in snapshot.leases}
    capacity_by_alias = {fact.profile_alias: fact for fact in snapshot.capacity}
    usage_by_alias = {usage.profile_alias: usage for usage in snapshot.usage}

    header_line = _header_text(snapshot, now)
    tick_line = _status_line(failure)

    attention_block = [
        _divider_text("Attention", width),
        _attention_text(snapshot, now),
    ]

    system_block = _system_block(
        snapshot.resource, snapshot.workers, now, width, detail
    )

    capacity_rows: list[str] = []
    for usage in snapshot.usage:
        capacity_rows.extend(
            _capacity_lines(
                usage.profile_alias,
                capacity_by_alias.get(usage.profile_alias),
                leases_by_alias.get(usage.profile_alias),
                now,
                width,
            )
        )
    capacity_rows.append(_codex_capacity_row(snapshot.codex, now))
    capacity_block = [_divider_text("Capacity", width), *capacity_rows]

    detail_block: list[str] = []
    if detail:
        detail_rows = [
            _detail_row(
                usage_by_alias[alias], capacity_by_alias.get(alias)
            )
            for alias in usage_by_alias
        ]
        detail_block = [_divider_text("Detail", width), *detail_rows]

    floor = [header_line, *attention_block, tick_line]
    remaining = height - len(floor)

    keep_system = remaining >= len(system_block)
    if keep_system:
        remaining -= len(system_block)
    keep_capacity = remaining >= len(capacity_block)
    if keep_capacity:
        remaining -= len(capacity_block)
    keep_detail = detail and remaining >= len(detail_block)
    if keep_detail:
        remaining -= len(detail_block)

    work_lines = _work_block(
        snapshot.tasks,
        snapshot.idle,
        snapshot.transitions,
        snapshot.resource,
        now,
        width,
        max(remaining, 0),
    )

    body = [header_line, *work_lines]
    if keep_capacity:
        body.extend(capacity_block)
    if keep_detail:
        body.extend(detail_block)
    if keep_system:
        body.extend(system_block)
    body.extend(attention_block)
    body.append(tick_line)

    # Safety net: whatever the section arithmetic above produced, the
    # frame is always exactly `height` lines — pane redraws never
    # append or scroll, so a short pane still gets a full-height block.
    if len(body) < height:
        body.extend([""] * (height - len(body)))
    elif len(body) > height:
        body = body[:height]

    return tuple(_finalize(line, width, color) for line in body)


def _finalize(line: str, width: int, color: bool) -> str:
    fitted = _fit_content(line, width)
    if color:
        fitted = _colorize(fitted)
    styled = _apply_markers(fitted, color)
    if color:
        # A bold/dim/color open code can be truncated away from its own
        # close marker when a span runs past `width` — an unconditional
        # trailing reset guarantees SGR state never bleeds into the
        # next line regardless.
        styled += _RESET
    return styled


def _fit_content(line: str, width: int) -> str:
    """Pad or truncate to exactly `width` visible chars, never mid-token.

    Padding is plain spaces. Truncation never cuts inside a word: it
    backs off to the last space at or before the budget and appends a
    single `…`, then pads out to the exact width if the backoff left
    it short.
    """

    length = visible_length(line)
    if length == width:
        return line
    if length < width:
        return line + " " * (width - length)
    ellipsized = _ellipsize_visible(line, width)
    ellipsized_length = visible_length(ellipsized)
    if ellipsized_length < width:
        ellipsized += " " * (width - ellipsized_length)
    return ellipsized


def _ellipsize_visible(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if width == 1:
        return "…"
    budget = width - 1
    kept: list[str] = []
    visible_count = 0
    last_space_cut: int | None = None
    index = 0
    truncated = False
    while index < len(text):
        match = _INVISIBLE_RE.match(text, index)
        if match:
            kept.append(match.group(0))
            index = match.end()
            continue
        if visible_count >= budget:
            truncated = True
            break
        char = text[index]
        kept.append(char)
        visible_count += 1
        if char == " ":
            last_space_cut = len(kept)
        index += 1
    else:
        truncated = index < len(text)
    if not truncated:
        return text
    if last_space_cut is not None:
        kept = kept[:last_space_cut]
    result = "".join(kept).rstrip()
    return result + "…"


def _colorize(text: str) -> str:
    def _wrap(match: re.Match[str]) -> str:
        word = match.group(1)
        return f"{_COLOR_WORDS[word]}{word}{_RESET}"

    return _COLOR_PATTERN.sub(_wrap, text)


def _apply_markers(text: str, color: bool) -> str:
    if color:
        return _MARKER_RE.sub(lambda m: _MARKER_CODES[m.group(0)], text)
    return _MARKER_RE.sub("", text)


def _header_text(snapshot: DashboardSnapshot, now: datetime) -> str:
    projects = sorted({task.project_key for task in snapshot.tasks})
    project_text = ",".join(projects) if projects else "no active projects"
    return f"HERMES {now.strftime('%H:%M:%S')}Z  {project_text}"


def _divider_text(label: str, width: int) -> str:
    # Short and fixed: `_fit_content` pads the rest with spaces at any
    # realistic width, so the dash fill never needs to encode `width`.
    return f"{_DIM_OPEN}── {label} ──{_DIM_CLOSE}"


def _wrap_plain(text: str, width: int) -> tuple[str, str | None]:
    """Split plain (marker/ANSI-free) text at the last word boundary at
    or before `width`; never splits a word. Returns (first, rest)."""

    if len(text) <= width:
        return text, None
    cut = text.rfind(" ", 0, width)
    if cut <= 0:
        cut = width
    first = text[:cut].rstrip()
    rest = text[cut:].lstrip()
    return first, (rest or None)


# ---------------------------------------------------------------------------
# Work
# ---------------------------------------------------------------------------


def _pr_text(
    pr_number: int | None, review_state: str | None, settlement_state: str | None
) -> str | None:
    if pr_number is None:
        return None
    label = (
        _REVIEW_STATE_LABELS.get(review_state, review_state)
        if review_state
        else "?"
    )
    # Sol correction deacc190: pr_number 0 is the explicit no-PR state a
    # corrections_required verdict may carry before Sol opens the sole
    # pull request (INFRA-202). Never point the operator at PR#0.
    if pr_number == 0:
        return f"pre-PR {label}"
    text = f"PR#{pr_number} {label}"
    if settlement_state:
        text += f" {settlement_state}"
    return text


def _work_row(task: TaskFact) -> str:
    issue = task.issue_id
    if task.operator_state == "Working":
        issue = f"{_BOLD_OPEN}{issue}{_BOLD_CLOSE}"
    if task.operator_state == "Paused":
        state_text = f"{_DIM_OPEN}Paused{_DIM_CLOSE}"
    else:
        state_text = task.operator_state
    lead = task.lead_profile or "-"
    parts = [issue, state_text, lead]
    # Children belong to the lead's session, not to every issue it
    # touches — show `kids` only on the row currently doing the work,
    # and only when there actually are any (INFRA-209 follow-up 1, 3).
    if task.operator_state == "Working" and task.children_total > 0:
        parts.append(f"kids {task.children_completed}/{task.children_total}")
    pr_text = _pr_text(task.pr_number, task.review_state, task.settlement_state)
    if pr_text is not None:
        parts.append(pr_text)
    return " ".join(parts)


def _idle_row(fact: IdleFact, resource: ResourceFact | None, now: datetime) -> str:
    hold = fact.hold
    if hold == "capacity limited" and resource is not None:
        hold = f"{hold} ({resource.pressure}, sample {_age(resource.sampled_at, now)})"
    return f"{fact.project_key}: {hold}"


def _transition_rows(
    transition: TransitionFact, now: datetime, width: int, *, allow_wrap: bool
) -> list[str]:
    text = (
        f"{transition.project_key} last: "
        f"{_age(transition.occurred_at, now)} {transition.phrase}"
    )
    if len(text) <= width:
        return [text]
    if not allow_wrap:
        # No room for a second line: drop the tail with a trailing
        # ellipsis (word-boundary safe) rather than truncate mid-token.
        return [_fit_content(text, width)]
    first, rest = _wrap_plain(text, width)
    if rest is None:
        return [first]
    indent = "  "
    second = indent + rest
    if len(second) > width:
        second = _fit_content(second, width)
    return [first, second]


def _work_block(
    tasks: tuple[TaskFact, ...],
    idle: tuple[IdleFact, ...],
    transitions: tuple[TransitionFact, ...],
    resource: ResourceFact | None,
    now: datetime,
    width: int,
    room: int,
) -> list[str]:
    if room <= 0:
        return []
    divider = _divider_text("Work", width)
    content_room = room - 1
    if content_room <= 0:
        return [divider]

    task_rows = [_work_row(task) for task in tasks]
    task_rows.extend(_idle_row(fact, resource, now) for fact in idle)
    rows: list[str] = []
    trimmed = 0

    for index, row in enumerate(task_rows):
        if len(rows) < content_room:
            rows.append(row)
        else:
            trimmed += len(task_rows) - index
            break

    remaining_transitions = list(transitions)
    for index, transition in enumerate(remaining_transitions):
        budget_left = content_room - len(rows)
        if budget_left <= 0:
            trimmed += len(remaining_transitions) - index
            break
        wrapped = _transition_rows(
            transition, now, width, allow_wrap=budget_left >= 2
        )
        if len(rows) + len(wrapped) <= content_room:
            rows.extend(wrapped)
        else:
            trimmed += len(remaining_transitions) - index
            break

    if trimmed:
        if len(rows) >= content_room:
            rows = rows[: max(content_room - 1, 0)]
        rows.append(f"+{trimmed} more")

    if not rows:
        rows = ["no in-flight work"]

    rows = rows[:content_room]
    rows.extend([""] * max(content_room - len(rows), 0))
    return [divider, *rows]


# ---------------------------------------------------------------------------
# Capacity
# ---------------------------------------------------------------------------


def _remaining(used: int | None) -> int | None:
    if used is None:
        return None
    return max(0, min(100, 100 - used))


def _window_expired(resets_at: str | None, now: datetime) -> bool:
    return resets_at is not None and _parse(resets_at) <= now


def _window_token(
    used: int | None, resets_at: str | None, label: str, now: datetime
) -> str:
    if used is None:
        return f"?%/{label}"
    if _window_expired(resets_at, now):
        return f"{label} reset"
    return f"{_remaining(used)}%/{label}"


def _numbers_freshness(windows: UsageWindows, now: datetime) -> str:
    if windows.fetched_at is None:
        return ""
    stale = _age_seconds(windows.fetched_at, now) > _CACHE_STALE_BOUND.total_seconds()
    return f"(cache {_age(windows.fetched_at, now)}{' stale' if stale else ''})"


def _numbers_text(
    windows: UsageWindows,
    now: datetime,
    *,
    include_five_hour: bool = True,
    include_freshness: bool = True,
) -> str:
    seven_day = _window_token(
        windows.seven_day_used, windows.seven_day_resets_at, "7d", now
    )
    fable = _window_token(windows.fable_used, windows.fable_resets_at, "7d", now)
    all_text = f"all {seven_day}"
    if include_five_hour:
        five_hour = _window_token(
            windows.five_hour_used, windows.five_hour_resets_at, "5h", now
        )
        all_text += f" {five_hour}"
    text = f"{all_text}  fable {fable}"
    if include_freshness:
        freshness = _numbers_freshness(windows, now)
        if freshness:
            text += f"  {freshness}"
    return text


def _valid_remaining(
    used: int | None, resets_at: str | None, now: datetime
) -> int | None:
    """A window's remaining% only counts as capacity EVIDENCE while its
    reset horizon has not yet passed — an expired window's old
    percentage is not evidence of anything current."""

    if used is None or _window_expired(resets_at, now):
        return None
    return max(0, min(100, 100 - used))


def _cache_availability(windows: UsageWindows, now: datetime) -> str | None:
    if windows.fetched_at is None:
        return None
    remainders = [
        value
        for value in (
            _valid_remaining(windows.five_hour_used, windows.five_hour_resets_at, now),
            _valid_remaining(windows.seven_day_used, windows.seven_day_resets_at, now),
            _valid_remaining(windows.fable_used, windows.fable_resets_at, now),
        )
        if value is not None
    ]
    if not remainders:
        return None
    if any(value <= 0 for value in remainders):
        return "capped"
    if any(value <= _NEAR_CAP_REMAINING for value in remainders):
        return "near cap"
    return "available"


def _observation_availability(capacity: CapacityFact, now: datetime) -> str | None:
    if capacity.observed_at is None:
        return None
    if (
        capacity.state == "capped"
        and capacity.resets_at is not None
        and _parse(capacity.resets_at) > now
    ):
        return "capped"
    return "available"


def _availability(capacity: CapacityFact, now: datetime) -> str:
    windows = capacity.windows
    cache_word = _cache_availability(windows, now)
    obs_word = _observation_availability(capacity, now)
    cache_at = _parse(windows.fetched_at) if windows.fetched_at else None
    obs_at = _parse(capacity.observed_at) if capacity.observed_at else None

    if cache_word is None and obs_word is None:
        return "unknown"
    if cache_word is not None and obs_word is not None and cache_word != obs_word:
        # Disagreement: the fresher source wins.
        word = cache_word if _later(cache_at, obs_at) else obs_word
    else:
        word = cache_word if cache_word is not None else obs_word

    freshest = _later_of(cache_at, obs_at)
    if word == "available" and (now - freshest) > _CACHE_STALE_BOUND:
        return f"stale {_age(freshest.isoformat(), now)}"
    return word


def _later(left: datetime | None, right: datetime | None) -> bool:
    """Return True when `left` is at least as fresh as `right`."""

    if left is None:
        return False
    if right is None:
        return True
    return left >= right


def _later_of(left: datetime | None, right: datetime | None) -> datetime:
    candidates = [value for value in (left, right) if value is not None]
    return max(candidates)


def _capacity_reset_text(capacity: CapacityFact, now: datetime) -> str:
    resets_at = capacity.windows.fable_resets_at or capacity.windows.seven_day_resets_at
    if not resets_at:
        return ""
    resets = _parse(resets_at)
    if resets <= now:
        return "reset passed"
    delta_seconds = (resets - now).total_seconds()
    if delta_seconds > 24 * 3600:
        return f"resets {resets.strftime('%H:%M')}Z"
    return f"resets in {_duration(delta_seconds)}"


def _capacity_age_source(capacity: CapacityFact, now: datetime) -> str:
    windows = capacity.windows
    cache_at = _parse(windows.fetched_at) if windows.fetched_at else None
    obs_at = _parse(capacity.observed_at) if capacity.observed_at else None
    if cache_at is None and obs_at is None:
        return ""
    if cache_at is not None and _later(cache_at, obs_at):
        return f"(cache {_age(windows.fetched_at, now)})"
    source_label = _CAPACITY_SOURCE_LABELS.get(capacity.source, capacity.source or "?")
    return f"({source_label} {_age(capacity.observed_at, now)})"


def _fit_line_with_drops(
    width: int,
    mandatory: list[str],
    optional: dict[str, str],
    order: list[str],
    drop_priority: list[str],
) -> str:
    """Join `mandatory` plus the present `optional` fields (in `order`,
    double-space separated). If it overflows `width`, drop optional
    fields one at a time in `drop_priority` order until it fits or none
    are left. Any further overflow is left for `_fit_content` at
    finalize time, which ellipsizes only the trailing (last remaining)
    field at a word boundary — never mid-token."""

    present = dict(optional)

    def build() -> str:
        segments = list(mandatory) + [present[key] for key in order if key in present]
        return "  ".join(segments)

    text = build()
    index = 0
    while visible_length(text) > width and index < len(drop_priority):
        present.pop(drop_priority[index], None)
        index += 1
        text = build()
    return text


def _capacity_line_wide(
    alias: str,
    capacity: CapacityFact,
    lease: ProfileLeaseFact | None,
    now: datetime,
    width: int,
) -> str:
    availability = _availability(capacity, now)
    reset_text = _capacity_reset_text(capacity, now)
    lease_text = f"lease {lease.project_key}" if lease is not None else "no lease"
    source_text = _capacity_age_source(capacity, now)

    include_five_hour = True
    include_freshness = True

    def build(
        include_five_hour: bool, include_freshness: bool, present: dict[str, str]
    ) -> str:
        numbers = _numbers_text(
            capacity.windows, now,
            include_five_hour=include_five_hour, include_freshness=include_freshness,
        )
        segments = [alias, availability, numbers]
        for key in ("reset", "lease", "source"):
            if key in present:
                segments.append(present[key])
        return "  ".join(segments)

    present = {}
    if reset_text:
        present["reset"] = reset_text
    present["lease"] = lease_text
    if source_text:
        present["source"] = source_text

    text = build(include_five_hour, include_freshness, present)
    drop_order = ["lease", "five_hour", "reset", "source"]
    index = 0
    while visible_length(text) > width and index < len(drop_order):
        key = drop_order[index]
        index += 1
        if key == "five_hour":
            include_five_hour = False
            include_freshness = False
        else:
            present.pop(key, None)
        text = build(include_five_hour, include_freshness, present)
    return text


def _capacity_line1_narrow(
    alias: str,
    capacity: CapacityFact,
    lease: ProfileLeaseFact | None,
    now: datetime,
    width: int,
) -> str:
    availability = _availability(capacity, now)
    reset_text = _capacity_reset_text(capacity, now)
    lease_text = f"lease {lease.project_key}" if lease is not None else "no lease"
    source_text = _capacity_age_source(capacity, now)
    optional = {}
    if reset_text:
        optional["reset"] = reset_text
    optional["lease"] = lease_text
    if source_text:
        optional["source"] = source_text
    return _fit_line_with_drops(
        width,
        mandatory=[alias, availability],
        optional=optional,
        order=["reset", "lease", "source"],
        drop_priority=["lease", "reset", "source"],
    )


def _capacity_line2_narrow(
    alias: str, windows: UsageWindows, now: datetime, width: int
) -> str:
    indent = " " * (len(alias) + 2)
    modes = ((True, True), (False, True), (False, False))
    for include_five_hour, include_freshness in modes:
        candidate = indent + _numbers_text(
            windows, now,
            include_five_hour=include_five_hour, include_freshness=include_freshness,
        )
        if visible_length(candidate) <= width:
            return candidate
    return indent + _numbers_text(
        windows, now, include_five_hour=False, include_freshness=False
    )


def _capacity_lines(
    alias: str,
    capacity: CapacityFact | None,
    lease: ProfileLeaseFact | None,
    now: datetime,
    width: int,
) -> list[str]:
    if capacity is None:
        capacity = CapacityFact(
            profile_alias=alias, state=None, source=None, observed_at=None,
            resets_at=None, detail=None,
        )
    if width < _NARROW_WIDTH:
        return [
            _capacity_line1_narrow(alias, capacity, lease, now, width),
            _capacity_line2_narrow(alias, capacity.windows, now, width),
        ]
    return [_capacity_line_wide(alias, capacity, lease, now, width)]


def _codex_capacity_row(codex: CodexFact, now: datetime) -> str:
    if not codex.available:
        seconds = (
            _age_seconds(codex.unavailable_since, now)
            if codex.unavailable_since
            else None
        )
        if seconds is not None and seconds < 60:
            return "codex unavailable"
        return f"codex unavailable {_age(codex.unavailable_since, now)}"
    if codex.primary_used_percent is None or codex.secondary_used_percent is None:
        return "codex unknown"
    primary_left = max(0, min(100, 100 - codex.primary_used_percent))
    secondary_left = max(0, min(100, 100 - codex.secondary_used_percent))
    reached = " limit reached" if codex.reached else ""
    return f"codex {primary_left}%/{secondary_left}% left{reached}"


def _detail_row(usage: ProfileUsage, capacity: CapacityFact | None) -> str:
    parts = [
        usage.profile_alias,
        f"fable {_compact_tokens(usage.fable_tokens)}",
        f"overall {_compact_tokens(usage.overall_tokens)}",
    ]
    windows = capacity.windows if capacity is not None else None
    if windows is not None:
        parts.append(f"5h {_pct(windows.five_hour_used)}")
        parts.append(f"7d {_pct(windows.seven_day_used)}")
        parts.append(f"fable_win {_pct(windows.fable_used)}")
        if windows.fable_severity:
            parts.append(windows.fable_severity)
        if windows.fable_active is not None:
            parts.append("active" if windows.fable_active else "inactive")
    if capacity is not None and capacity.detail:
        parts.append(f"detail={capacity.detail}")
    return " ".join(parts)


def _pct(value: int | None) -> str:
    return "?" if value is None else f"{value}%"


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------


def _meter(used_fraction: float, slots: int = 8) -> str:
    used_fraction = max(0.0, min(1.0, used_fraction))
    filled = round(used_fraction * slots)
    return "[" + "#" * filled + " " * (slots - filled) + "]"


def _system_mem_line(resource: ResourceFact, now: datetime) -> str:
    used_fraction = (
        1 - (resource.available_memory_bytes / resource.total_memory_bytes)
        if resource.total_memory_bytes
        else 0.0
    )
    available_gib = resource.available_memory_bytes / (1024**3)
    total_gib = resource.total_memory_bytes / (1024**3)
    return (
        f"{resource.pressure} {_meter(used_fraction)} "
        f"mem {available_gib:.1f}/{total_gib:.1f}G"
    )


def _system_disk_line(resource: ResourceFact, workers: WorkerFact) -> str:
    disk_text = (
        f"disk {_compact_bytes(resource.min_disk_free_bytes)} free"
        if resource.min_disk_free_bytes is not None
        else "disk unknown"
    )
    return (
        f"{disk_text}  load {resource.load_one:.1f}/{resource.logical_cpus}  "
        f"workers {workers.active_total}"
    )


def _system_content_lines(
    resource: ResourceFact | None, workers: WorkerFact, now: datetime, width: int
) -> list[str]:
    if resource is None:
        return ["no resource sample"]
    if width < _NARROW_WIDTH:
        # Narrow: swap and sample age drop out entirely to keep both
        # lines scan-friendly rather than truncating them.
        return [_system_mem_line(resource, now), _system_disk_line(resource, workers)]
    disk_text = (
        f"disk {_compact_bytes(resource.min_disk_free_bytes)} free"
        if resource.min_disk_free_bytes is not None
        else "disk unknown"
    )
    return [
        f"{_system_mem_line(resource, now)} "
        f"{disk_text} "
        f"swap {_compact_bytes(resource.swap_used_bytes)} "
        f"load {resource.load_one:.1f}/{resource.logical_cpus} "
        f"workers {workers.active_total} "
        f"{_age(resource.sampled_at, now)}"
    ]


def _system_kind_text(workers: WorkerFact) -> str:
    if not workers.active_by_kind:
        return "workers: none active"
    return "workers " + " ".join(f"{kind} {n}" for kind, n in workers.active_by_kind)


def _system_block(
    resource: ResourceFact | None,
    workers: WorkerFact,
    now: datetime,
    width: int,
    detail: bool,
) -> list[str]:
    lines = [
        _divider_text("System", width),
        *_system_content_lines(resource, workers, now, width),
    ]
    if detail:
        lines.append(_system_kind_text(workers))
    return lines


# ---------------------------------------------------------------------------
# Attention
# ---------------------------------------------------------------------------


def _attention_text(snapshot: DashboardSnapshot, now: datetime) -> str:
    if snapshot.resource is not None and snapshot.resource.pressure == "red":
        return "system pressure red"

    capacity_by_alias = {fact.profile_alias: fact for fact in snapshot.capacity}
    for lease in snapshot.leases:
        capacity = capacity_by_alias.get(lease.profile_alias)
        word = "unknown" if capacity is None else _availability(capacity, now)
        if word in ("unknown", "capped"):
            return (
                f"{lease.profile_alias} capacity {word} "
                f"(leading {lease.project_key})"
            )

    if snapshot.attention_control is not None:
        issue = next(
            (
                task.issue_id
                for task in snapshot.tasks
                if task.project_key == snapshot.attention_control.project_key
            ),
            None,
        )
        target = issue or snapshot.attention_control.project_key
        return f"confirm channel dialog for {target}"

    corrections = next(
        (
            task
            for task in snapshot.tasks
            if task.review_state == "corrections_required"
        ),
        None,
    )
    if corrections is not None:
        if not corrections.pr_number:
            # No pull request exists yet (pr_number 0/None): a pre-PR
            # correction, never a reference to a nonexistent PR#0.
            return f"corrections requested before PR ({corrections.issue_id})"
        return (
            f"corrections requested on PR#{corrections.pr_number} "
            f"({corrections.issue_id})"
        )

    # Paused is a deliberate hold, not a problem — only Blocked (failed
    # /blocked/stalled) issues are Attention-worthy (INFRA-209 follow-up
    # point 4).
    blocked = next(
        (task for task in snapshot.tasks if task.operator_state == "Blocked"), None
    )
    if blocked is not None:
        return f"blocked: {blocked.issue_id}"

    if snapshot.transitions:
        latest = max(snapshot.transitions, key=lambda t: t.occurred_at)
        return f"{latest.project_key}: {_age(latest.occurred_at, now)} {latest.phrase}"

    return "nothing needs attention"


# ---------------------------------------------------------------------------
# Shared formatting helpers
# ---------------------------------------------------------------------------


def _compact_tokens(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}k"
    return str(value)


def _compact_bytes(value: int) -> str:
    gib = value / (1024**3)
    if gib >= 1:
        return f"{gib:.1f}G"
    mib = value / (1024**2)
    if mib >= 1:
        return f"{mib:.0f}M"
    kib = value / 1024
    return f"{kib:.0f}K"


def _parse(iso: str) -> datetime:
    value = datetime.fromisoformat(iso)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value


def _age_seconds(iso: str, now: datetime) -> float:
    return (now - _parse(iso)).total_seconds()


def _age(iso: str | None, now: datetime) -> str:
    if iso is None:
        return "—"
    try:
        seconds = max(0, int(_age_seconds(iso, now)))
    except ValueError:
        return "—"
    return _duration(float(seconds))


def _duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    return f"{days}d"
