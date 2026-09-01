# INFRA-217 delegation plan — GitHub-derived merge identity, pre-persistence verdict validation

Authored by the Fable lead (session `8f880073-…`, profile `max-c`,
cell `b29691ef-…`) under the operator's post-PR-48 assignment. Base:
`feature/infra-217` at `74518c2` — origin/main, byte-identical to the
ACTIVE runtime. Issue A of the operator's two-issue split:
[INFRA-217](https://linear.app/jo-solutions/issue/INFRA-217) (related:
INFRA-198, INFRA-218). Issue B (INFRA-218: candidate supersession +
Linear-as-projection) is deliberately NOT worked here — one PR at a
time.

## Scope (exactly two behaviors)

1. **GitHub-derived PR and merge identity.** `pr_number` is removed as
   reviewer-supplied authority: the reviewer's verdict document no
   longer carries it anywhere. The exact pull request is discovered
   from GitHub by repository, head branch, and reviewed head SHA —
   including a PR that is already merged — and merge identity derives
   from that discovery.
2. **Verdict validation before persistence.** Every invalid
   submission is rejected before anything durable: no terminal row,
   no conflicting row, so a corrected retry for the same wake always
   starts clean.

Invariants preserved: strict manifest-hash and candidate-SHA
validation, exact candidate identity, exactly-once submission CAS,
non-settling observation. No new protocol, provenance, sandbox, or
retry machinery.

## Lead-verified defects (base `74518c2`)

- `merger_turns.submit_review` builds the expected `VerdictBinding`
  with `pr_number=self._document_pr_number(verdict_json)` — the
  expected value is read out of the document under validation, so the
  pr_number "binding" is vacuous: the reviewer is the authority for
  repository bookkeeping the repo itself should assert.
- Settlement resolves the PR via `_pull_number`, which accepts only
  the candidate at the head of the SOLE OPEN pull request; a PR merged
  before settlement (a permitted direct Sol merge, or any external
  merge) makes the resolution fail and the wake is completed
  terminally with kind "rejected" — a correct verdict wedges instead
  of converging onto the existing external-merge reconciliation.
- An invalid-but-parseable document (vacuous pr_number match) can
  reach persistence and settlement, leaving terminal rows a corrected
  retry then trips over.

## Packets

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| G1 | GitHub PR discovery: `discover_pull_request(repository, *, branch, head_sha)` on the existing GitHub client — searches open AND closed/merged pulls, returns the exactly-one PR whose head ref is `branch` and head SHA is `head_sha` (typed result: number, open/merged/closed state, merged flag, merge SHA when merged); no match returns None; more than one exact match raises the existing ambiguity error, fail closed. Read-only; no merge-driver changes. | `src/hermes_orchestrator/github.py`, `tests/test_github.py` | Sonnet | 1 |
| G2 | Verdict surface: `verdicts.py` drops `pr_number` from the envelope and packet schemas (strict schemas reject the retired field); `submit_review` derives the PR identity via G1's discovery BEFORE persistence — approved requires a discovered PR at the exact reviewed head (open or merged), corrections_required tolerates none — and every rejection path (schema, binding, manifest, discovery) raises `SubmissionRejected` with no durable write; settlement resolves the PR from the same discovery instead of `_pull_number`'s sole-open-PR rule, so an already-merged exact-head PR flows into the EXISTING external-merge reconciliation instead of terminal rejection; `reviews.py` rows keep their `pr_number` column, now always carrying the derived number; `prompts/codex-merger.md` verdict-schema text updated to the new document shape. | `src/hermes_orchestrator/verdicts.py`, `src/hermes_orchestrator/merger_turns.py`, `src/hermes_orchestrator/reviews.py`, `prompts/codex-merger.md`, `tests/test_verdicts.py`, `tests/test_merger_turns.py` | Sonnet | 1 |

Interface contract pinned for parallel execution (G2 codes against
it; G1 implements it exactly):

```python
@dataclass(frozen=True, slots=True)
class DiscoveredPull:
    number: int
    state: str            # "open" | "closed"
    merged: bool
    head_sha: str
    merge_sha: str | None # set iff merged

# on the existing GitHub client class in github.py
def discover_pull_request(
    self, repository: str, *, branch: str, head_sha: str
) -> DiscoveredPull | None: ...
```

Lead-owned: this plan; diff-scope verification; integration; ONE
full-suite gate at the final head; push; candidate emission to Sol.
Both packets wave 1 (files disjoint; the interface is pinned above).
`tests/integration/test_fable_ready_acceptance.py` and other shared
fixtures are lead-integrated if the schema change ripples into them
(mechanical fixture updates only, recorded at integration).

## Out of scope

INFRA-218 entirely (supersession, Linear projection); any change to
merge execution, CI-window, reviewer-fix, or channel machinery beyond
the PR-resolution call; compatibility shims for the old document
shape (the strict schema simply rejects it, and the prompt is the
single source Sol reads).
