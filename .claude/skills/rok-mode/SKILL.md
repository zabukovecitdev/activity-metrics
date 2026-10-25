---
name: rok-mode
description: Rok's working conventions for git, verification, build gates, code structure, and explanations. Use for Rok, /rok-mode, or requests to work in Rok's style.
disable-model-invocation: true
---

# Rok mode

## Ask before build

- A question gets an answer, not a build. "Just an answer" or "don't implement yet" means read-only.
- For a design question, give a recommendation first. Compare with how similar real projects do it.
- Build only after an explicit go-ahead ("yes", "do it", "go ahead"). A go-ahead covers the agreed scope only.
- One change per message. Do not add extras. Do not widen scope.
- Remove fields, metrics, and config the user calls redundant. Keep new schemas and dashboards minimal.

## Code structure

- Read the existing code first. Match its style and naming. Pick the best existing file as the model.
- One class, enum, settings group, consumer, producer, or repository per file.
- Read configuration through a pydantic settings class. Never scatter `os.environ.get`.
- Match real-world naming for the pattern. Do not call a thing a repository if it is not one. Base classes carry a `Base` suffix.
- Debug a pasted error or log from the evidence. Use the systematic-debugging skill.

## Verify

- Done means a live end-to-end run on real data. Run the pipeline and check that the results match. Check that the database receives every row.
- Read logs and real state. Do not trust a claim, including your own. Confirm that a change is applied.
- Add quality tools when asked, such as pylint or mutmut. Run them and fix what they find.
- Keep benchmark and experiment code in its own directory. Gitignore it.

## Git

- Split work into separate branches and MRs by concern. Open a branch for each concern, such as signal handling, checkpoint config, and metrics.
- Split commits into logical units. Commit before a refactor starts.
- Start new work from an updated master.
- Commit, push, PR, merge, and retarget when told. Use the mr-description-generator skill for the MR text.
- Keep one clean master. When asked, delete merged branches, stale MRs, and worktrees. Avoid worktrees unless needed.

## Explain

- Explain for interviews. Say why the design is this way. Name the details an interviewer may probe, and the trade-offs.
- Use plain words. Explain a new term the first time it appears.
