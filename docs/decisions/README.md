# Decision records (ADRs)

Short, dated records of **why** a non-obvious decision was made — especially architecture,
deployment, and security calls whose rationale would otherwise be lost and have to be
reverse-engineered from the code later.

An ADR captures: the **context**, the **decision**, the **consequences / accepted risk**, and
— when the decision is only valid under current conditions — an explicit **revisit trigger**.
It is *retrospective* (what we decided and why), distinct from any `plans/` dir which is
*prospective* (what we're going to build).

Numbered sequentially (`0001-…`). Status is one of `Accepted`, `Superseded by NNNN`, or
`Proposed`. Add a new record rather than rewriting an old one; supersede when a decision changes.
