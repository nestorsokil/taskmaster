## Guidelines

### Testing
- Unit tests for isolated logic; integration tests for complete end-to-end flows
- Integration tests should be high-level, short, and readable — add helpers/abstractions as needed
- Run unit tests first when validating a change
- If tests fail, diagnose whether it's a direct consequence of the change or a regression. For regressions, explain and stop for input before proceeding
- Always ask before running commands that touch external systems

### Code Quality
- Self-descriptive names for functions and types; comprehensive variable names except for trivial cases
- Comments explain WHY, not HOW; only for non-obvious logic
- Document only major stable components with external dependants
- Never break existing tests; always ask before deleting files or large refactors

### Logging & Error Handling
- Include as much context as possible in log lines
- Errors logged exactly once at the handling point; if only propagating, do not log

### Workflow
- If a task involved a repeatable multi-step workflow (3+ steps, likely to recur), suggest turning it into a Skill
- If a change introduces new patterns not yet documented, suggest updating CLAUDE.md or the relevant spec
- If a change will break existing API or dependants, highlight this in CAPS in the plan and output