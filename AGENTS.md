# Taskmaster
PostgreSQL-backed task queue. Owns no business logic, only supplies tasks to workers via pull model. Written in Java/Spring. REST API serves both worker polling and client task submission. Background reapers handle task deadlines, worker heartbeat timeouts and retention.

## Architecture
See README.md for more context as needed.

## Guidelines

### Testing
- Unit tests for isolated logic; integration tests for complete end-to-end flows
- Integration tests should be high-level, short, and readable — add helpers/abstractions as needed
- Make the tests as short-running as possible; for time-based tests always consider the impact on runtime
- Run unit tests first when validating a change `mvn test -DexcludedGroups=integration`
- If tests fail, diagnose whether it's a direct consequence of the change or a regression
- For regressions, explain and stop for input before proceeding
- Always ask before running commands that touch external systems
- Integration tests are written in Java and require containers from docker-compose running


### Code Quality
- Adhere to DRY principle for complex logic but do not overcomplicate
- Apply design patterns where appropriate but do not over-engineer the simplest cases
- Self-descriptive names for functions and types; comprehensive variable names except for trivial cases
- Comments explain WHY, not HOW; only for non-obvious logic
- Document only major stable components with external dependants
- Never break existing tests unless doing large business logic changes
- Always ask before deleting files or large refactors

### Logging & Error Handling
- Include as much context as possible in log lines
- Errors logged exactly once at the handling point; if only propagating, do not log

### Workflow
- If a task involved a repeatable multi-step workflow (3+ steps, likely to recur), suggest turning it into a Skill
- If a change introduces new patterns not yet documented, suggest updating CLAUDE.md or the relevant spec
- If a change will break existing API or dependants, highlight this in CAPS in the plan and output
- If a change is affecting business logic and not just bug/edge case fix -- check if README requires update
- When implementing an MD spec and changes are requested make sure it's reflected back into spec file
- When implementing an MD spec change the status of the spec