# Verification & Evidence

| Area | Evidence | Reproduction |
|---|---|---|
| Shared event contracts | shared/tests/test_event_contracts.py | pytest shared/tests/ -v |
| Service behavior | .github/workflows/ci.yml | GitHub Actions matrix |
| Container buildability | .github/workflows/ci.yml | Docker Buildx jobs |
| Static security analysis | .github/workflows/codeql.yml | CodeQL workflow |
| Workflow supply-chain posture | .github/workflows/scorecard.yml | OpenSSF Scorecard |

## Publication rule

Protocol or reliability numbers must include workload, environment, run count, timestamp, and commit. Capabilities are claims about implemented behavior, not measured production performance.