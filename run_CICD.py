"""Run the CI/CD pipeline, identically locally and in GitHub Actions.

    python run_CICD.py list
    python run_CICD.py changed [--base origin/main] [--format text|json]
    python run_CICD.py <python|java|rust|all> [step|fast|check|ci ...] [--fail-fast]
    python run_CICD.py changed <step...>          # steps, for changed services only
    python run_CICD.py security [trivy|osv|gitleaks|semgrep ...]
    python run_CICD.py sonar <python|java|rust>   # needs local SonarQube (sonar/README.md)
"""

from __future__ import annotations

import argparse
import json
import sys

from cicd import changed as changed_mod
from cicd import runner, services
from cicd.common import StepFailed
from cicd.steps import security, sonar


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("target", help="list | changed | security | sonar | all | a service name")
    p.add_argument("steps", nargs="*")
    p.add_argument("--base", default="origin/main")
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--fail-fast", action="store_true")
    a = p.parse_args(argv)

    if a.target == "list":
        for s in services.SERVICES:
            print(f"{s.name:8} {s.path:30} {'ready' if s.ready else 'wip'}")
        return 0

    if a.target == "security":
        return 0 if security.run(a.steps, a.fail_fast) else 1

    if a.target == "sonar":
        if len(a.steps) != 1:
            p.error("usage: run_CICD.py sonar <service>")
        try:
            sonar.run(services.get(a.steps[0]))
        except (StepFailed, KeyError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        return 0

    if a.target == "changed":
        names = changed_mod.affected(changed_mod.changed_files(a.base))
        if not a.steps:
            print(json.dumps(names) if a.format == "json" else "\n".join(names))
            return 0
        targets = [services.get(n) for n in names]
    elif a.target == "all":
        targets = list(services.SERVICES)
    else:
        try:
            targets = [services.get(a.target)]
        except KeyError as exc:
            print(exc, file=sys.stderr)
            return 2

    ok = True
    for svc in targets:
        ok &= runner.run_steps(svc, a.steps, a.fail_fast)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
