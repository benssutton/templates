# Local SonarQube (Community Build)

The Community edition has no monorepo support, so each service is its own Sonar project
(`templates-python`, `templates-java`, `templates-rust`), configured by the
`sonar-project.properties` in the service directory.

## Setup

1. Start Docker Desktop, then `docker compose -f sonar/compose.yml up -d`; wait for
   http://localhost:9000 (log in `admin` / `admin`, set a new password).
2. My Account -> Security -> generate a **user token**; set it for your user, e.g.
   PowerShell: `[Environment]::SetEnvironmentVariable("SONARQUBE_TOKEN", "<token>", "User")`
   (restart the terminal / VS Code afterwards). Never commit the token.
3. Analyse: `python run_CICD.py sonar <python|java|rust>`. Python runs pytest with coverage
   (`coverage.xml`), Java runs `mvnw verify` (JaCoCo, `target/site/jacoco/jacoco.xml`), Rust is
   scanned without tests (no Rust coverage is imported). Then the pinned
   `sonarsource/sonar-scanner-cli` container runs against the service's `sonar-project.properties`.
   The scanner joins SonarQube's Docker network and talks to `sonarqube:9000` directly (not
   `localhost`), so the server must be running; the step fails fast if it is not.

## Seeing the results

**Dashboard (browser):** `http://localhost:9000/dashboard?id=templates-<service>`, e.g.
http://localhost:9000/dashboard?id=templates-python.

| Tab / page | Shows |
|---|---|
| Overview | Quality gate (pass / fail), new vs overall code, coverage, duplications |
| Issues | Bugs, vulnerabilities and code smells; filter by file, severity or rule |
| Security Hotspots | Code to review by hand |
| Measures | Per-file and per-folder metrics |
| Code | Browse files with issues and coverage marked line by line |

The scanner log ends with `ANALYSIS SUCCESSFUL`: that means the upload worked, not that the
code is clean. Open the dashboard for the findings.

Findings already reviewed and rejected are listed in [false-positives.md](false-positives.md):
check it before acting on a finding and add to it when you reject one.

The gate and rule set can be changed in the web UI (Quality Gates, Quality Profiles).
