"""Pinned scanner images: local and CI use exactly these."""

TRIVY = "aquasec/trivy:0.76.2"
OSV = "ghcr.io/google/osv-scanner:v2.7.0"
GITLEAKS = "zricethezav/gitleaks:v8.30.1"
SEMGREP = "semgrep/semgrep:1.184.0"
SONAR_SCANNER = "sonarsource/sonar-scanner-cli:11"

# Resolves the Java service's dependency tree into a CycloneDX SBOM for OSV.
MAVEN = "maven:3.9.16-eclipse-temurin-25"
CYCLONEDX_MAVEN_PLUGIN = "org.cyclonedx:cyclonedx-maven-plugin:2.9.3"
