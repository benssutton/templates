# Templates

This repo is intended to serve as a template for services in Python, Rust, Java and Typscript that serve as solid foundations for development with agentic AI.  The templates purposefully do not contain business logic, but do contain wiring to common technologies, implement fully automated tests with covage > 90% and aim to demonstrate simple and transparent patterns.  The objectives include:

1. Provide proven, re-useable boilerplate that covers common technologies.  There is little value spending time or tokens re-creating boilerplate for new applications.  
2. Demonstrates best in class design for testability.  Faithful and comprehensive test coverage is a critical guardrail when using AI.
3. Demonstrate enterprise-level robustness and stability through self-recovery and exposure of observability data points.
4. Serve as a reference for teams (and AI agents) to converge on.  Recognisable patterns facilitate understanding.
5. Separate concerns between modules.  Decoupling infrastructure from domain logic, and further decoupling sub-domains delineates and improves understanding of scope.
6. Use idomatic code wherever possible; and transparent reasoning where not.
7. Through all of the above assist code reviewers pattern-match against what good looks like.
8. Serve as a light-weight educational/debugging tool that developers can easily step through.


# Architectural Decisions

Testing
1. Testing should use real services/dependenices wherever possible.  Avoid mocks and patches.
2. Testing should exercise real endpoints (HTTP, websockets)
3. Tests should be async and run in parallel where possible to shorten the test cycle.
4. Tests should include performance benchmarks
5. Tests should be runnable locally as well as in the build pipeline.  Results of tests should be used as control gates in the CI/CD process.

Languages
1. Compiler support should be leveraged wherever possible.  Pylance and Pyright should be used to support applications in Python

APIs
1. All services should be accessed through REST endpoints: application management, configuration, observability and functional services should all be accessible via REST.
2. MCPs are optional.  MCP Tools should simply mirror REST endpoints.  

Resilience
1. Critical dependencies should be eagerly loaded and smoke-tested on application start (Fail Fast).  Disconnects should be re-tried with exponential back-off.  After a specified number of re-try failures, the application should shut down.

Observability
1. The application should expose endpoints that support optimal monitoring and observability


# Assumed Use Cases & Technologies

## Languages
- Python: exceptionally, where access to analytic, statistical and ML libraries are the key priority
- Rust: exceptionally, for high-CPU or concurrent workloads where use of parallelism and SIMD vectors are able to make a material difference to processing time.
- Typescript: for UIs
- Java: everything else.  The default language.


# Current Progress

The Python Webservice under python-webservice-template is ready for use.

The Java Webservice under java-webservice-template is ready for use — a Micronaut
(Java 25) mirror of the Python template with the same endpoints and feature set.

Other templates that might show in this repo are still works-in-progress.