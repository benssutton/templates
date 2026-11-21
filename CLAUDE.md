# CLAUDE.md

Read [README.md](README.md) first: it describes the intent, architectural decisions and current status of this repo.

## Python environment
Use `C:\Users\Alexander\miniconda3\envs\p315\python.exe` (Python 3.15) for anything Python at the repo root, including the pipeline. Example (Git Bash):
`/c/Users/Alexander/miniconda3/envs/p315/python.exe run_CICD.py list`

## Running the pipeline
Run from the repo root. See the "Pipeline" section of README.md.
- `python run_CICD.py changed fast`  — format + lint for changed services (seconds)
- `python run_CICD.py changed ci`    — everything GitHub runs, for changed services
- `python run_CICD.py <python|java|rust> <step...>` — one service
- `python run_CICD.py security`      — repo-wide scanners
Each service also has its own CLAUDE.md with service-specific detail.

## Comments
Each service's code must read on its own: no comment refers to another template
(tests/cicd/test_no_cross_template_refs.py enforces this). Repeat an explanation
in each project rather than pointing at another. Comment why, briefly, and only
where the code is not self-explanatory.
