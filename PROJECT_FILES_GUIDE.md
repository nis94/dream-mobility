# Project Files Guide — Junior Level Explanations

A friendly walkthrough of the key config/tooling files in this repo: what they are, why they exist, and how they fit together.

**Table of contents**
1. [Makefile](#1-makefile--command-shortcuts)
2. [docker-compose.yml](#2-docker-composeyml--local-infrastructure)
3. [go.mod](#3-gomod--gos-project-manifest)
4. [.golangci.yml](#4-golangciyml--go-linter-configuration)
5. [pyproject.toml](#5-pyprojecttoml--pythons-modern-project-manifest)
6. [.github/workflows/ci.yml](#6-githubworkflowsciyml--github-actions-pipeline)
7. [How they all fit together](#how-they-all-fit-together)

---

## 1. `Makefile` — command shortcuts

A **Makefile** is a plain text file that defines **shortcuts for long shell commands**. It's read by a program called `make` (built into macOS/Linux).

Think of it as a "command cheat sheet that runs itself."

### Why we need it

Without a Makefile, you'd have to remember and type things like:

```bash
docker compose -f deploy/docker-compose.yml -p dream-mobility up -d --wait
go test -race -count=1 -tags=integration -timeout=10m ./...
cd tools/generator && uv run python gen.py
```

With the Makefile, those become:

```bash
make up
make test-integration
make generator-run
```

Much easier to remember, document, and share with teammates. Everyone runs the same command, so there's no "works on my machine" because one dev typed `-count=1` and another typed `-count=2`.

### How it works — anatomy of a target

From `Makefile`:

```makefile
.PHONY: test
test: ## Run unit tests
    go test -race -count=1 ./...
```

Breakdown:
- `test:` — the **target name** (what you type after `make`)
- `## Run unit tests` — a comment used by the `help` target to auto-generate docs (try `make help`)
- `go test -race -count=1 ./...` — the actual shell command that runs (must be indented with a **TAB**, not spaces — classic Makefile gotcha)
- `.PHONY: test` — tells `make` "this isn't a file, it's just a command name" (without this, if you had a file called `test`, make would get confused)

### Variables

Top of the Makefile:

```makefile
COMPOSE := docker compose -f deploy/docker-compose.yml -p dream-mobility
```

Then later `$(COMPOSE) up -d --wait` expands to the full command. DRY principle — change it in one place.

### Key commands to try

```bash
make help              # auto-generated list of all commands
make up                # start all local infra
make ps                # see what's running
make logs              # tail logs from all services
make down              # stop everything (keep data)
make down-v            # stop AND wipe data (the "nuke" option)
make test              # run Go unit tests
make kafka-topics      # peek at Kafka topics inside the container
```

---

## 2. `docker-compose.yml` — local infrastructure

Located at `deploy/docker-compose.yml`.

### What it is

**docker-compose** is a tool for running **many containers together** with one command. The `docker-compose.yml` file describes:
- which services to run (Kafka, Postgres, ClickHouse, MinIO, etc.)
- how they connect to each other (network, ports)
- where data is persisted (volumes)
- health checks (how do we know a service is ready?)

### Is it different from a Makefile? YES — totally different layer.

They solve different problems and work **together**, not instead of each other.

|                 | **Makefile**           | **Dockerfile**                      | **docker-compose.yml**                  |
| --------------- | ---------------------- | ----------------------------------- | --------------------------------------- |
| What it is      | Command shortcuts      | Recipe for building ONE container   | Recipe for running MANY containers      |
| Run by          | `make`                 | `docker build`                      | `docker compose up`                     |
| Level           | Developer workflow     | Container image                     | Container orchestration                 |
| Example problem | "how do I start everything?" | "how do I package my app?"   | "how do all my services connect?"       |

### How the layers stack

```
You type:         make up
                     │
Makefile runs:    docker compose -f deploy/docker-compose.yml up -d --wait
                     │
docker-compose    reads deploy/docker-compose.yml, starts all ~8 services
starts:              │
Docker pulls:     confluentinc/cp-kafka:7.6.1, postgres:16-alpine, etc.
                     │
Containers run:   Kafka, Postgres, ClickHouse, MinIO... all talking to each other
```

You could skip the Makefile and type the full `docker compose -f deploy/docker-compose.yml -p dream-mobility up -d --wait` every time. The Makefile just makes it ergonomic.

---

## 3. `go.mod` — Go's project manifest

```go
module github.com/nis94/dream-mobility

go 1.22
```

### What it is
Every Go project has a `go.mod` file. It's the Go equivalent of `package.json` (Node.js), `requirements.txt` (Python), or `pom.xml` (Java).

### What the lines mean

- **`module github.com/nis94/dream-mobility`** — the project's **unique name/identity**. When another Go project imports your code, this is the path they use. It's conventionally a GitHub URL because `go get` uses it to download code, but it doesn't *have* to be a real URL for a private project.

- **`go 1.22`** — the minimum Go version required. If someone tries to build with Go 1.19, they get an error.

### Why we need it

1. **Dependency management** — when you add third-party packages (e.g., `go get github.com/segmentio/kafka-go`), Go records them here with exact versions. This file will grow, and a companion `go.sum` file will appear with cryptographic checksums (security — proves the downloaded code wasn't tampered with).

2. **Reproducible builds** — teammate clones the repo, runs `go build`, and gets *exactly* the versions you had.

3. **Module boundary** — tells Go "everything under this folder is one project." Without it, `go build ./...` wouldn't know where the project starts.

### Typical shape once you add dependencies

```go
module github.com/nis94/dream-mobility

go 1.22

require (
    github.com/segmentio/kafka-go v0.4.47
    github.com/jackc/pgx/v5 v5.5.0
)
```

---

## 4. `.golangci.yml` — Go linter configuration

### What is a linter?

A **linter** is a tool that reads your code and complains about problems *without running it*. Things like:
- unused variables
- ignored errors (`result, _ := doThing()` — you swallowed an error!)
- spelling mistakes in comments
- formatting inconsistencies

`golangci-lint` is a meta-linter: it runs ~50 linters in parallel. This file picks **which ones** to use and **how strict** they are.

### The config explained

```yaml
run:
  timeout: 5m           # kill the linter if it takes > 5 minutes
  tests: true           # also lint _test.go files (many projects skip tests — we don't)

linters:
  disable-all: true     # start from zero — don't enable the defaults
  enable:               # pick exactly these
    - errcheck          # catches ignored errors ("you forgot `if err != nil`")
    - govet             # catches suspicious constructs (mismatched Printf args, etc.)
    - staticcheck       # huge set of correctness checks (the gold standard)
    - ineffassign       # finds assignments that are never read
    - unused            # finds unused functions/vars/consts
    - gofmt             # formatting — must match `gofmt` output
    - goimports         # import grouping + unused imports
    - misspell          # typos in comments & strings
    - unconvert         # flags pointless type conversions like int(x) where x is already int
    - unparam           # flags function params that are never used

issues:
  exclude-dirs:
    - proto             # skip the proto/ folder (usually auto-generated code — not worth linting)
  exclude-rules:
    - path: _test\.go   # in test files only...
      linters:
        - unparam       # ...don't complain about unused params (test helpers often have them)
```

### Why we need it
- **Consistency** — everyone's code looks and behaves the same way.
- **Catches bugs before CI/prod** — `errcheck` alone prevents a whole class of real production incidents.
- **Code reviews focus on logic, not style** — no more "you forgot a tab" PR comments.

### How you run it

```bash
make lint          # via the Makefile → `golangci-lint run ./...`
```

It also runs automatically on every PR (see `ci.yml` below).

---

## 5. `pyproject.toml` — Python's modern project manifest

Located at `tools/generator/pyproject.toml`.

```toml
[project]
name = "dream-mobility-generator"
version = "0.1.0"
description = "Synthetic GPS-like movement event generator..."
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27.0",
]

[dependency-groups]
dev = [
    "ruff>=0.6.0",
]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "W", "UP", "SIM"]
```

### What it is

`pyproject.toml` is the **modern standard** for Python projects (replaces older `setup.py` / `requirements.txt`). It serves two purposes in one file:

1. **Project metadata + dependencies** (`[project]` section)
2. **Tool configuration** (`[tool.ruff]`, `[tool.pytest]`, `[tool.mypy]`, etc.)

### Section by section

**`[project]`** — who am I?
- `name`, `version`, `description` — self-explanatory.
- `requires-python = ">=3.11"` — won't install on Python 3.10. Guarantees modern features (like better type hints).
- `dependencies` — runtime packages. `httpx` is an HTTP client (used by the generator to POST events).

**`[dependency-groups]`** — optional extras.
- `dev = ["ruff>=0.6.0"]` — only installed when developing (linters, test runners, etc.). Production deployments don't need `ruff`, so this keeps the install lean.

**`[tool.ruff]`** — config for **Ruff**, which is a super-fast Python linter + formatter (Python's equivalent of `golangci-lint`).
- `line-length = 100` — wrap at 100 chars.
- `target-version = "py311"` — assume Python 3.11 features are available.

**`[tool.ruff.lint]`**
- `select = ["E", "F", "I", "B", "W", "UP", "SIM"]` — rule categories to enable:
  - `E`/`W` — style (PEP 8)
  - `F` — pyflakes (undefined names, unused imports)
  - `I` — isort (import ordering)
  - `B` — bugbear (likely bugs)
  - `UP` — pyupgrade (modernize old syntax)
  - `SIM` — simplify (code smell simplifications)

### How it's used in this project

From the Makefile:

```makefile
generator-install: ## Install Python generator deps via uv
    cd tools/generator && uv sync
```

`uv` is a modern Python package manager (think `npm` for Python). It reads `pyproject.toml` → installs `httpx` + `ruff` → creates a lockfile for reproducibility.

### The parallel with Go

|                 | Go                 | Python (modern)                           |
| --------------- | ------------------ | ----------------------------------------- |
| Project manifest | `go.mod`          | `pyproject.toml`                          |
| Lockfile        | `go.sum`           | `uv.lock` / `poetry.lock`                 |
| Install         | `go mod download`  | `uv sync`                                 |
| Lint config     | `.golangci.yml`    | `[tool.ruff]` in `pyproject.toml`         |

---

## 6. `.github/workflows/ci.yml` — GitHub Actions pipeline

### What is CI?

**CI = Continuous Integration.** Automated checks that run on every push/PR, on GitHub's servers (not your laptop). The goal: catch problems **before** code merges to `main`.

GitHub Actions is GitHub's built-in CI. It reads any `.yml` file in `.github/workflows/` and runs it.

### Top section — when to run

```yaml
name: CI                      # display name in the GitHub UI

on:                           # triggers
  push:
    branches: [main]          # run on every push to main
  pull_request:
    branches: [main]          # run on every PR targeting main

permissions:
  contents: read              # this workflow can only read the repo, not modify it
                              # (security best practice — least privilege)
```

### Jobs

A **job** is an independent task that runs on a fresh Ubuntu VM. This file defines **4 jobs** that run **in parallel**:

#### Job 1: `go-lint`

```yaml
steps:
  - uses: actions/checkout@v4              # git clone the repo into the VM
  - uses: actions/setup-go@v5              # install Go 1.22
    with:
      go-version: '1.22'
      cache: true                          # cache Go modules between runs (faster)
  - name: golangci-lint
    uses: golangci/golangci-lint-action@v6 # pre-made action that runs golangci-lint
    with:
      version: latest
      args: --timeout=5m
```

Runs the same linter as `make lint` — but on a clean VM. Catches "works on my machine" issues.

#### Job 2: `go-test`

```yaml
- run: go build ./...                  # make sure everything compiles
- run: go test -race -count=1 ./...    # run unit tests with race detector
```

- `-race` — detects data races in concurrent code (critical for a Kafka pipeline!)
- `-count=1` — disables Go's test cache; forces fresh run every time.

#### Job 3: `python-lint`

```yaml
- uses: astral-sh/setup-uv@v3          # install uv (the fast Python package manager)
- working-directory: tools/generator
  run: uv sync --all-extras --dev      # install httpx + ruff
- run: uv run ruff check .             # lint the Python code
```

#### Job 4: `compose-validate`

```yaml
- run: docker compose -f deploy/docker-compose.yml config -q
```

Just validates that `docker-compose.yml` is syntactically correct (doesn't actually start anything). Cheap sanity check — catches broken YAML before someone runs `make up` on their laptop.

### Key concepts to remember

- **`uses:`** — reuses a pre-made "action" from the GitHub Actions marketplace (like importing a library).
- **`run:`** — runs a raw shell command.
- **Jobs run on isolated VMs**, start empty, so every step must install what it needs. That's why you see `setup-go`, `setup-uv`, etc.
- **Jobs run in parallel by default** — if one fails, the others keep going. You see all failures at once.
- **PR shows ✅/❌** — GitHub blocks merging if any job fails (if branch protection is enabled).

### Why we need it
- **Safety net before `main`** — bad code gets caught before it reaches shared branches.
- **Consistent environment** — "works on my machine" is eliminated. If it passes CI, it works on a vanilla Linux box.
- **Documentation** — the CI file is essentially executable docs for "how do I build and test this project?"

---

## How they all fit together

```
You write Go code
    │
    ├─ go.mod defines what Go version + which deps you're allowed to use
    ├─ .golangci.yml defines what "good code" looks like for Go
    │
You write Python code (the generator)
    │
    └─ pyproject.toml defines deps + what "good code" looks like for Python
    │
You run things locally
    │
    └─ Makefile gives you short commands (make up, make test, make lint)
        └─ which often shell out to docker-compose.yml to spin up infra
    │
You commit + push / open a PR
    │
    └─ .github/workflows/ci.yml wakes up, runs ALL the checks above on a fresh VM
         ✅ merge allowed    ❌ merge blocked, fix the problem
```

The Makefile is the developer-facing layer (you type `make lint` locally); the CI file runs the *same tools* on every PR. Local Makefile and CI should always agree — if something passes locally but fails in CI, something is misconfigured.

### TL;DR table

| File                         | Purpose                                              | Who reads it                       |
| ---------------------------- | ---------------------------------------------------- | ---------------------------------- |
| `Makefile`                   | Short aliases for long commands                      | `make` (and humans)                |
| `deploy/docker-compose.yml`  | Declares the local infra stack                       | `docker compose`                   |
| `go.mod`                     | Go project identity + dependencies + Go version      | `go` toolchain                     |
| `.golangci.yml`              | Which Go linters to run and how                      | `golangci-lint`                    |
| `tools/generator/pyproject.toml` | Python project deps + tool config (ruff, etc.)   | `uv`, `ruff`                       |
| `.github/workflows/ci.yml`   | Automated checks on every push/PR                    | GitHub Actions                     |
