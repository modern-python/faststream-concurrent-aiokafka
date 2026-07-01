default: install lint build test

down:
    docker compose down --remove-orphans

test *args: build down && down
    docker compose run application uv run --no-sync pytest {{ args }}

test-branch:
    @just test --cov-branch

build:
    docker compose build application

install:
    uv lock --upgrade
    uv sync --all-extras --frozen --group lint

lint:
    uv run eof-fixer .
    uv run ruff format
    uv run ruff check --fix
    uv run ty check

lint-ci:
    uv run eof-fixer . --check
    uv run ruff format --check
    uv run ruff check --no-fix
    uv run ty check
    uv run python planning/index.py --check

# Print the planning change index (flat, newest-first) to stdout.
index:
    uv run python planning/index.py

# Validate planning bundles + decisions; CI runs this.
check-planning:
    uv run python planning/index.py --check

# Auth via PyPI Trusted Publishing (OIDC); uv publish auto-detects the CI id-token.
publish:
    rm -rf dist
    uv version $GITHUB_REF_NAME
    uv build
    uv publish
