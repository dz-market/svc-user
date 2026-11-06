<div align="center">

# User Service

User profiles and personal data.

[![CI](https://github.com/dz-market/svc-user/actions/workflows/ci.yml/badge.svg)](https://github.com/dz-market/svc-user/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dz-market/svc-user/main)](https://app.codecov.io/gh/dz-market/svc-user)
[![Go version](https://img.shields.io/github/go-mod/go-version/dz-market/svc-user)](go.mod)
[![Release](https://img.shields.io/github/v/release/dz-market/svc-user)](https://github.com/dz-market/svc-user/releases)
[![License](https://img.shields.io/github/license/dz-market/svc-user)](LICENSE)

</div>

## Run

Required Docker Compose and Task. Clone [infra](https://github.com/dz-market/infra) and [svc-auth](https://github.com/dz-market/svc-auth) alongside this repository.

```bash
task --dir ../infra up -- auth user
```

## Development

Requires Go, Task and Goose. With infrastructure running, stop the containerized service and run it locally:

```bash
docker compose -f ../infra/local/compose.yml --profile user stop svc-user
cp .env.example .env # adjust connection settings
task migrate:up
go run ./cmd/service
```

The auth service must be running: its public key is fetched at startup to verify access tokens.