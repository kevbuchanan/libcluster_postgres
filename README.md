# Cluster.Strategy.Postgres

Postgres [LISTEN/NOTIFY](https://www.postgresql.org/docs/9.1/static/sql-notify.html)
strategy for [libcluster](https://github.com/bitwalker/libcluster).

## Installation

TODO

## Usage

```elixir
config :libcluster,
  topologies: [
    postgres_example: [
      strategy: Cluster.Strategy.Postgres,
      config: [
        hostname: "yourdbhost",
        database: "example",
        username: "username",
        password: "password",
      ]
```
