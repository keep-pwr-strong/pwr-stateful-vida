# PWR Stateful VIDA

A multi-language reference implementation for a stateful blockchain node, focused on synchronizing and validating VIDA transactions using a Merkle tree-backed database. The project demonstrates equivalent logic in Python, Go, Java, and Rust.

## Features

- **Account balances** and **block root hashes** stored in a persistent Merkle tree (RocksDB/BoltDB/BBolt).
- REST API endpoint `/rootHash` to query the Merkle root for a given block.
- Synchronization logic for VIDA transactions.
- Modular, idiomatic code in each language.

## Project Structure

```
python/
  src/
    main.py              # Main application entry point
    api/get.py           # Flask API: /rootHash endpoint
    database_service.py  # Merkle tree-backed database logic

go/
  main.go                # Main application entry point
  api/get.go             # Gin API: /rootHash endpoint
  database/main.go       # Merkle tree-backed database logic

java/
  src/main/java/
    main/Main.java       # Main application entry point
    api/GET.java         # Spark API: /rootHash endpoint
    main/DatabaseService.java # Merkle tree-backed database logic

rust/
  src/
    main.rs              # Main application entry point
    api/mod.rs           # Warp API: /rootHash endpoint
    database_service/mod.rs # Merkle tree-backed database logic
```

## API

All implementations expose a REST endpoint:

```
GET /rootHash?blockNumber=<number>
```

- Returns the Merkle root hash for the specified block.
- Returns the current root hash if `blockNumber` is the latest.
- Returns historical root hash for previous blocks (if available).
- Returns error for invalid or missing block numbers.

## Running

Each language implementation is self-contained. See below for how to run each:

### Python

```bash
cd python/src
python main.py
# API runs on http://127.0.0.1:8080 by default
```

### Go

```bash
cd go
go run main.go
# API runs on http://127.0.0.1:8080 by default
```

### Java

```bash
cd java
mvn compile exec:java -Dexec.mainClass=main.Main
# API runs on http://localhost:8080 by default
```

### Rust

```bash
cd rust
cargo run
# API runs on http://127.0.0.1:8080 by default
```

## Database Service

- All implementations use a singleton service to manage the Merkle tree.
- Supports: get/set balance, transfer, flush, revert, block root hash storage.
- Database is automatically closed on shutdown.

## Notes

- The code is intended for educational/reference purposes.
- Each language version is designed to be as close as possible in logic and structure.
- Requires the respective language's toolchain and dependencies. 