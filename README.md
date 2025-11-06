<div align="center">
  <h1>OmniQL</h1>
  <p><strong>Universal query abstraction layer for all databases</strong></p>
  <p>Write once, deploy anywhere. One language for PostgreSQL, MySQL, MongoDB, and Redis.</p>
</div>

---

## The Problem

Modern applications use multiple databases (polyglot persistence), but each database requires different query syntax:

- PostgreSQL uses SQL
- MongoDB uses JSON queries
- Redis uses command syntax
- Switching databases means rewriting all queries


**Context-switching kills productivity.**


## The Solution

OmniQL provides a single, universal query language that translates to native database syntax:
```oql
:GET User WHERE age > 25 AND status = active LIMIT 10
```

**Translates to:**

**PostgreSQL:**
```sql
SELECT * FROM users WHERE age > 25 AND status = 'active' LIMIT 10;
```

**MySQL:**
```sql
SELECT * FROM users WHERE age > 25 AND status = 'active' LIMIT 10;
```

**MongoDB:**
```json
db.users.find({
  age: { $gt: 25 },
  status: "active"
}).limit(10)
```

**Redis:**
```redis
SCAN 0 MATCH tenant:user:* COUNT 10
# (with server-side filtering)
```

**One query. Four databases. Zero rewrites.**


## Features

✅ **70+ Operations** - CRUD, DDL, DQL, TCL, DCL operations
✅ **Zero-Latency Translation** - Parse and translate in microseconds
✅ **Type Safety** - Universal type system with database-specific mappings
✅ **Operator Abstraction** - 50+ operators work across all databases
✅ **No Lock-In** - Native queries still work (no `:` prefix = native pass-through)
✅ **Production Ready** - Battle-tested in TenantsDB platform

## Why OmniQL?

**1. Write Once, Deploy Anywhere**
Change databases without rewriting queries. PostgreSQL to MongoDB? Just change the connection string.

**2. Polyglot Persistence Made Simple**
Use the right database for the job without learning new query languages.

**3. Future-Proof Your Stack**
Database landscape changes. Your queries shouldn't.

**4. Database Migrations = Configuration Change**
What used to take weeks of query rewrites now takes minutes.


## Installation

**Go:**
```bash
go get github.com/omniql-engine/omniql
```

> **Note:** Currently Go only. JavaScript/TypeScript and Python SDKs coming soon.


## Supported Databases

| Database   | Status          | CRUD | DDL | DQL | TCL | DCL |
|------------|-----------------|:----:|:---:|:---:|:---:|:---:|
| PostgreSQL | ✅ Production   | ✅   | ✅  | ✅  | ✅  | ✅  |
| MySQL      | ✅ Production   | ✅   | ✅  | ✅  | ✅  | ✅  |
| MongoDB    | ✅ Production   | ✅   | ✅  | ✅  | ✅  | ✅  |
| Redis      | ✅ Production   | ✅   | ❌  | ❌  | ✅  | ✅  |
| SQLite     | ⚠️ Untested.    | 🚧   | 🚧  | 🚧  | 🚧  | 🚧  |
| QuestDB    | ⏳ Coming Soon  | ⏳   | ⏳  | ⏳  | ⏳  | ⏳  |

**Operation Groups:**
- **CRUD** - Create, Read, Update, Delete (7 operations)
- **DDL** - Data Definition Language (14 operations)
- **DQL** - Data Query Language (31 operations - joins, aggregations, window functions)
- **TCL** - Transaction Control (8 operations)
- **DCL** - Data Control Language (9 operations - permissions, users, roles)

## Documentation

📚 **Full documentation coming soon at [omniql.com](https://omniql.com)**

For now, explore:
- `/engine` - Core translation engine
- `/parser` - OQL syntax parser
- `/translator` - Database-specific translators
- `/mapping` - Operation and operator mappings


## Examples

**More examples coming soon!**

For now, check out the `/examples` directory (coming soon) or explore the test files in the repository.

## Contributing

We welcome contributions! OmniQL is open-source and community-driven.

**Areas for contribution:**
- Additional database support (QuestDB, TimescaleDB, etc.)
- SDK implementations (Node.js, Python, Rust)
- Documentation improvements
- Bug fixes and performance improvements

**To contribute:**
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

## About

OmniQL is developed and maintained by [Binary Leap](https://binaryleap.com), the team behind [TenantsDB](https://tenantsdb.com) - a multi-tenant database platform powered by OmniQL.

**Why we built this:**
We needed a way to abstract database operations across multiple database types for TenantsDB. Instead of keeping it proprietary, we're open-sourcing it to help the entire developer community.

---

**⭐ Star this repo if you find it useful!**

**Questions? [Open an issue](https://github.com/omniql-engine/omniql/issues)**