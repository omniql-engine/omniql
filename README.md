<div align="center">
  <h1>OmniQL</h1>
  <p><strong>Universal Query Language for All Databases</strong></p>
  <p>Write once, run anywhere. One language for PostgreSQL, MySQL, MongoDB, and Redis.</p>
  
  [![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
  [![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
  [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)
</div>

---

## 🎯 The Problem

Modern applications use multiple databases (polyglot persistence), but each requires different query syntax:

- **PostgreSQL** uses SQL with `$1, $2` parameters
- **MySQL** uses SQL with `?` placeholders  
- **MongoDB** uses JSON queries with `{$gt: 25}`
- **Redis** uses command syntax like `ZRANGEBYSCORE`

**Context-switching kills productivity.** Developers spend hours rewriting queries when changing databases.

---

## ✨ The Solution

OmniQL provides a **single, universal query language** that translates to native database syntax:
```oql
GET User WHERE age > 25 AND status = 'active' LIMIT 10
```

**Translates to:**

**PostgreSQL:**
```sql
SELECT * FROM users WHERE age > $1 AND status = $2 LIMIT 10
```

**MySQL:**
```sql
SELECT * FROM users WHERE age > ? AND status = ? LIMIT 10
```

**MongoDB:**
```javascript
db.users.find({age: {$gt: 25}, status: "active"}).limit(10)
```

**Redis:**
```redis
SCAN 0 MATCH user:* COUNT 10
```

**One query. Four databases. Zero rewrites.**

---

## ✨ Features

### 🏗️ Production-Ready Architecture (v1.0)

- ✅ **TrueAST Parser** - 100% expression-based AST, recursive structures everywhere
- ✅ **Bidirectional Translation** - OQL ↔ Native (PostgreSQL, MySQL, MongoDB, Redis)
- ✅ **Smart Error Messages** - Typo detection with "Did you mean?" suggestions
- ✅ **Zero-Latency Translation** - Parse and translate in microseconds  
- ✅ **Type Safety** - Universal type system with database-specific mappings
- ✅ **Battle-Tested** - 765+ tests across all components

### 🔄 Bidirectional Translation

**OQL → Native (Forward):**
```go
query, _ := parser.Parse("GET User WHERE id = 1")
result, _ := translator.Translate(query, "postgresql", "tenant1")
// → SELECT * FROM users WHERE id = $1
```

**Native → OQL (Reverse):**
```go
query, _ := reverse.PostgreSQLToQuery("SELECT * FROM users WHERE id = 1")
// → {Operation: "GET", Entity: "User", Conditions: [...]}

query, _ := reverse.MySQLToQuery("SELECT * FROM users WHERE status = 'active'")
query, _ := reverse.MongoDBToQuery(`{"find": "users", "filter": {"id": 1}}`)
query, _ := reverse.RedisToQuery("HGETALL tenant:123:users:1")
```

### 💡 Smart Error Messages

OmniQL detects typos and suggests corrections:
```
GTE User
→ parse error: unknown operation 'GTE'. Did you mean 'GET'?

GET User WHER id = 1
→ parse error: unknown keyword 'WHER'. Did you mean 'WHERE'?

GET User WHERE id INN (1,2,3)
→ parse error: unknown operator 'INN'. Did you mean 'IN'?

GET User ORDRE BY name
→ parse error: unknown keyword 'ORDRE'. Did you mean 'ORDER BY'?
```

**Coverage:** 87 operations, 16 clauses, 19 operators - all with typo detection.

### 🎯 87 Universal Operations

| Category | Operations | Count |
|----------|------------|-------|
| **CRUD** | GET, CREATE, UPDATE, DELETE, UPSERT, BULK INSERT, REPLACE | ✅ 7 |
| **DDL** | CREATE/DROP/ALTER TABLE, INDEX, VIEW, DATABASE, SCHEMA, SEQUENCE, TRIGGER, FUNCTION, TYPE, DOMAIN, POLICY, RULE, EXTENSION | ✅ 28 |
| **DQL** | JOIN (5 types), Aggregations (5), Window Functions (6), CTEs, Subqueries, Set Operations (4), CASE, EXISTS, PARTITION BY | ✅ 31 |
| **TCL** | BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO, RELEASE SAVEPOINT, START, SET TRANSACTION | ✅ 8 |
| **DCL** | GRANT, REVOKE, CREATE/DROP/ALTER USER, CREATE/DROP ROLE, ASSIGN ROLE, REVOKE ROLE | ✅ 9 |
| **TOTAL** | | **✅ 87** |

### 🧮 Advanced Expression Engine

**Expressions work everywhere:** UPDATE SET, WHERE, ORDER BY, SELECT WITH

**Binary Arithmetic:**
```oql
UPDATE Product SET price = price * 1.1
UPDATE Order SET total = price * quantity
UPDATE Sale SET profit = (price - cost) * qty * (1 - discount)
```

**String Functions:**
```oql
UPDATE User SET name = UPPER(name)
UPDATE Profile SET full_name = CONCAT(first, ' ', last)
GET User WHERE UPPER(email) = 'ADMIN@EXAMPLE.COM'
```

**CASE WHEN Logic:**
```oql
UPDATE User SET status = CASE 
  WHEN age >= 18 THEN 'adult' 
  WHEN age >= 13 THEN 'teen' 
  ELSE 'child' 
END
```

**Calculated Columns:**
```oql
GET Order WITH price * quantity AS total
GET Sale WITH price - cost AS profit, price * qty AS revenue
```

**Supported:**
- **Operators:** `+`, `-`, `*`, `/`, `%`, `<`, `>`, `<=`, `>=`, `=`, `!=`, `AND`, `OR`, `NOT`
- **Functions:** `UPPER`, `LOWER`, `CONCAT`, `LENGTH`, `ABS`, `ROUND`, `NOW`, `COALESCE`

### 📊 Complete Aggregation Support
```oql
# Basic aggregations
COUNT User
SUM Sale amount
AVG Score points
MIN Price value
MAX Stock quantity

# With filtering
COUNT User WHERE age > 25
SUM Sale WHERE status = 'completed' amount

# With grouping
COUNT Order GROUP BY customer
SUM Revenue GROUP BY region

# With HAVING
SUM Sale GROUP BY dept HAVING SUM(amount) > 10000

# Complex combinations
SUM Order WHERE status = 'active' GROUP BY customer 
  HAVING SUM(total) > 1000 ORDER BY customer LIMIT 10
```

### 🔗 Advanced Query Features

**JOINs (5 types):**
```oql
GET User INNER JOIN Order ON User.id = Order.user_id
GET User LEFT JOIN Order ON User.id = Order.user_id WHERE Order.status = 'active'
GET Product RIGHT JOIN Category ON Product.category_id = Category.id
GET TableA FULL JOIN TableB ON TableA.key = TableB.key
GET UserA CROSS JOIN UserB
```

**Window Functions (6 types):**
```oql
GET Sale WITH ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rank
GET Employee WITH LAG(salary) OVER (ORDER BY hire_date) AS prev_salary
GET Student WITH RANK() OVER (PARTITION BY class ORDER BY score DESC) AS rank
GET Data WITH DENSE_RANK() OVER (ORDER BY value) AS dense_rank
GET Category WITH NTILE(4) OVER (ORDER BY revenue DESC) AS quartile
GET Metric WITH LEAD(value) OVER (ORDER BY date) AS next_value
```

**Common Table Expressions (CTEs):**
```oql
CTE regional_sales AS (SUM Sale GROUP BY region) 
  GET regional_sales WHERE total > 100000
```

**Set Operations:**
```oql
GET User WHERE age > 25 UNION GET User WHERE status = 'premium'
GET Product WHERE category = 'electronics' INTERSECT GET Product WHERE price < 1000
GET Employee EXCEPT GET Employee WHERE department = 'deprecated'
UNION ALL ...
```

### 🔒 Enterprise-Grade Security

- ✅ **SQL Injection Prevention** - Parameterized queries for all operations
- ✅ **Input Sanitization** - Automatic escaping of special characters  
- ✅ **Safe String Handling** - Apostrophes (`O'Brien`), quotes, backslashes handled correctly
- ✅ **Type Validation** - Strong type checking at parse time

### ⚡ Transaction Support
```oql
BEGIN
UPDATE Account SET balance = balance - 100 WHERE id = 1
UPDATE Account SET balance = balance + 100 WHERE id = 2
COMMIT

# Or rollback on error
BEGIN
UPDATE Inventory SET stock = stock - 10
ROLLBACK
```

**Advanced Transaction Control:**
- ✅ **SAVEPOINT** - Create rollback points within transactions
- ✅ **ROLLBACK TO** - Partial rollback to savepoint
- ✅ **Isolation Levels** - READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- ✅ **ACID Guarantees** - Full transactional integrity

---

## 🗄️ Supported Databases

| Database | Version | CRUD | Expressions | Aggregations | JOINs | Window Fns | Transactions | Reverse Parser |
|----------|---------|------|-------------|--------------|-------|------------|--------------|----------------|
| **PostgreSQL** | 16+ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **MySQL** | 8.0+ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **MongoDB** | 8.0+ | ✅ | ✅ | ✅ | ✅ via $lookup | ⚠️ Limited | ✅ | ✅ |
| **Redis** | 7.0+ | ✅ | ⚠️ Limited | ✅ via SCAN | ❌ | ❌ | ✅ | ✅ |

**Tested versions:** PostgreSQL 16.10, MySQL 8.0.44, MongoDB 8.0.15, Redis 7.4.4

**Legend:**
- ✅ Full support
- ⚠️ Partial support (database architectural limitations)  
- ❌ Not applicable to database type

---

## 🚀 Installation
```bash
go get github.com/omniql-engine/omniql
```

---

## 💡 Quick Examples

### Basic CRUD
```go
import (
    "github.com/omniql-engine/omniql/engine/parser"
    "github.com/omniql-engine/omniql/engine/translator"
)

// Parse OQL
query, _ := parser.Parse("GET User WHERE age > 25")

// Translate to any database
pgResult, _ := translator.Translate(query, "postgresql", "tenant1")
myResult, _ := translator.Translate(query, "mysql", "tenant1")
mongoResult, _ := translator.Translate(query, "mongodb", "tenant1")
redisResult, _ := translator.Translate(query, "redis", "tenant1")
```

### Reverse Parsing (SQL → OQL)
```go
import "github.com/omniql-engine/omniql/engine/reverse"

// PostgreSQL → OQL
query, _ := reverse.PostgreSQLToQuery("SELECT * FROM users WHERE id = 1")
fmt.Println(query.Operation) // "GET"
fmt.Println(query.Entity)    // "User"

// MySQL → OQL
query, _ := reverse.MySQLToQuery("INSERT INTO users (name, age) VALUES ('John', 25)")
fmt.Println(query.Operation) // "CREATE"

// MongoDB → OQL
query, _ := reverse.MongoDBToQuery(`{"find": "users", "filter": {"status": "active"}}`)

// Redis → OQL
query, _ := reverse.RedisToQuery("HGETALL tenant:123:users:1")
```

### Error Handling with Suggestions
```go
query, err := parser.Parse("GET User WHER id = 1")
if err != nil {
    fmt.Println(err)
    // parse error at line 1, column 10: unknown keyword 'WHER'. Did you mean 'WHERE'?
}
```

---

## 📁 Project Structure
```
omniql/
├── engine/
│   ├── ast/           # Abstract Syntax Tree nodes
│   ├── models/        # Query model definitions
│   ├── lexer/         # Tokenizer with error suggestions
│   ├── parser/        # OQL → AST parser
│   ├── builders/      # AST → Native query builders
│   │   ├── mongodb/
│   │   ├── mysql/
│   │   ├── postgres/
│   │   └── redis/
│   ├── reverse/       # Native → OQL parsers
│   ├── translator/    # Translation orchestration
│   └── validator/     # Query validation per database
├── mapping/           # Operations, clauses, operators (SSOT)
└── utilities/
    └── proto/         # Protocol buffer definitions
```

---

## 🎉 What's New in v1.0

### Major Features

**🔄 Bidirectional Translation**
- Forward: OQL → PostgreSQL, MySQL, MongoDB, Redis
- Reverse: PostgreSQL, MySQL, MongoDB, Redis → OQL
- Full round-trip support for query migration

**💡 Smart Error Messages**
- Levenshtein distance-based typo detection
- Suggestions for 87 operations, 16 clauses, 19 operators
- Unconsumed token detection (no more silent failures)

**🏗️ TrueAST Architecture**
- 100% expression-based AST
- Recursive structures for complex expressions
- Clean separation: Lexer → Parser → Translator → Builder

**✅ Comprehensive Testing**
- 765+ tests across all components
- Round-trip validation for all reverse parsers
- Expression parsing edge cases covered

---

## 🤝 Contributing

We welcome contributions! OmniQL is open-source and community-driven.

**Areas for contribution:**
- Additional database support (CassandraDB, TimescaleDB, ClickHouse)
- SDK implementations (Node.js, Python, Rust)
- Documentation improvements
- Bug fixes and performance improvements

**To contribute:**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 🗺️ Roadmap

### v1.1 (Planned)
- [ ] Advanced JOIN operations (LATERAL, CROSS APPLY)
- [ ] More window functions (FIRST_VALUE, LAST_VALUE, CUME_DIST)
- [ ] Enhanced CTEs (recursive CTEs)
- [ ] Performance optimizations

### v1.2 (Planned)
- [ ] JavaScript/TypeScript SDK
- [ ] Python SDK
- [ ] Query optimization hints
- [ ] Execution plan analysis

### v2.0 (Future)
- [ ] Additional databases (CassandraDB, TimescaleDB, ClickHouse)
- [ ] GraphQL-style nested queries
- [ ] Query result caching
- [ ] Real-time query monitoring

**💡 Want a feature?** [Open an issue!](https://github.com/omniql-engine/omniql/issues)

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 💬 About

OmniQL is developed and maintained by **Binary Leap OÜ** (https://binaryleap.eu).

**Why we built this:**

We needed a universal query abstraction for a multi-tenant database platform. Instead of keeping it proprietary, we're open-sourcing it to help the entire developer community solve the polyglot persistence challenge.

**Powered by OmniQL:** [TenantsDB](https://tenantsdb.com) - Multi-tenant database platform

---

<div align="center">
  
**⭐ Star this repo if you find it useful!**

**Questions?** [Open an issue](https://github.com/omniql-engine/omniql/issues)

</div>