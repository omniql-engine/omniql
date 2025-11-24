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
:GET User WHERE age > 25 AND status = active LIMIT 10
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
# (with server-side filtering)
```

**One query. Four databases. Zero rewrites.**

---

## ✨ Features

### 🏗️ Production-Ready Architecture (v1.0)

- ✅ **Clean Builder Separation** - SQL/query generation fully extracted and testable
- ✅ **Zero-Latency Translation** - Parse and translate in microseconds  
- ✅ **Type Safety** - Universal type system with database-specific mappings
- ✅ **Battle-Tested** - 200+ integration tests, concurrent transaction tests
- ✅ **No Lock-In** - Native queries still work (no `:` prefix = pass-through)

### 🎯 70+ Universal Operations

<table>
<thead>
<tr>
<th>Category</th>
<th>Operations</th>
<th>Count</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>CRUD</strong></td>
<td>CREATE, READ, UPDATE, DELETE, UPSERT, BULK INSERT, REPLACE</td>
<td>✅ 7</td>
</tr>
<tr>
<td><strong>DDL</strong></td>
<td>CREATE/DROP/ALTER TABLE, INDEX, VIEW, DATABASE</td>
<td>✅ 14</td>
</tr>
<tr>
<td><strong>DQL</strong></td>
<td>JOIN (5 types), Aggregations (5), Window Functions (6), CTEs, Subqueries, Set Operations (3), LIKE, CASE</td>
<td>✅ 31</td>
</tr>
<tr>
<td><strong>TCL</strong></td>
<td>BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO, RELEASE, SET TRANSACTION, Isolation Levels</td>
<td>✅ 8</td>
</tr>
<tr>
<td><strong>DCL</strong></td>
<td>GRANT, REVOKE, CREATE USER, DROP USER, ALTER USER, CREATE ROLE, DROP ROLE, ASSIGN ROLE, REVOKE ROLE</td>
<td>✅ 9</td>
</tr>
<tr>
<td colspan="2"><strong>TOTAL</strong></td>
<td><strong>✅ 69</strong></td>
</tr>
</tbody>
</table>

### 🧮 Advanced Expression Engine

**Expressions work everywhere:** UPDATE SET, WHERE, ORDER BY, SELECT

**Binary Arithmetic:**
```oql
UPDATE Product SET price = price * 1.1                    # 10% increase
UPDATE Order SET total = price * quantity                 # Field-to-field
UPDATE Sale SET profit = (price - cost) * qty * (1 - discount)  # Nested
```

**String Functions:**
```oql
UPDATE User SET name = UPPER(name)                        # Case conversion
UPDATE Profile SET full_name = CONCAT(first, last)        # Concatenation
WHERE UPPER(email) = ADMIN@EXAMPLE.COM                    # Function in WHERE
```

**CASE WHEN Logic:**
```oql
UPDATE User SET status = CASE 
  WHEN age >= 18 THEN adult 
  WHEN age >= 13 THEN teen 
  ELSE child 
END
```

**Calculated Columns:**
```oql
GET Order WITH price * quantity AS total                  # Simple calculation
GET Sale WITH price - cost AS profit, price * qty AS revenue  # Multiple
```

**Supported:**
- **Operators:** `+`, `-`, `*`, `/`, `%`, `<`, `>`, `<=`, `>=`, `=`, `!=`, `AND`, `OR`, `NOT`
- **Functions:** `UPPER`, `LOWER`, `CONCAT`, `LENGTH`, `ABS`, `ROUND`, `NOW`, `COALESCE`

### 📊 Complete Aggregation Support

**All aggregations work with ALL clauses:**
```oql
# Basic aggregations
GET User COUNT
GET Sale SUM amount
GET Score AVG points
GET Price MIN value
GET Stock MAX quantity

# With filtering
GET User WHERE age > 25 COUNT
GET Sale WHERE status = completed SUM amount

# With grouping
GET Order GROUP BY customer COUNT
GET Revenue GROUP BY region SUM amount

# With HAVING
GET Sale GROUP BY dept SUM amount HAVING SUM(amount) > 10000

# Complex combinations
GET Order WHERE status = active GROUP BY customer SUM total 
  HAVING SUM(total) > 1000 ORDER BY customer:ASC LIMIT 10
```

**Aggregation + Clause Support Matrix:**

<table>
<thead>
<tr>
<th>Clause</th>
<th>COUNT</th>
<th>SUM</th>
<th>AVG</th>
<th>MIN</th>
<th>MAX</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>WHERE</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>LIMIT</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>OFFSET</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>ORDER BY</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>GROUP BY</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>HAVING</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>DISTINCT</strong></td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>❌</td>
<td>❌</td>
</tr>
</tbody>
</table>

### 🔗 Advanced Query Features

**JOINs (5 types):**
```oql
GET User INNER JOIN Order ON User.id = Order.user_id
GET User LEFT JOIN Order ON User.id = Order.user_id WHERE Order.status = active
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
WITH regional_sales AS (GET Sale GROUP BY region SUM amount) 
  GET regional_sales WHERE total > 100000
```

**Set Operations:**
```oql
GET User WHERE age > 25 UNION GET User WHERE status = premium
GET Product WHERE category = electronics INTERSECT GET Product WHERE price < 1000
GET Employee EXCEPT GET Employee WHERE department = deprecated
```

### 🔒 Enterprise-Grade Security

- ✅ **SQL Injection Prevention** - Parameterized queries for all operations
- ✅ **Input Sanitization** - Automatic escaping of special characters  
- ✅ **Safe String Handling** - Apostrophes (`O'Brien`), quotes, backslashes handled correctly
- ✅ **Type Validation** - Strong type checking at parse time
- ✅ **Tested Against Attacks** - Comprehensive security test suite

### ⚡ Transaction Support
```oql
BEGIN                                          # Start transaction
UPDATE Account SET balance = balance - 100 WHERE id = 1
UPDATE Account SET balance = balance + 100 WHERE id = 2
COMMIT                                         # Commit changes

# Or rollback on error
BEGIN
UPDATE Inventory SET stock = stock - 10
ROLLBACK                                       # Undo changes
```

**Advanced Transaction Control:**
- ✅ **SAVEPOINT** - Create rollback points within transactions
- ✅ **ROLLBACK TO** - Partial rollback to savepoint
- ✅ **Isolation Levels** - READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- ✅ **ACID Guarantees** - Full transactional integrity

---

## 🗄️ Supported Databases

<table>
<thead>
<tr>
<th>Database</th>
<th>Version</th>
<th>CRUD</th>
<th>Expressions</th>
<th>Aggregations</th>
<th>JOINs</th>
<th>Window Fns</th>
<th>Transactions</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>PostgreSQL</strong></td>
<td>15-17*</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>MySQL</strong></td>
<td>8.0+</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
</tr>
<tr>
<td><strong>MongoDB</strong></td>
<td>7.0-8.x*</td>
<td>✅</td>
<td>✅</td>
<td>✅</td>
<td>✅ via $lookup</td>
<td>⚠️ Limited</td>
<td>✅</td>
</tr>
<tr>
<td><strong>Redis</strong></td>
<td>7.0+*</td>
<td>✅</td>
<td>⚠️ Limited</td>
<td>✅ via SCAN</td>
<td>❌</td>
<td>❌</td>
<td>✅</td>
</tr>
</tbody>
</table>

**\*Tested versions:** PostgreSQL 16.10, MySQL 8.0.44, MongoDB 8.0.15, Redis 7.4.4

**Legend:**
- ✅ Full support
- ⚠️ Partial support (database architectural limitations)
- ❌ Not applicable to database type

**Legend:**
- ✅ Full support
- ⚠️ Partial support (database architectural limitations)
- ❌ Not applicable to database type

### Database-Specific Notes:

**PostgreSQL** - Full OQL support, all 69 operations work perfectly

**MySQL** - Full OQL support, minor syntax differences handled automatically  

**MongoDB** - Full document operations, aggregation pipelines, JOIN support via `$lookup`

**Redis** - Key-value operations, aggregations via SCAN, no complex queries (not a relational database)

---

## 🚀 Installation

**Go:**
```bash
go get github.com/omniql-engine/omniql
```

> **Note:** Currently Go only. JavaScript/TypeScript and Python SDKs planned for future releases.

---

## 💡 Quick Examples

### Basic CRUD
```go
import "github.com/omniql-engine/omniql/engine/translator"
import "github.com/omniql-engine/omniql/engine/parser"

// Parse OQL
query, _ := parser.Parse(":GET User WHERE age > 25")

// Translate to PostgreSQL
result, _ := translator.Translate(query, "PostgreSQL", "")
sql := result.GetRelational().Sql
// "SELECT * FROM users WHERE age > $1"

// Execute with your own database
db.Query(sql, 25)
```

### Advanced Expressions
```go
// Complex calculation with CASE WHEN
oql := `:UPDATE Product SET 
  discount = CASE 
    WHEN stock > 100 THEN 0.20 
    WHEN stock > 50 THEN 0.10 
    ELSE 0.05 
  END,
  final_price = price * (1 - discount)
  WHERE category = electronics`

query, _ := parser.Parse(oql)
result, _ := translator.Translate(query, "MySQL", "")
sql := result.GetRelational().Sql
// MySQL-specific SQL with proper syntax
```

### Aggregations with GROUP BY
```go
oql := `:GET Sale 
  WHERE date >= 2024-01-01 
  GROUP BY region 
  SUM amount 
  HAVING SUM(amount) > 100000 
  ORDER BY region:ASC`

query, _ := parser.Parse(oql)
result, _ := translator.Translate(query, "PostgreSQL", "")
sql := result.GetRelational().Sql
// Complex aggregation with grouping and filtering
```

### Multi-Database Support
```go
oql := ":GET User WHERE status = active ORDER BY created_at:DESC LIMIT 10"
query, _ := parser.Parse(oql)

// Same query, different databases
pgResult, _ := translator.Translate(query, "PostgreSQL", "")
pgSQL := pgResult.GetRelational().Sql

mysqlResult, _ := translator.Translate(query, "MySQL", "")
mysqlSQL := mysqlResult.GetRelational().Sql

mongoResult, _ := translator.Translate(query, "MongoDB", "")
mongoQuery := mongoResult.GetDocument().Query

// All three return database-native syntax
```

---

## 🎉 What's New in v1.0

### Major Improvements

**🏗️ Clean Architecture**
- Extracted SQL builders into separate, testable modules
- Clear separation: Translation → Building → Execution
- Single source of truth for query generation

**🧮 Complete Expression Support**
- Binary arithmetic in all contexts (UPDATE, WHERE, SELECT, ORDER BY)
- String functions (UPPER, LOWER, CONCAT, LENGTH)
- Math functions (ABS, ROUND)
- CASE WHEN statements with multiple conditions
- Nested expressions with parentheses

**📊 Full Aggregation Coverage**
- All 5 aggregations (COUNT, SUM, AVG, MIN, MAX)
- Work with ALL clauses (WHERE, GROUP BY, HAVING, LIMIT, etc.)
- DISTINCT support for COUNT, SUM, AVG
- Complex multi-clause combinations

**🔒 Production-Ready Security**
- Comprehensive SQL injection prevention
- Safe handling of special characters
- Input validation at parse time
- Tested with malicious inputs

**✅ Battle-Tested**
- 200+ integration tests across all databases
- Concurrent transaction testing
- Expression security testing
- Correctness verification tests

### Breaking Changes from v0.x

- ❌ **Removed SQLite** - Untested, removed from official support
- ✅ **Translators now populate output fields** - `result.Sql`, `result.Query`, `result.CommandString` are now populated
- ✅ **Builder functions exported** - Available at `github.com/omniql-engine/omniql/engine/builders`

---

## 📚 Documentation

**Full documentation coming soon at omniql.com**

For now, explore:
- `/engine/parser` - OQL syntax parser
- `/engine/translator` - Database-specific translators
- `/engine/builders` - SQL/query builders
- `/mapping` - Operation and operator mappings

---

## 🤝 Contributing

We welcome contributions! OmniQL is open-source and community-driven.

**Areas for contribution:**
- Additional database support (CassandraDB, TimescaleDB, QuestDB)
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
- [ ] Additional databases (CassandraDB, TimescaleDB, QuestDB)
- [ ] GraphQL-style nested queries
- [ ] Query result caching
- [ ] Real-time query monitoring

**💡 Want a feature?** [Open an issue!](https://github.com/omniql-engine/omniql/issues)

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 💬 About

OmniQL is developed and maintained by **Binary Leap EU** (https://binaryleap.eu).

**Why we built this:**

We needed a universal query abstraction for a multi-tenant database platform. Instead of keeping it proprietary, we're open-sourcing it to help the entire developer community solve the polyglot persistence challenge.

**Powered by OmniQL:** [TenantsDB](https://tenantsdb.com) - Multi-tenant database platform

---

<div align="center">
  
**⭐ Star this repo if you find it useful!**

**Questions?** [Open an issue](https://github.com/omniql-engine/omniql/issues)

**Follow us:** [Twitter](https://twitter.com/omniql) • [LinkedIn](https://linkedin.com/company/omniql)

</div>