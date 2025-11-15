package redis

// ============================================================================
// REDIS BUILDERS
// ============================================================================
//
// Unlike PostgreSQL, MySQL, and MongoDB, Redis does not use separate builder
// functions for command construction.
//
// Redis commands are simple key-value operations that are built directly in
// the translator (oql/translator/redis.go) without requiring complex SQL or
// aggregation pipeline construction.
//
// This package exists for consistency with the project structure but contains
// no builder functions.
//
// For Redis command construction, see:
//   - oql/translator/redis.go (command building)
//   - database/redis/crud.go (command execution)
//   - database/redis/dql.go (aggregation operations)
//
// ============================================================================