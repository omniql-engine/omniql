package mapping

import "strings"

// OperationGroups maps each operation to its group (CRUD, DDL, DQL, TCL, DCL)
// Used by parser to route operations dynamically - no hardcoded lists!
var OperationGroups = map[string]string{
	// ========== GROUP 1: CRUD (7 operations) ==========
	"GET":         "CRUD",
	"CREATE":      "CRUD",
	"UPDATE":      "CRUD",
	"DELETE":      "CRUD",
	"UPSERT":      "CRUD", // Insert or Update
	"BULK INSERT": "CRUD", // Insert multiple rows
	"REPLACE":     "CRUD", // Delete + Insert (MySQL)
	
	// ========== GROUP 2: DDL (14 operations) ==========
	"CREATE TABLE":      "DDL",
	"ALTER TABLE":       "DDL",
	"DROP TABLE":        "DDL",
	"TRUNCATE TABLE":    "DDL",
	"TRUNCATE":          "DDL",
	"CREATE INDEX":      "DDL",
	"DROP INDEX":        "DDL",
	"CREATE COLLECTION": "DDL",
	"DROP COLLECTION":   "DDL",
	"CREATE DATABASE":   "DDL",
	"DROP DATABASE":     "DDL",
	"CREATE VIEW":       "DDL",
	"DROP VIEW":         "DDL",
	"ALTER VIEW":        "DDL",
	"RENAME TABLE":      "DDL",



	// ↓↓↓ PG SPECIFIC↓↓↓
	"CREATE SEQUENCE":   "DDL",
	"ALTER SEQUENCE":    "DDL",
	"DROP SEQUENCE":     "DDL",
	"CREATE EXTENSION":  "DDL",
	"DROP EXTENSION":    "DDL",
	"CREATE SCHEMA":     "DDL",
	"DROP SCHEMA":       "DDL",
	"CREATE TYPE":       "DDL",
	"DROP TYPE":         "DDL",
	"ALTER TYPE":        "DDL",
	"CREATE DOMAIN":     "DDL",
	"DROP DOMAIN":       "DDL",
	"CREATE FUNCTION":   "DDL",
	"DROP FUNCTION":     "DDL",
	"ALTER FUNCTION":    "DDL",
	"CREATE TRIGGER":    "DDL",
	"DROP TRIGGER":      "DDL",
	"CREATE POLICY":     "DDL",
	"DROP POLICY":       "DDL",
	"CREATE RULE":       "DDL",
	"DROP RULE":         "DDL",
	"COMMENT ON":        "DDL",
	
	// ========== GROUP 3: DQL (31 operations) ==========
	// JOIN operations
	"INNER JOIN": "DQL",
	"LEFT JOIN":  "DQL",
	"RIGHT JOIN": "DQL",
	"FULL JOIN":  "DQL",
	"CROSS JOIN": "DQL",
	
	// Aggregate functions
	"COUNT": "DQL",
	"SUM":   "DQL",
	"AVG":   "DQL",
	"MIN":   "DQL",
	"MAX":   "DQL",
	
	// Query modifiers
	// "GROUP BY": "DQL",
	// "ORDER BY": "DQL",
	// "HAVING":   "DQL",
	// "DISTINCT": "DQL",
	// "LIMIT":    "DQL",
	// "OFFSET":   "DQL",
	
	// Set operations
	"UNION":     "DQL",
	"UNION ALL": "DQL",
	"INTERSECT": "DQL",
	"EXCEPT":    "DQL",
	
	// Window functions
	"ROW NUMBER":  "DQL",
	"RANK":        "DQL",
	"DENSE RANK":  "DQL",
	"LAG":         "DQL",
	"LEAD":        "DQL",
	"NTILE":       "DQL",
	"PARTITION BY": "DQL",
	
	// Advanced query features
	"CTE":      "DQL", // Common Table Expressions (WITH)
	"SUBQUERY": "DQL", // Nested SELECT
	"EXISTS":   "DQL", // Existence check
	// "LIKE":     "DQL", // Pattern matching
	"CASE":     "DQL", // Conditional logic
	
	// ========== GROUP 4: TCL (8 operations) ==========
	"BEGIN":              "TCL",
	"COMMIT":             "TCL",
	"ROLLBACK":           "TCL",
	"SAVEPOINT":          "TCL",
	"ROLLBACK TO":        "TCL",
	"START":              "TCL", // Alias for BEGIN
	"RELEASE SAVEPOINT":  "TCL",
	"SET TRANSACTION":    "TCL", // Isolation levels
	
	// ========== GROUP 5: DCL (9 operations) ==========
	"GRANT":       "DCL",
	"REVOKE":      "DCL",
	"CREATE ROLE": "DCL",
	"DROP ROLE":   "DCL",
	"ASSIGN ROLE": "DCL",
	"REVOKE ROLE": "DCL",
	"CREATE USER": "DCL",
	"DROP USER":   "DCL",
	"ALTER USER":  "DCL",
}

// OperationSubTypes provides finer classification within groups
var OperationSubTypes = map[string]string{
	// CRUD Sub-types
	"GET":         "READ",
	"CREATE":      "WRITE",
	"UPDATE":      "WRITE",
	"DELETE":      "WRITE",
	"UPSERT":      "WRITE",
	"BULK INSERT": "WRITE",
	"REPLACE":     "WRITE",
	
	// DDL Sub-types
	"CREATE TABLE":      "SCHEMA CREATE",
	"ALTER TABLE":       "SCHEMA MODIFY",
	"DROP TABLE":        "SCHEMA DROP",
	"TRUNCATE TABLE":    "SCHEMA TRUNCATE",
	"TRUNCATE":          "SCHEMA TRUNCATE",
	"CREATE INDEX":      "INDEX CREATE",
	"DROP INDEX":        "INDEX DROP",
	"CREATE COLLECTION": "SCHEMA CREATE",
	"DROP COLLECTION":   "SCHEMA DROP",
	"CREATE DATABASE":   "DATABASE CREATE",
	"DROP DATABASE":     "DATABASE DROP",
	"CREATE VIEW":       "VIEW CREATE",
	"DROP VIEW":         "VIEW DROP",
	"ALTER VIEW":        "VIEW MODIFY",
	"RENAME TABLE":      "SCHEMA RENAME",
	
	// DQL Sub-types
	"INNER JOIN": "JOIN",
	"LEFT JOIN":  "JOIN",
	"RIGHT JOIN": "JOIN",
	"FULL JOIN":  "JOIN",
	"CROSS JOIN": "JOIN",
	
	"COUNT": "AGGREGATE",
	"SUM":   "AGGREGATE",
	"AVG":   "AGGREGATE",
	"MIN":   "AGGREGATE",
	"MAX":   "AGGREGATE",
	
	"UNION":     "SET",
	"UNION ALL": "SET",
	"INTERSECT": "SET",
	"EXCEPT":    "SET",
	
	"ROW NUMBER":   "WINDOW",
	"RANK":         "WINDOW",
	"DENSE RANK":   "WINDOW",
	"LAG":          "WINDOW",
	"LEAD":         "WINDOW",
	"NTILE":        "WINDOW",
	"PARTITION BY": "WINDOW",
	
	"CTE":      "ADVANCED",
	"SUBQUERY": "ADVANCED",
	"EXISTS":   "ADVANCED",
	// "LIKE":     "PATTERN",
	"CASE":     "CONDITIONAL",
	
	// TCL Sub-types
	"BEGIN":             "TRANSACTION START",
	"START":             "TRANSACTION START",
	"COMMIT":            "TRANSACTION END",
	"ROLLBACK":          "TRANSACTION ABORT",
	"SAVEPOINT":         "TRANSACTION POINT",
	"ROLLBACK TO":       "TRANSACTION PARTIAL",
	"RELEASE SAVEPOINT": "TRANSACTION RELEASE",
	"SET TRANSACTION":   "TRANSACTION CONFIG",
	
	// DCL Sub-types
	"GRANT":       "PERMISSION GRANT",
	"REVOKE":      "PERMISSION REVOKE",
	"CREATE ROLE": "ROLE CREATE",
	"DROP ROLE":   "ROLE DROP",
	"ASSIGN ROLE": "ROLE ASSIGN",
	"REVOKE ROLE": "ROLE REVOKE",
	"CREATE USER": "USER CREATE",
	"DROP USER":   "USER DROP",
	"ALTER USER":  "USER MODIFY",
}

// OperationMap - Runtime mapping for translators
var OperationMap = map[string]map[string]string{
	"PostgreSQL": {
		// ========== GROUP 1: CRUD Operations ==========
		"GET":         "select",
		"CREATE":      "insert",
		"UPDATE":      "update",
		"DELETE":      "delete",
		"UPSERT":      "upsert",
		"BULK INSERT": "bulk_insert",
		"REPLACE":     "insert", // PostgreSQL uses INSERT ... ON CONFLICT
		
		// ========== GROUP 2: DDL Operations ==========
		"CREATE TABLE":   "create_table",
		"ALTER TABLE":    "alter_table",
		"DROP TABLE":     "drop_table",
		"TRUNCATE TABLE": "truncate_table",
		"CREATE INDEX":   "create_index",
		"DROP INDEX":     "drop_index",
		"CREATE DATABASE": "create_database",
		"DROP DATABASE":   "drop_database",
		"CREATE VIEW":     "create_view",
		"DROP VIEW":       "drop_view",
		"ALTER VIEW": "alter_view",
		"RENAME TABLE":    "alter_table_rename",

		// PostgreSQL-specific DDL
		"CREATE SEQUENCE":  "create_sequence",
		"ALTER SEQUENCE":   "alter_sequence",
		"DROP SEQUENCE":    "drop_sequence",
		"CREATE EXTENSION": "create_extension",
		"DROP EXTENSION":   "drop_extension",
		"CREATE SCHEMA":    "create_schema",
		"DROP SCHEMA":      "drop_schema",
		"CREATE TYPE":      "create_type",
		"ALTER TYPE":       "alter_type",
		"DROP TYPE":        "drop_type",
		"CREATE DOMAIN":    "create_domain",
		"DROP DOMAIN":      "drop_domain",
		"CREATE FUNCTION":  "create_function",
		"ALTER FUNCTION":   "alter_function",
		"DROP FUNCTION":    "drop_function",
		"CREATE TRIGGER":   "create_trigger",
		"DROP TRIGGER":     "drop_trigger",
		"CREATE POLICY":    "create_policy",
		"DROP POLICY":      "drop_policy",
		"CREATE RULE":      "create_rule",
		"DROP RULE":        "drop_rule",
		"COMMENT ON":       "comment_on",
		
		// ========== GROUP 3: DQL Operations ==========
		// JOIN operations
		"INNER JOIN": "inner_join",
		"LEFT JOIN":  "left_join",
		"RIGHT JOIN": "right_join",
		"FULL JOIN":  "full_join",
		"CROSS JOIN": "cross_join",
		
		// Aggregate functions
		"COUNT": "count",
		"SUM":   "sum",
		"AVG":   "avg",
		"MIN":   "min",
		"MAX":   "max",
		
		// Query modifiers
		"GROUP BY": "group_by",
		"ORDER BY": "order_by",
		"HAVING":   "having",
		"DISTINCT": "distinct",
		"LIMIT":    "limit",
		"OFFSET":   "offset",
		
		// Set operations
		"UNION":     "union",
		"UNION ALL": "union_all",
		"INTERSECT": "intersect",
		"EXCEPT":    "except",
		
		// Window functions
		"ROW NUMBER":   "row_number",
		"RANK":         "rank",
		"DENSE RANK":   "dense_rank",
		"LAG":          "lag",
		"LEAD":         "lead",
		"NTILE":        "ntile",
		"PARTITION BY": "partition_by",
		
		// Advanced query features
		"CTE":      "with",
		"SUBQUERY": "subquery",
		"EXISTS":   "exists",
		"LIKE":     "like",
		"CASE":     "case",
		
		// ========== GROUP 4: TCL Operations ==========
		"BEGIN":             "begin",
		"START":             "begin",
		"COMMIT":            "commit",
		"ROLLBACK":          "rollback",
		"SAVEPOINT":         "savepoint",
		"ROLLBACK TO":       "rollback_to",
		"RELEASE SAVEPOINT": "release_savepoint",
		"SET TRANSACTION":   "set_transaction",
		
		// ========== GROUP 5: DCL Operations ==========
		"GRANT":       "grant",
		"REVOKE":      "revoke",
		"CREATE ROLE": "create_role",
		"DROP ROLE":   "drop_role",
		"ASSIGN ROLE": "assign_role",  
		"REVOKE ROLE": "revoke_role",
		"CREATE USER": "create_user",
		"DROP USER":   "drop_user",
		"ALTER USER":  "alter_user",
	},
	"MySQL": {
		// ========== GROUP 1: CRUD Operations ==========
		"GET":         "select",
		"CREATE":      "insert",
		"UPDATE":      "update",
		"DELETE":      "delete",
		"UPSERT":      "upsert",
		"BULK INSERT": "bulk_insert",
		"REPLACE":     "replace", // MySQL has native REPLACE
		
		// ========== GROUP 2: DDL Operations ==========
		"CREATE TABLE":   "create_table",
		"ALTER TABLE":    "alter_table",
		"DROP TABLE":     "drop_table",
		"TRUNCATE TABLE": "truncate_table",
		"CREATE INDEX":   "create_index",
		"DROP INDEX":     "drop_index",
		"CREATE DATABASE": "create_database",
		"DROP DATABASE":   "drop_database",
		"CREATE VIEW":     "create_view",
		"DROP VIEW":       "drop_view",
		"ALTER VIEW":      "alter_view",
		"RENAME TABLE":    "rename_table",
		
		// ========== GROUP 3: DQL Operations ==========
		"INNER JOIN": "inner_join",
		"LEFT JOIN":  "left_join",
		"RIGHT JOIN": "right_join",
		"FULL JOIN":  "full_join",
		"CROSS JOIN": "cross_join",
		
		"COUNT": "count",
		"SUM":   "sum",
		"AVG":   "avg",
		"MIN":   "min",
		"MAX":   "max",
		
		"GROUP BY": "group_by",
		"ORDER BY": "order_by",
		"HAVING":   "having",
		"DISTINCT": "distinct",
		"LIMIT":    "limit",
		"OFFSET":   "offset",
		
		"UNION":     "union",
		"UNION ALL": "union_all",
		"INTERSECT": "intersect",
		"EXCEPT":    "except",
		
		// Window functions (MySQL 8.0+)
		"ROW NUMBER":   "row_number",
		"RANK":         "rank",
		"DENSE RANK":   "dense_rank",
		"LAG":          "lag",
		"LEAD":         "lead",
		"NTILE":        "ntile",
		"PARTITION BY": "partition_by",
		
		// Advanced query features
		"CTE":      "with",
		"SUBQUERY": "subquery",
		"EXISTS":   "exists",
		"LIKE":     "like",
		"CASE":     "case",
		
		// ========== GROUP 4: TCL Operations ==========
		"BEGIN":             "start_transaction",
		"START":             "start_transaction",
		"COMMIT":            "commit",
		"ROLLBACK":          "rollback",
		"SAVEPOINT":         "savepoint",
		"ROLLBACK TO":       "rollback_to",
		"RELEASE SAVEPOINT": "release_savepoint",
		"SET TRANSACTION":   "set_transaction",
		
		// ========== GROUP 5: DCL Operations ==========
		"GRANT":       "grant",
		"REVOKE":      "revoke",
		"CREATE ROLE": "create_role",
		"DROP ROLE":   "drop_role",
		"ASSIGN ROLE": "assign_role",  
		"REVOKE ROLE": "revoke_role",
		"CREATE USER": "create_user",
		"DROP USER":   "drop_user",
		"ALTER USER":  "alter_user",
	},
	"SQLite": {
		// ========== GROUP 1: CRUD Operations ==========
		"GET":         "select",
		"CREATE":      "insert",
		"UPDATE":      "update",
		"DELETE":      "delete",
		"UPSERT":      "upsert",
		"BULK INSERT": "bulk_insert",
		"REPLACE":     "replace", // SQLite has INSERT OR REPLACE
		
		// ========== GROUP 2: DDL Operations ==========
		"CREATE TABLE":   "create_table",
		"ALTER TABLE":    "alter_table",
		"DROP TABLE":     "drop_table",
		"TRUNCATE TABLE": "delete", // SQLite doesn't have TRUNCATE
		"CREATE INDEX":   "create_index",
		"DROP INDEX":     "drop_index",
		"CREATE DATABASE": "attach",    // SQLite uses ATTACH DATABASE
		"DROP DATABASE":   "detach",    // SQLite uses DETACH DATABASE
		"CREATE VIEW":     "create_view",
		"DROP VIEW":       "drop_view",
		"ALTER VIEW":      "drop_create_view", // SQLite: drop then create
		"RENAME TABLE":    "alter_table_rename",
		
		// ========== GROUP 3: DQL Operations ==========
		"INNER JOIN": "inner_join",
		"LEFT JOIN":  "left_join",
		"RIGHT JOIN": "right_join",
		"FULL JOIN":  "full_join",
		"CROSS JOIN": "cross_join",
		
		"COUNT": "count",
		"SUM":   "sum",
		"AVG":   "avg",
		"MIN":   "min",
		"MAX":   "max",
		
		"GROUP BY": "group_by",
		"ORDER BY": "order_by",
		"HAVING":   "having",
		"DISTINCT": "distinct",
		"LIMIT":    "limit",
		"OFFSET":   "offset",
		
		"UNION":     "union",
		"UNION ALL": "union_all",
		"INTERSECT": "intersect",
		"EXCEPT":    "except",
		
		// Window functions (SQLite 3.25+)
		"ROW NUMBER":   "row_number",
		"RANK":         "rank",
		"DENSE RANK":   "dense_rank",
		"LAG":          "lag",
		"LEAD":         "lead",
		"NTILE":        "ntile",
		"PARTITION BY": "partition_by",
		
		// Advanced query features
		"CTE":      "with",
		"SUBQUERY": "subquery",
		"EXISTS":   "exists",
		"LIKE":     "like",
		"CASE":     "case",
		
		// ========== GROUP 4: TCL Operations ==========
		"BEGIN":             "begin",
		"START":             "begin",
		"COMMIT":            "commit",
		"ROLLBACK":          "rollback",
		"SAVEPOINT":         "savepoint",
		"ROLLBACK TO":       "rollback_to",
		"RELEASE SAVEPOINT": "release",
		"SET TRANSACTION":   "unsupported", // SQLite has limited transaction config
		
		// ========== GROUP 5: DCL Operations ==========
		"GRANT":       "unsupported",
		"REVOKE":      "unsupported",
		"CREATE ROLE": "unsupported",
		"DROP ROLE":   "unsupported",
		"ASSIGN ROLE": "unsupported",
		"REVOKE ROLE": "unsupported",
		"CREATE USER": "unsupported",
		"DROP USER":   "unsupported",
		"ALTER USER":  "unsupported",
	},
	"MongoDB": {
		// ========== GROUP 1: CRUD Operations ==========
		"GET":         "find",
		"CREATE":      "insertOne",
		"UPDATE":      "updateOne",
		"DELETE":      "deleteOne",
		"UPSERT":      "updateOne", // MongoDB updateOne with upsert: true
		"BULK INSERT": "insertMany",
		"REPLACE":     "replaceOne",
		
		// ========== GROUP 2: DDL Operations ==========
		"CREATE TABLE":    "createCollection",  // MongoDB: collection = table
		"CREATE COLLECTION": "createCollection",
		"ALTER TABLE":     "modifyCollection",
		"DROP TABLE":      "dropCollection",
		"TRUNCATE TABLE":  "deleteMany",
		"CREATE INDEX":    "create_index",
		"DROP INDEX":      "drop_index",
		"CREATE DATABASE": "use", // MongoDB creates DB on first write
		"DROP DATABASE":   "drop_database",
		"CREATE VIEW":     "create_view",
		"DROP VIEW":       "drop_view",
		"ALTER VIEW":      "alter_view",
		"RENAME TABLE":    "renameCollection",
		"TRUNCATE": "deleteMany",
		
		// ========== GROUP 3: DQL Operations ==========
		"INNER JOIN": "lookup",
		"LEFT JOIN":  "lookup",
		"RIGHT JOIN": "lookup",
		"FULL JOIN":  "lookup",
		"CROSS JOIN": "lookup",
		
		"COUNT": "count",
		"SUM":   "sum",
		"AVG":   "avg",
		"MIN":   "min",
		"MAX":   "max",
		
		"GROUP BY": "group",
		"ORDER BY": "sort",
		"HAVING":   "match",
		"DISTINCT": "distinct",
		"LIMIT":    "limit",
		"OFFSET":   "skip",
		
		"UNION":     "unionWith",
		"UNION ALL": "unionWith",
		"INTERSECT": "intersect",
		"EXCEPT":    "setDifference",
		
		// Window functions (MongoDB 5.0+)
		"ROW NUMBER":   "row_number",
		"RANK":         "rank",
		"DENSE RANK":   "dense_rank",
		"LAG":          "shift",
		"LEAD":         "shift",
		"NTILE":        "ntile",
		"PARTITION BY": "partition_by",
		
		// Advanced query features
		"CTE":      "unsupported", // MongoDB doesn't have CTEs
		"SUBQUERY": "pipeline",    // MongoDB uses aggregation pipeline
		"EXISTS":   "exists",
		"LIKE":     "regex",
		"CASE":     "cond",
		
		// ========== GROUP 4: TCL Operations ==========
		"BEGIN":             "start_transaction",
		"START":             "start_transaction",
		"COMMIT":            "commit",
		"ROLLBACK":          "abort",
		"SAVEPOINT":         "unsupported",
		"ROLLBACK TO":       "unsupported",
		"RELEASE SAVEPOINT": "unsupported",
		"SET TRANSACTION":   "set_transaction",
		
		// ========== GROUP 5: DCL Operations ==========
		"GRANT":       "grant",
		"REVOKE":      "revoke",
		"CREATE ROLE": "create_role",
		"DROP ROLE":   "drop_role",
		"ASSIGN ROLE": "grant_role",
		"REVOKE ROLE": "revoke_role",
		"CREATE USER": "create_user",
		"DROP USER":   "drop_user",
		"ALTER USER":  "alter_user",
	},
		"Redis": {
		// ========== GROUP 1: CRUD Operations ==========
		"GET":         "HGETALL",     // ← CHANGED from "GET"
		"CREATE":      "HMSET",       // ← CHANGED from "SET"  
		"UPDATE":      "HSET",        // ← CHANGED from "SET"
		"DELETE":      "DEL",
		"COUNT":       "COUNT",
		"SUM":         "SUM",      
		"AVG":         "AVG",     
		"MIN":         "MIN",      
		"MAX":         "MAX",      
		"UPSERT":      "HMSET",       // ← CHANGED from "SET"
		"BULK INSERT": "BULK INSERT", // ← NO CHANGE (special handling)
		"REPLACE":     "HMSET",       // ← CHANGED from "SET"

		// ========== GROUP 2: DDL Operations ==========
        "DROP TABLE":  "DROP_TABLE",  // ← ADD THIS LINE ONLY
		
		// ========== GROUP 4: TCL Operations ==========
		"BEGIN":       "MULTI",       // ← NO CHANGE
		"START":       "MULTI",       // ← NO CHANGE
		"COMMIT":      "EXEC",        // ← NO CHANGE
		"ROLLBACK":    "DISCARD",     // ← NO CHANGE
		"SAVEPOINT":   "",            // ← NO CHANGE
		"ROLLBACK TO": "",            // ← NO CHANGE
		"RELEASE SAVEPOINT": "",      // ← NO CHANGE
		"SET TRANSACTION": "",        // ← NO CHANGE
		
		// ========== GROUP 5: DCL Operations ==========
		"GRANT":       "ACL",  // Translator must add "SETUSER" as first arg
		"REVOKE":      "ACL",  // Translator must add "SETUSER" as first arg
		"CREATE USER": "ACL",  // Translator must add "SETUSER" as first arg
		"DROP USER":   "ACL",  // Translator must add "DELUSER" as first arg
		"ALTER USER":  "ACL",  // Translator must add "SETUSER" as first arg
		"CREATE ROLE": "",     // Redis doesn't have roles, only users with permissions
		"DROP ROLE":   "",     // Redis doesn't have roles
		"ASSIGN ROLE": "",     // Redis doesn't have roles
		"REVOKE ROLE": "",     // Redis doesn't have roles
	},
}

// TableNamingRules defines how operations convert entity names
var TableNamingRules = map[string]string{
		// ========== GROUP 1: CRUD - use plural ==========
		"GET":         "plural",
		"CREATE":      "plural",
		"UPDATE":      "plural",
		"DELETE":      "plural",
		"UPSERT":      "plural",
		"BULK INSERT": "plural",
		"REPLACE":     "plural",

		// ========== GROUP 2: DDL - table operations use plural, others use exact ==========
		"CREATE TABLE":      "plural",  // Creates plural table names
		"ALTER TABLE":       "plural",  // References tables created by CREATE TABLE
		"DROP TABLE":        "plural",  // References tables created by CREATE TABLE
		"TRUNCATE TABLE":    "plural",  // References tables created by CREATE TABLE
		"CREATE INDEX":      "plural",  // Index on plural table names
		"DROP INDEX":        "plural",  // Index on plural table names
		"RENAME TABLE":      "plural",  // Renames plural tables
		"CREATE COLLECTION": "exact",   // MongoDB - explicit names
		"DROP COLLECTION":   "exact",   // MongoDB - explicit names
		"CREATE DATABASE":   "exact",   // Database names are explicit
		"DROP DATABASE":     "exact",   // Database names are explicit
		"CREATE VIEW":       "exact",   // Views can have custom names
		"DROP VIEW":         "exact",   // Views can have custom names
		"ALTER VIEW":        "exact",   // Views can have custom names
	
		// ========== GROUP 3: DQL - use plural ==========
		"INNER JOIN": "plural",
		"LEFT JOIN":  "plural",
		"RIGHT JOIN": "plural",
		"FULL JOIN":  "plural",
		"CROSS JOIN": "plural",
		
		"COUNT": "plural",
		"SUM":   "plural",
		"AVG":   "plural",
		"MIN":   "plural",
		"MAX":   "plural",
		
		"GROUP BY": "plural",
		"ORDER BY": "plural",
		"HAVING":   "plural",
		"DISTINCT": "plural",
		"LIMIT":    "plural",
		"OFFSET":   "plural",
		
		"UNION":     "plural",
		"UNION ALL": "plural",
		"INTERSECT": "plural",
		"EXCEPT":    "plural",
		
		"ROW NUMBER":   "plural",
		"RANK":         "plural",
		"DENSE RANK":   "plural",
		"LAG":          "plural",
		"LEAD":         "plural",
		"NTILE":        "plural",
		"PARTITION BY": "plural",
		
		"CTE":      "plural",
		"SUBQUERY": "plural",
		"EXISTS":   "plural",
		"LIKE":     "plural",
		"CASE":     "plural",
		
		// ========== GROUP 4: TCL - no table ==========
		"BEGIN":             "none",
		"START":             "none",
		"COMMIT":            "none",
		"ROLLBACK":          "none",
		"SAVEPOINT":         "none",
		"ROLLBACK TO":       "none",
		"RELEASE SAVEPOINT": "none",
		"SET TRANSACTION":   "none",
		
		// ========== GROUP 5: DCL ==========
		"GRANT":       "plural",
		"REVOKE":      "plural",
		"CREATE ROLE": "none",
		"DROP ROLE":   "none",
		"ASSIGN ROLE": "none",
		"REVOKE ROLE": "none",
		"CREATE USER": "none",
		"DROP USER":   "none",
		"ALTER USER":  "none",
}

// OperationMapping for documentation
type OperationMapping struct {
	OQL        string
	PostgreSQL string
	MySQL      string
	SQLite     string
	MongoDB    string
	Redis      string
}

// OperationTemplates - Extended documentation
var OperationTemplates = map[string]OperationMapping{
	// ========== GROUP 1: CRUD Operations ==========
	"GET ALL": {
		OQL:        "GET {Entity}",
		PostgreSQL: "SELECT * FROM {table}",
		MySQL:      "SELECT * FROM {table}",
		SQLite:     "SELECT * FROM {table}",
		MongoDB:    "db.{table}.find({})",
		Redis:      "KEYS {tenant}:{entity}:*",
	},
	"GET WHERE": {
		OQL:        "GET {Entity} WHERE {conditions}",
		PostgreSQL: "SELECT * FROM {table} WHERE {conditions}",
		MySQL:      "SELECT * FROM {table} WHERE {conditions}",
		SQLite:     "SELECT * FROM {table} WHERE {conditions}",
		MongoDB:    "db.{table}.find({conditions})",
		Redis:      "GET {tenant}:{entity}:{id}",
	},
	"CREATE": {
		OQL:        "CREATE {Entity} WITH {fields}",
		PostgreSQL: "INSERT INTO {table} ({fields}) VALUES ({values})",
		MySQL:      "INSERT INTO {table} ({fields}) VALUES ({values})",
		SQLite:     "INSERT INTO {table} ({fields}) VALUES ({values})",
		MongoDB:    "db.{table}.insertOne({document})",
		Redis:      "SET {tenant}:{entity}:{id} {json}",
	},
	"UPDATE": {
		OQL:        "UPDATE {Entity} SET {fields} WHERE {conditions}",
		PostgreSQL: "UPDATE {table} SET {fields} WHERE {conditions}",
		MySQL:      "UPDATE {table} SET {fields} WHERE {conditions}",
		SQLite:     "UPDATE {table} SET {fields} WHERE {conditions}",
		MongoDB:    "db.{table}.updateOne({filter}, {$set: {fields}})",
		Redis:      "SET {tenant}:{entity}:{id} {json}",
	},
	"DELETE": {
		OQL:        "DELETE {Entity} WHERE {conditions}",
		PostgreSQL: "DELETE FROM {table} WHERE {conditions}",
		MySQL:      "DELETE FROM {table} WHERE {conditions}",
		SQLite:     "DELETE FROM {table} WHERE {conditions}",
		MongoDB:    "db.{table}.deleteOne({filter})",
		Redis:      "DEL {tenant}:{entity}:{id}",
	},
	"UPSERT": {
		OQL:        "UPSERT {Entity} WITH {fields} ON {conflict_fields}",
		PostgreSQL: "INSERT INTO {table} ({fields}) VALUES ({values}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}",
		MySQL:      "INSERT INTO {table} ({fields}) VALUES ({values}) ON DUPLICATE KEY UPDATE {updates}",
		SQLite:     "INSERT OR REPLACE INTO {table} ({fields}) VALUES ({values})",
		MongoDB:    "db.{table}.updateOne({filter}, {$set: {fields}}, {upsert: true})",
	},
	"BULK INSERT": {
		OQL:        "BULK INSERT {Entity} WITH [{rows}]",
		PostgreSQL: "INSERT INTO {table} ({fields}) VALUES {multiple_rows}",
		MySQL:      "INSERT INTO {table} ({fields}) VALUES {multiple_rows}",
		SQLite:     "INSERT INTO {table} ({fields}) VALUES {multiple_rows}",
		MongoDB:    "db.{table}.insertMany([{documents}])",
		Redis:      "MSET {key1} {value1} {key2} {value2}",
	},
	
	// ========== GROUP 2: DDL Operations ==========
	"CREATE TABLE": {
		OQL:        "CREATE TABLE {table} WITH {columns}",
		PostgreSQL: "CREATE TABLE {table} ({columns})",
		MySQL:      "CREATE TABLE {table} ({columns})",
		SQLite:     "CREATE TABLE {table} ({columns})",
		MongoDB:    "db.createCollection('{collection}')",
	},
	"DROP TABLE": {
		OQL:        "DROP TABLE {table}",
		PostgreSQL: "DROP TABLE IF EXISTS {table}",
		MySQL:      "DROP TABLE IF EXISTS {table}",
		SQLite:     "DROP TABLE IF EXISTS {table}",
		MongoDB:    "db.{collection}.drop()",
	},
	"CREATE VIEW": {
		OQL:        "CREATE VIEW {view_name} AS {query}",
		PostgreSQL: "CREATE VIEW {view_name} AS {query}",
		MySQL:      "CREATE VIEW {view_name} AS {query}",
		SQLite:     "CREATE VIEW {view_name} AS {query}",
		MongoDB:    "db.createView('{view}', '{collection}', [{pipeline}])",
	},
	"CREATE DATABASE": {
		OQL:        "CREATE DATABASE {database_name}",
		PostgreSQL: "CREATE DATABASE {database_name}",
		MySQL:      "CREATE DATABASE {database_name}",
		SQLite:     "ATTACH DATABASE '{file}' AS {database_name}",
		MongoDB:    "use {database_name}",
	},
	
	// ========== GROUP 3: DQL Operations ==========
	"INNER JOIN": {
		OQL:        "INNER JOIN {Entity} {JoinTable} ON {left} = {right}",
		PostgreSQL: "SELECT * FROM {table} INNER JOIN {join_table} ON {condition}",
		MySQL:      "SELECT * FROM {table} INNER JOIN {join_table} ON {condition}",
		SQLite:     "SELECT * FROM {table} INNER JOIN {join_table} ON {condition}",
		MongoDB:    "db.{collection}.aggregate([{$lookup: {...}}])",
	},
	"ROW NUMBER": {
		OQL:        "GET {Entity} WITH ROW NUMBER OVER (PARTITION BY {field} ORDER BY {field})",
		PostgreSQL: "SELECT *, ROW NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) FROM {table}",
		MySQL:      "SELECT *, ROW NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) FROM {table}",
		SQLite:     "SELECT *, ROW NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) FROM {table}",
		MongoDB:    "db.{collection}.aggregate([{$setWindowFields: {partitionBy: '$field', sortBy: {field: 1}, output: {row: {$documentNumber: {}}}}}])",
	},
	"CTE": {
		OQL:        "WITH {cte_name} AS ({query}) GET {Entity}",
		PostgreSQL: "WITH {cte_name} AS ({query}) SELECT * FROM {table}",
		MySQL:      "WITH {cte_name} AS ({query}) SELECT * FROM {table}",
		SQLite:     "WITH {cte_name} AS ({query}) SELECT * FROM {table}",
		MongoDB:    "N/A (use aggregation pipeline)",
	},
	"SUBQUERY": {
		OQL:        "GET {Entity} WHERE {field} IN (GET {SubEntity})",
		PostgreSQL: "SELECT * FROM {table} WHERE {field} IN (SELECT {field} FROM {subtable})",
		MySQL:      "SELECT * FROM {table} WHERE {field} IN (SELECT {field} FROM {subtable})",
		SQLite:     "SELECT * FROM {table} WHERE {field} IN (SELECT {field} FROM {subtable})",
		MongoDB:    "db.{collection}.find({field: {$in: [{values}]}})",
	},
	"LIKE": {
		OQL:        "GET {Entity} WHERE {field} LIKE {pattern}",
		PostgreSQL: "SELECT * FROM {table} WHERE {field} LIKE '{pattern}'",
		MySQL:      "SELECT * FROM {table} WHERE {field} LIKE '{pattern}'",
		SQLite:     "SELECT * FROM {table} WHERE {field} LIKE '{pattern}'",
		MongoDB:    "db.{collection}.find({field: {$regex: /{pattern}/}})",
	},
	"CASE": {
		OQL:        "GET {Entity} WITH CASE WHEN {condition} THEN {value} ELSE {default}",
		PostgreSQL: "SELECT *, CASE WHEN {condition} THEN {value} ELSE {default} END AS {alias} FROM {table}",
		MySQL:      "SELECT *, CASE WHEN {condition} THEN {value} ELSE {default} END AS {alias} FROM {table}",
		SQLite:     "SELECT *, CASE WHEN {condition} THEN {value} ELSE {default} END AS {alias} FROM {table}",
		MongoDB:    "db.{collection}.aggregate([{$project: {alias: {$cond: [{condition}, {value}, {default}]}}}])",
	},
	
	// ========== GROUP 4: TCL Operations ==========
	"BEGIN": {
		OQL:        "BEGIN",
		PostgreSQL: "BEGIN",
		MySQL:      "START TRANSACTION",
		SQLite:     "BEGIN TRANSACTION",
		MongoDB:    "session.startTransaction()",
		Redis:      "MULTI",
	},
	"COMMIT": {
		OQL:        "COMMIT",
		PostgreSQL: "COMMIT",
		MySQL:      "COMMIT",
		SQLite:     "COMMIT",
		MongoDB:    "session.commitTransaction()",
		Redis:      "EXEC",
	},
	"ROLLBACK": {
		OQL:        "ROLLBACK",
		PostgreSQL: "ROLLBACK",
		MySQL:      "ROLLBACK",
		SQLite:     "ROLLBACK",
		MongoDB:    "session.abortTransaction()",
		Redis:      "DISCARD",
	},
	"SAVEPOINT": {
		OQL:        "SAVEPOINT {name}",
		PostgreSQL: "SAVEPOINT {name}",
		MySQL:      "SAVEPOINT {name}",
		SQLite:     "SAVEPOINT {name}",
	},
	"SET TRANSACTION": {
		OQL:        "SET TRANSACTION ISOLATION LEVEL {level}",
		PostgreSQL: "SET TRANSACTION ISOLATION LEVEL {level}",
		MySQL:      "SET TRANSACTION ISOLATION LEVEL {level}",
		SQLite:     "PRAGMA read_uncommitted = {value}",
		MongoDB:    "session.startTransaction({readConcern: {level}})",
	},
	
	// ========== GROUP 5: DCL Operations ==========
	"GRANT": {
		OQL:        "GRANT {permissions} ON {table} TO {target}",
		PostgreSQL: "GRANT {permissions} ON {table} TO {target}",
		MySQL:      "GRANT {permissions} ON {database}.{table} TO '{target}'@'localhost'",
		SQLite:     "N/A (file-based permissions)",
		MongoDB:    "db.grantRolesToUser('{target}', [{role: '{permission}', db: '{database}'}])",
		Redis:      "ACL SETUSER {target} on +{permissions}",  // ✅ CHANGED - shows actual Redis syntax
	},
	"REVOKE": {
		OQL:        "REVOKE {permissions} ON {table} FROM {target}",
		PostgreSQL: "REVOKE {permissions} ON {table} FROM {target}",
		MySQL:      "REVOKE {permissions} ON {database}.{table} FROM '{target}'@'localhost'",
		SQLite:     "N/A (file-based permissions)",
		MongoDB:    "db.revokeRolesFromUser('{target}', [{role: '{permission}', db: '{database}'}])",
		Redis:      "ACL SETUSER {target} -{permissions}",  // ✅ CHANGED - minus sign removes permissions
	},
	"CREATE USER": {
		OQL:        "CREATE USER {username} WITH PASSWORD {password}",
		PostgreSQL: "CREATE USER {username} WITH PASSWORD '{password}'",
		MySQL:      "CREATE USER '{username}'@'localhost' IDENTIFIED BY '{password}'",
		SQLite:     "N/A",
		MongoDB:    "db.createUser({user: '{username}', pwd: '{password}', roles: []})",
		Redis:      "ACL SETUSER {username} on >{password}",  // ✅ CHANGED - shows actual Redis syntax
	},
	"DROP USER": {
		OQL:        "DROP USER {username}",
		PostgreSQL: "DROP USER {username}",
		MySQL:      "DROP USER '{username}'@'localhost'",
		SQLite:     "N/A",
		MongoDB:    "db.dropUser('{username}')",
		Redis:      "ACL DELUSER {username}",  // ✅ CHANGED - shows actual Redis syntax
	},
	"ALTER USER": {
		OQL:        "ALTER USER {username} WITH PASSWORD {password}",
		PostgreSQL: "ALTER USER {username} WITH PASSWORD '{password}'",
		MySQL:      "ALTER USER '{username}'@'localhost' IDENTIFIED BY '{password}'",
		SQLite:     "N/A",
		MongoDB:    "db.updateUser('{username}', {pwd: '{password}'})",
		Redis:      "ACL SETUSER {username} >{password}",  // ✅ ADDED
	},
	"CREATE ROLE": {
		OQL:        "CREATE ROLE {role_name}",
		PostgreSQL: "CREATE ROLE {role_name}",
		MySQL:      "CREATE ROLE {role_name}",
		SQLite:     "N/A",
		MongoDB:    "db.createRole({role: '{role_name}', privileges: [], roles: []})",
		Redis:      "N/A (Redis has users with permissions, not roles)",  // ✅ ADDED
	},
	"DROP ROLE": {
		OQL:        "DROP ROLE {role_name}",
		PostgreSQL: "DROP ROLE {role_name}",
		MySQL:      "DROP ROLE {role_name}",
		SQLite:     "N/A",
		MongoDB:    "db.dropRole('{role_name}')",
		Redis:      "N/A (Redis has users with permissions, not roles)",  // ✅ ADDED
	},
	"ASSIGN ROLE": {
		OQL:        "ASSIGN ROLE {role_name} TO {username}",
		PostgreSQL: "GRANT {role_name} TO {username}",
		MySQL:      "GRANT {role_name} TO '{username}'@'localhost'",
		SQLite:     "N/A",
		MongoDB:    "db.grantRolesToUser('{username}', ['{role_name}'])",
		Redis:      "N/A (Redis has users with permissions, not roles)",  // ✅ ADDED
	},
	"REVOKE ROLE": {
		OQL:        "REVOKE ROLE {role_name} FROM {username}",
		PostgreSQL: "REVOKE {role_name} FROM {username}",
		MySQL:      "REVOKE {role_name} FROM '{username}'@'localhost'",
		SQLite:     "N/A",
		MongoDB:    "db.revokeRolesFromUser('{username}', ['{role_name}'])",
		Redis:      "N/A (Redis has users with permissions, not roles)",  // ✅ ADDED
	},
}

// TranslatedToGroup - reverse mapping built from above
var TranslatedToGroup map[string]map[string]string

func init() {
    TranslatedToGroup = make(map[string]map[string]string)
    
    for dbType, ops := range OperationMap {
        TranslatedToGroup[dbType] = make(map[string]string)
        
        for oqlOp, translated := range ops {
            if group, exists := OperationGroups[oqlOp]; exists {
                TranslatedToGroup[dbType][translated] = group
            }
        }
    }
	
}

// IsAggregate checks if operation is an aggregate function
func IsAggregate(op string) bool {
    return OperationSubTypes[strings.ToUpper(op)] == "AGGREGATE"
}