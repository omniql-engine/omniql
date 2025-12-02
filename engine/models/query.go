package models

// ============================================================================
// QUERY - Universal container for all operation types
// ============================================================================

// Query represents a parsed OQL query
// Supports all 5 operation groups: CRUD, DDL, DQL, TCL, DCL
type Query struct {
	// ========== BASIC INFO (All Operations) ==========
	Operation string // GET, CREATE, UPDATE, DELETE, CREATE TABLE, INNER JOIN, COUNT, BEGIN, GRANT, etc.
	Entity    string // User, Order, etc. (main table/collection)
	Columns       []string       // Column selection (e.g., ["User.name", "Project.title"])
	SelectColumns []SelectColumn // ✅ ADD THIS LINE: SELECT with expressions (e.g., price * quantity AS total)

	// ========== GROUP 1: CRUD ==========
	Conditions []Condition // WHERE conditions
	Fields     []Field     // Field values (CREATE/UPDATE) or Column definitions (CREATE TABLE)
	Limit      int         // LIMIT clause (pagination)
	Offset     int         // OFFSET clause (pagination)
	Distinct   bool  

	// ========== GROUP 1: CRUD EXTENSIONS ==========
	Upsert   *Upsert       // UPSERT operation (ON CONFLICT)
	BulkData [][]Field     // BULK INSERT data (multiple rows)
	Pattern  string        // LIKE pattern matching

	// ========== GROUP 2: DDL ==========
	DatabaseName string // CREATE DATABASE, DROP DATABASE
	ViewName     string // CREATE VIEW, DROP VIEW, ALTER VIEW
	ViewQuery    string // View definition (SELECT query)
	NewName      string // RENAME TABLE (new name)

	// ========== GROUP 3: DQL (Data Query Language) ==========
	Joins           []Join            // JOIN clauses (INNER, LEFT, RIGHT, FULL, CROSS)
	Aggregate       *Aggregation      // Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
	GroupBy         []string          // GROUP BY fields
	Having          []Condition       // HAVING conditions (filters after grouping)
	OrderBy         []OrderBy         // ORDER BY clauses
	WindowFunctions []WindowFunction  // Window functions (ROW NUMBER, RANK, etc.)
	CTE             *CTE              // Common Table Expressions (WITH clause)
	Subquery        *Subquery         // Subqueries (IN, EXISTS)
	CaseStatement   *CaseStatement    // CASE WHEN statements
	SetOperation    *SetOperation     // Set operations (UNION, INTERSECT, EXCEPT)

	// ========== GROUP 4: TCL (Transaction Control Language) ==========
	Transaction *Transaction // Transaction operations (BEGIN, COMMIT, ROLLBACK, etc.)

	// ========== GROUP 5: DCL (Data Control Language) ==========
	Permission *Permission // Permission operations (GRANT, REVOKE, user/role management)
}

// ============================================================================
// BASIC STRUCTURES (Used by CRUD and DDL)
// ============================================================================

// Condition represents a WHERE or HAVING condition
type Condition struct {
	Field      string      // Field name (e.g., "id", "age")
	Operator   string      // =, >, <, !=, >=, <=, IN, LIKE, BETWEEN, IS NULL
	Value      string      // The value to compare
	Value2     string      // NEW: Second value for BETWEEN operator
	Values     []string    // NEW: Multiple values for IN operator
	Logic      string      // NEW: "AND" or "OR" (for combining conditions)
	Conditions []Condition // NEW: Nested conditions for parentheses grouping
}

// Field represents a field for CREATE/UPDATE or column definitions
type Field struct {
	Name        string   // Field/column name
	Value       string   // Type for DDL, value for CRUD (STILL HERE - existing code works)
	Constraints []string // DDL constraints: UNIQUE, NOT_NULL, PRIMARY_KEY, etc.
	
	// NEW: Expression support (optional - nil for non-expressions)
	Expression *FieldExpression // Only set for UPDATE with expressions
}

// FieldExpression represents binary expressions, functions, and CASE WHEN
type FieldExpression struct {
	Type         string // "BINARY", "FUNCTION", or "CASEWHEN"
	
	// For binary expressions (e.g., value + 1)
	LeftOperand  string
	Operator     string // "+", "-", "*", "/"
	RightOperand string
	LeftIsField  bool
	RightIsField bool
	
	// For functions (e.g., UPPER(name))
	FunctionName string
	FunctionArgs []string
	
	// For CASE WHEN statements
	CaseConditions []CaseCondition
	CaseElse       string
}

// CaseCondition represents a WHEN-THEN pair in CASE WHEN statements
type CaseCondition struct {
	Condition string // "age >= 18"
	ThenValue string // "adult"
}

// SelectColumn represents a column in SELECT with optional expression
type SelectColumn struct {
	Expression    string           // The full expression as string
	ExpressionObj *FieldExpression // Parsed expression object
	Alias         string           // Optional alias (AS name)
}

// ============================================================================
// GROUP 1: CRUD EXTENSIONS
// ============================================================================

// Upsert represents UPSERT operation (INSERT ... ON CONFLICT UPDATE)
type Upsert struct {
	ConflictFields []string // Fields that define uniqueness (e.g., ["email"])
	UpdateFields   []Field  // Fields to update on conflict
}

// ============================================================================
// GROUP 3: DQL (Data Query Language) - JOIN SUPPORT
// ============================================================================

// Join represents a JOIN clause
type Join struct {
	Type       JoinType // INNER, LEFT, RIGHT, FULL, CROSS
	Table      string   // Table to join (e.g., "projects")
	LeftField  string   // Left side of ON (e.g., "users.id")
	RightField string   // Right side of ON (e.g., "projects.user_id")
}

// JoinType represents the type of join
type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
	FullJoin  JoinType = "FULL"
	CrossJoin JoinType = "CROSS"
)

// ============================================================================
// GROUP 3: DQL - AGGREGATION SUPPORT
// ============================================================================

// Aggregation represents aggregate functions
type Aggregation struct {
	Function AggregateFunc // COUNT, SUM, AVG, MIN, MAX
	Field    string        // Field to aggregate (empty for COUNT(*))
}

// AggregateFunc represents aggregate function types
type AggregateFunc string

const (
	Count AggregateFunc = "COUNT"
	Sum   AggregateFunc = "SUM"
	Avg   AggregateFunc = "AVG"
	Min   AggregateFunc = "MIN"
	Max   AggregateFunc = "MAX"
)

// ============================================================================
// GROUP 3: DQL - ORDER BY SUPPORT
// ============================================================================

// OrderBy represents ORDER BY clause
type OrderBy struct {
	Field      string           // Field to sort by
	Expression *FieldExpression // Expression support (price * quantity)
	Direction  SortDirection    // ASC or DESC
}

// SortDirection represents sort order
type SortDirection string

const (
	Ascending  SortDirection = "ASC"
	Descending SortDirection = "DESC"
)

// ============================================================================
// GROUP 3: DQL - WINDOW FUNCTIONS
// ============================================================================

// WindowFunction represents window function operations
type WindowFunction struct {
	Function    WindowFunc // ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
	Field       string     // Field for LAG/LEAD (e.g., "amount")
	Alias       string     // Result column alias
	PartitionBy []string   // PARTITION BY fields
	OrderBy     []OrderBy  // ORDER BY within window
	Offset      int        // Offset for LAG/LEAD (default 1)
	Buckets     int        // Number of buckets for NTILE
}

// WindowFunc represents window function types
type WindowFunc string

const (
	RowNumber  WindowFunc = "ROW NUMBER"
	Rank       WindowFunc = "RANK"
	DenseRank  WindowFunc = "DENSE RANK"
	Lag        WindowFunc = "LAG"
	Lead       WindowFunc = "LEAD"
	Ntile      WindowFunc = "NTILE"
)

// ============================================================================
// GROUP 3: DQL - COMMON TABLE EXPRESSIONS (CTE)
// ============================================================================

// CTE represents Common Table Expressions (WITH clause)
type CTE struct {
	Name      string  // CTE name (e.g., "temp_users")
	Query     string  // SELECT query defining the CTE
	Recursive bool    // Is this a recursive CTE?
	MainQuery string  // Main query that uses the CTE
}

// ============================================================================
// GROUP 3: DQL - SUBQUERIES
// ============================================================================

// Subquery represents nested SELECT statements
type Subquery struct {
	Type  string // "IN", "EXISTS", "NOT_IN", "NOT_EXISTS", "SCALAR"
	Field string // Field to compare (for IN/NOT_IN)
	Query string // The nested SELECT statement
	Alias string // Subquery alias (for scalar subqueries)
}

// ============================================================================
// GROUP 3: DQL - CASE STATEMENTS
// ============================================================================

// CaseStatement represents CASE WHEN statements
type CaseStatement struct {
	WhenClauses []CaseWhen // Multiple WHEN conditions
	ElseValue   string     // ELSE clause (optional)
	Alias       string     // Result column alias
}

// CaseWhen represents a WHEN condition in CASE statement
type CaseWhen struct {
	Condition string // WHEN condition (e.g., "status = 'active'")
	ThenValue string // THEN value
}

// ============================================================================
// GROUP 3: DQL - SET OPERATIONS
// ============================================================================

// SetOperation represents set operations that combine two queries
type SetOperation struct {
	Type       SetOperationType // UNION, UNION ALL, INTERSECT, EXCEPT
	LeftQuery  *Query           // First query
	RightQuery *Query           // Second query
}

// SetOperationType represents types of set operations
type SetOperationType string

const (
	Union     SetOperationType = "UNION"
	UnionAll  SetOperationType = "UNION ALL"
	Intersect SetOperationType = "INTERSECT"
	Except    SetOperationType = "EXCEPT"
)

// ============================================================================
// GROUP 4: TCL (Transaction Control Language)
// ============================================================================

// Transaction represents transaction control operations
type Transaction struct {
	Operation      string // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO, RELEASE SAVEPOINT, SET TRANSACTION
	SavepointName  string // Savepoint name (for SAVEPOINT/ROLLBACK TO/RELEASE SAVEPOINT)
	IsolationLevel string // SERIALIZABLE, REPEATABLE READ, READ COMMITTED, READ UNCOMMITTED
	ReadOnly       bool   // Read-only transaction flag
}

// ============================================================================
// GROUP 5: DCL (Data Control Language)
// ============================================================================

// Permission represents permission and user/role management operations
type Permission struct {
	Operation   string   // GRANT, REVOKE, CREATE ROLE, DROP ROLE, ASSIGN ROLE, REVOKE ROLE, CREATE USER, DROP USER, ALTER USER
	Permissions []string // Permission list (["READ", "WRITE", "DELETE"])
	Target      string   // Target tenant_id or role_name (for GRANT/REVOKE)
	RoleName    string   // Role name (for role operations)
	UserName    string   // User name (for user operations)
	Password    string   // User password (for CREATE USER/ALTER USER)
	Roles       []string // User roles (for CREATE USER/ALTER USER)
}