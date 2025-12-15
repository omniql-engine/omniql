package models

// ============================================================================
// QUERY - Universal container for all operation types (100% TrueAST)
// ============================================================================

// Query represents a parsed OQL query
// All expression fields use *Expression for recursive tree structure
type Query struct {
	// ========== BASIC INFO ==========
	Operation     string         // Keyword: GET, CREATE, UPDATE, DELETE, etc.
	Entity        string         // Table/collection name
	Columns       []*Expression  // 100% TrueAST - column selection
	SelectColumns []SelectColumn // SELECT with aliases

	// ========== CRUD ==========
	Conditions []Condition // WHERE conditions
	Fields     []Field     // Field assignments or column definitions
	Limit      int         // LIMIT clause
	Offset     int         // OFFSET clause
	Distinct   bool

	// ========== CRUD EXTENSIONS ==========
	Upsert   *Upsert   // UPSERT operation
	BulkData [][]Field // BULK INSERT data
	Pattern  string    // LIKE pattern matching

	// ========== DDL ==========
	AlterAction  string // ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, MODIFY_COLUMN
	DatabaseName string // Name identifier
	ViewName     string // Name identifier
	ViewQuery    *Query // 100% TrueAST - parsed subquery
	NewName      string // Name identifier

	// ========== DQL ==========
	Joins           []Join           // JOIN clauses
	Aggregate       *Aggregation     // Aggregate functions
	GroupBy         []*Expression    // 100% TrueAST
	Having          []Condition      // HAVING conditions
	OrderBy         []OrderBy        // ORDER BY clauses
	WindowFunctions []WindowFunction // Window functions
	SetOperation    *SetOperation    // UNION, INTERSECT, EXCEPT
	CTE             *CTE             // Common Table Expressions (WITH clause)
	Subquery        *Subquery        // Subqueries (IN, EXISTS)
	CaseStatement   *CaseStatement   // CASE WHEN statements

	// ========== TCL ==========
	Transaction *Transaction

	// ========== DCL ==========
	Permission *Permission
}

// ============================================================================
// EXPRESSION - Recursive tree node (100% TrueAST)
// ============================================================================

// Expression represents any expression in the AST
// This is the core building block for 100% TrueAST
type Expression struct {
	Type     string // BINARY, FUNCTION, CASEWHEN, FIELD, LITERAL, WINDOW
	Position int

	// For leaf nodes (FIELD, LITERAL)
	Value string

	// For BINARY expressions
	Left     *Expression
	Operator string
	Right    *Expression

	// For FUNCTION calls
	FunctionName string
	FunctionArgs []*Expression

	// For CASEWHEN
	CaseConditions []*CaseCondition
	CaseElse       *Expression

	// For WINDOW functions
	PartitionBy   []*Expression
	WindowOrderBy []OrderBy
	WindowOffset  int
	WindowBuckets int
}

// ============================================================================
// CONDITION - WHERE/HAVING clause (100% TrueAST)
// ============================================================================

// Condition represents a WHERE or HAVING condition
type Condition struct {
	FieldExpr  *Expression   // 100% TrueAST - left side
	Operator   string        // =, !=, >, <, IN, BETWEEN, etc.
	ValueExpr  *Expression   // 100% TrueAST - right side
	Value2Expr *Expression   // For BETWEEN second value
	ValuesExpr []*Expression // For IN operator values
	Logic      string        // AND, OR
	Nested     []Condition   // For parentheses grouping
}

// ============================================================================
// FIELD - Column definition or assignment (100% TrueAST)
// ============================================================================

// Field represents a field for CREATE/UPDATE or column definitions
type Field struct {
	NameExpr    *Expression // 100% TrueAST - field name
	ValueExpr   *Expression // 100% TrueAST - field value or type
	Constraints []string    // DDL constraints: UNIQUE, NOT_NULL, PRIMARY_KEY
}

// ============================================================================
// CASE CONDITION (100% TrueAST)
// ============================================================================

// CaseCondition represents a WHEN-THEN pair
type CaseCondition struct {
	Condition *Condition  // 100% TrueAST - WHEN condition
	ThenExpr  *Expression // 100% TrueAST - THEN value
}

// ============================================================================
// SELECT COLUMN (100% TrueAST)
// ============================================================================

// SelectColumn represents a column in SELECT with optional alias
type SelectColumn struct {
	ExpressionObj *Expression // 100% TrueAST
	Alias         string      // Optional alias
}

// ============================================================================
// UPSERT (100% TrueAST)
// ============================================================================

// Upsert represents UPSERT operation
type Upsert struct {
	ConflictFields []*Expression // 100% TrueAST
	UpdateFields   []Field
}

// ============================================================================
// JOIN (100% TrueAST)
// ============================================================================

// Join represents a JOIN clause
type Join struct {
	Type      JoinType    // INNER, LEFT, RIGHT, FULL, CROSS
	Table     string      // Table name
	LeftExpr  *Expression // 100% TrueAST
	RightExpr *Expression // 100% TrueAST
}

// JoinType for type safety
type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
	FullJoin  JoinType = "FULL"
	CrossJoin JoinType = "CROSS"
)

// ============================================================================
// AGGREGATION (100% TrueAST)
// ============================================================================

// Aggregation represents aggregate functions
type Aggregation struct {
	Function  AggregateFunc // COUNT, SUM, AVG, MIN, MAX
	FieldExpr *Expression   // 100% TrueAST
}

// AggregateFunc for type safety
type AggregateFunc string

const (
	Count AggregateFunc = "COUNT"
	Sum   AggregateFunc = "SUM"
	Avg   AggregateFunc = "AVG"
	Min   AggregateFunc = "MIN"
	Max   AggregateFunc = "MAX"
)

// ============================================================================
// ORDER BY (100% TrueAST)
// ============================================================================

// OrderBy represents ORDER BY clause
type OrderBy struct {
	FieldExpr *Expression   // 100% TrueAST
	Direction SortDirection // ASC, DESC
}

// SortDirection for type safety
type SortDirection string

const (
	Asc  SortDirection = "ASC"
	Desc SortDirection = "DESC"
)

// ============================================================================
// WINDOW FUNCTION (100% TrueAST)
// ============================================================================

// WindowFunction represents window function operations
type WindowFunction struct {
	Function    WindowFunc    // ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
	FieldExpr   *Expression   // 100% TrueAST
	Alias       string        // Result column alias
	PartitionBy []*Expression // 100% TrueAST
	OrderBy     []OrderBy
	Offset      int // For LAG/LEAD
	Buckets     int // For NTILE
}

// WindowFunc for type safety
type WindowFunc string

const (
	RowNumber WindowFunc = "ROW_NUMBER"
	Rank      WindowFunc = "RANK"
	DenseRank WindowFunc = "DENSE_RANK"
	Lag       WindowFunc = "LAG"
	Lead      WindowFunc = "LEAD"
	Ntile     WindowFunc = "NTILE"
)

// ============================================================================
// SET OPERATION (100% TrueAST)
// ============================================================================

// SetOperation represents UNION, INTERSECT, EXCEPT
type SetOperation struct {
	Type       SetOperationType // UNION, UNION_ALL, INTERSECT, EXCEPT
	LeftQuery  *Query
	RightQuery *Query
}

// SetOperationType for type safety
type SetOperationType string

const (
	Union     SetOperationType = "UNION"
	UnionAll  SetOperationType = "UNION_ALL"
	Intersect SetOperationType = "INTERSECT"
	Except    SetOperationType = "EXCEPT"
)

// ============================================================================
// CTE - Common Table Expression (100% TrueAST)
// ============================================================================

// CTE represents Common Table Expression (WITH clause)
type CTE struct {
	Name      string // CTE name
	Query     *Query // CTE query definition
	MainQuery *Query // Main query that uses the CTE
	Recursive bool   // Is it recursive CTE?
}

// ============================================================================
// SUBQUERY (100% TrueAST)
// ============================================================================

// Subquery represents subqueries (IN, EXISTS)
type Subquery struct {
	Type      string      // IN, EXISTS
	FieldExpr *Expression // 100% TrueAST - field for IN clause
	Query     *Query      // The subquery
	Alias     string      // Optional alias
}

// ============================================================================
// CASE STATEMENT (100% TrueAST)
// ============================================================================

// CaseStatement represents standalone CASE WHEN statement
type CaseStatement struct {
	WhenClauses []*CaseCondition // WHEN-THEN pairs
	ElseExpr    *Expression      // 100% TrueAST - ELSE value
	Alias       string           // Result column alias
}

// ============================================================================
// TRANSACTION (TCL)
// ============================================================================

// Transaction represents transaction control operations
type Transaction struct {
	Operation      string // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, etc.
	SavepointName  string
	IsolationLevel string // SERIALIZABLE, REPEATABLE READ, etc.
	ReadOnly       bool
}

// ============================================================================
// PERMISSION (DCL)
// ============================================================================

// Permission represents permission and user/role management
type Permission struct {
	Operation   string   // GRANT, REVOKE, CREATE USER, etc.
	Permissions []string // READ, WRITE, DELETE
	Target      string   // Target tenant_id or role_name
	RoleName    string
	UserName    string
	Password    string
	Roles       []string
}