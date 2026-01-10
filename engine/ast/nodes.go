package ast

// Node is the interface all AST nodes implement
type Node interface {
	node()
	Pos() int
}

// QueryNode is the root node for any OQL query
type QueryNode struct {
	Operation   string           // Keyword: GET, CREATE, UPDATE, DELETE
	Entity      string           // Table/collection name
	Position    int
	
	// CRUD
	Fields      []FieldNode
	Conditions  *WhereNode
	OrderBy     []OrderByNode
	Limit       *int
	Offset      *int
	Distinct    bool
	Columns     []*ExpressionNode  // 100% TrueAST
	SelectColumns []SelectColumnNode
	
	// CRUD extensions
	Upsert      *UpsertNode
	BulkData    [][]FieldNode
	
	// DDL
	AlterAction  string         // ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, MODIFY_COLUMN
	DatabaseName string         // Name identifier
	ViewName     string         // Name identifier
	ViewQuery    *QueryNode     // 100% TrueAST - parsed subquery
	NewName      string         // Name identifier

	// PostgreSQL DDL
	SequenceName      string
	SequenceStart     int64
	SequenceIncrement int64
	SequenceMin       int64
	SequenceMax       int64
	SequenceCache     int64
	SequenceCycle     bool
	SequenceRestart   int64

	ExtensionName string

	SchemaName  string
	SchemaOwner string

	TypeName     string
	TypeKind     string   // ENUM, COMPOSITE
	EnumValues   []string
	EnumValue    string
	NewEnumValue string

	DomainName       string
	DomainType       string
	DomainDefault    string
	DomainConstraint string

	FuncName     string
	FuncBody     string
	FuncArgs     []string
	FuncReturns  string
	FuncLanguage string
	FuncOwner    string

	TriggerName    string
	TriggerTiming  string
	TriggerEvents  string
	TriggerForEach string

	PolicyName  string
	PolicyFor   string
	PolicyTo    string
	PolicyUsing string
	PolicyCheck string

	RuleName   string
	RuleEvent  string
	RuleAction string

	CommentTarget string
	CommentText   string

	Cascade bool
	
	// DQL
	Joins           []JoinNode
	Aggregate       *AggregateNode
	GroupBy         []*ExpressionNode  // 100% TrueAST
	Having          []ConditionNode
	WindowFunctions []WindowNode
	SetOperation    *SetOperationNode
	
	// TCL
	Transaction *TransactionNode
	
	// DCL
	Permission *PermissionNode
}

func (n *QueryNode) node() {}
func (n *QueryNode) Pos() int { return n.Position }

// WhereNode represents WHERE clause
type WhereNode struct {
	Conditions []ConditionNode
	Position   int
}

func (n *WhereNode) node() {}
func (n *WhereNode) Pos() int { return n.Position }

// ConditionNode represents a single condition (100% TrueAST)
type ConditionNode struct {
	FieldExpr  *ExpressionNode    // Left side
	Operator   string             // =, !=, >, <, IN, BETWEEN, etc.
	ValueExpr  *ExpressionNode    // Right side
	Value2Expr *ExpressionNode    // For BETWEEN second value
	ValuesExpr []*ExpressionNode  // For IN operator values
	Logic      string             // AND, OR
	Nested     []ConditionNode    // For parentheses grouping
	Position   int
}

func (n *ConditionNode) node() {}
func (n *ConditionNode) Pos() int { return n.Position }

// FieldNode represents a field assignment (100% TrueAST)
type FieldNode struct {
	NameExpr    *ExpressionNode  // 100% TrueAST - field name
	ValueExpr   *ExpressionNode  // 100% TrueAST - field value
	Constraints []string         // DDL keywords: UNIQUE, NOT_NULL, PRIMARY_KEY
	Position    int
}

func (n *FieldNode) node() {}
func (n *FieldNode) Pos() int { return n.Position }

// ExpressionNode represents expressions (100% TrueAST - recursive)
type ExpressionNode struct {
	Type     string  // BINARY, FUNCTION, CASEWHEN, FIELD, LITERAL, WINDOW
	Position int
	
	// For leaf nodes (FIELD, LITERAL)
	Value string
	
	// For BINARY expressions
	Left     *ExpressionNode
	Operator string
	Right    *ExpressionNode
	
	// For FUNCTION calls
	FunctionName string
	FunctionArgs []*ExpressionNode
	
	// For CASEWHEN
	CaseConditions []*CaseConditionNode
	CaseElse       *ExpressionNode
	
	// For WINDOW functions
	PartitionBy   []*ExpressionNode  // 100% TrueAST
	WindowOrderBy []OrderByNode      // Reuse OrderByNode
	WindowOffset  int                // For LAG/LEAD
	WindowBuckets int                // For NTILE
}

func (n *ExpressionNode) node() {}
func (n *ExpressionNode) Pos() int { return n.Position }

// CaseConditionNode represents WHEN-THEN pair (100% TrueAST)
type CaseConditionNode struct {
	Condition *ConditionNode   // 100% TrueAST - WHEN condition
	ThenExpr  *ExpressionNode  // 100% TrueAST - THEN value
	Position  int
}

func (n *CaseConditionNode) node() {}
func (n *CaseConditionNode) Pos() int { return n.Position }

// OrderByNode represents ORDER BY clause (100% TrueAST)
type OrderByNode struct {
	FieldExpr *ExpressionNode  // 100% TrueAST
	Direction string           // Keyword: ASC, DESC
	Position  int
}

func (n *OrderByNode) node() {}
func (n *OrderByNode) Pos() int { return n.Position }

// JoinNode represents JOIN clause (100% TrueAST)
type JoinNode struct {
	Type      string           // Keyword: INNER, LEFT, RIGHT, FULL, CROSS
	Table     string           // Table name identifier
	LeftExpr  *ExpressionNode  // 100% TrueAST
	RightExpr *ExpressionNode  // 100% TrueAST
	Position  int
}

func (n *JoinNode) node() {}
func (n *JoinNode) Pos() int { return n.Position }

// AggregateNode represents aggregate functions (100% TrueAST)
type AggregateNode struct {
	Function  string           // Keyword: COUNT, SUM, AVG, MIN, MAX
	FieldExpr *ExpressionNode  // 100% TrueAST
	Position  int
}

func (n *AggregateNode) node() {}
func (n *AggregateNode) Pos() int { return n.Position }

// WindowNode represents window functions (100% TrueAST)
type WindowNode struct {
	Function    string              // Keyword: ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
	FieldExpr   *ExpressionNode     // 100% TrueAST
	Alias       string              // Alias identifier
	PartitionBy []*ExpressionNode   // 100% TrueAST
	OrderBy     []OrderByNode
	Offset      int
	Buckets     int
	Position    int
}

func (n *WindowNode) node() {}
func (n *WindowNode) Pos() int { return n.Position }

// UpsertNode represents UPSERT operation (100% TrueAST)
type UpsertNode struct {
	ConflictFields []*ExpressionNode  // 100% TrueAST
	UpdateFields   []FieldNode
	Position       int
}

func (n *UpsertNode) node() {}
func (n *UpsertNode) Pos() int { return n.Position }

// SetOperationNode represents UNION, INTERSECT, EXCEPT
type SetOperationNode struct {
	Type       string  // Keyword: UNION, UNION ALL, INTERSECT, EXCEPT
	LeftQuery  *QueryNode
	RightQuery *QueryNode
	Position   int
}

func (n *SetOperationNode) node() {}
func (n *SetOperationNode) Pos() int { return n.Position }

// TransactionNode represents TCL operations
type TransactionNode struct {
	Operation      string  // Keyword: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
	SavepointName  string  // Name identifier
	IsolationLevel string  // Keyword: SERIALIZABLE, REPEATABLE READ, etc.
	ReadOnly       bool
	Position       int
}

func (n *TransactionNode) node() {}
func (n *TransactionNode) Pos() int { return n.Position }

// PermissionNode represents DCL operations
type PermissionNode struct {
	Operation   string    // Keyword: GRANT, REVOKE, CREATE USER
	Permissions []string  // Keywords: READ, WRITE, DELETE
	Target      string    // Name identifier
	RoleName    string    // Name identifier
	UserName    string    // Name identifier
	Password    string    // Literal value
	Roles       []string  // Name identifiers
	Position    int
}

func (n *PermissionNode) node() {}
func (n *PermissionNode) Pos() int { return n.Position }

// SelectColumnNode represents SELECT expression with alias (100% TrueAST)
type SelectColumnNode struct {
	ExpressionObj *ExpressionNode  // 100% TrueAST
	Alias         string           // Alias identifier
	Position      int
}

func (n *SelectColumnNode) node() {}
func (n *SelectColumnNode) Pos() int { return n.Position }