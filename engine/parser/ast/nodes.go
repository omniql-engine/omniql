package ast

// Node is the interface all AST nodes implement
type Node interface {
	node()
	Pos() int
}

// QueryNode is the root node for any OQL query
type QueryNode struct {
	Operation   string
	Entity      string
	Position    int
	
	// CRUD
	Fields      []FieldNode
	Conditions  *WhereNode
	OrderBy     []OrderByNode
	Limit       *int
	Offset      *int
	Distinct    bool
	Columns     []string
	
	// CRUD extensions
	Upsert      *UpsertNode
	BulkData    [][]FieldNode
	
	// DDL
	DatabaseName string
	ViewName     string
	ViewQuery    string
	NewName      string
	
	// DQL
	Joins           []JoinNode
	Aggregate       *AggregateNode
	GroupBy         []string
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

// ConditionNode represents a single condition
type ConditionNode struct {
	Field      string
	Operator   string
	Value      string
	Value2     string           // For BETWEEN
	Values     []string         // For IN
	Logic      string           // AND, OR
	Nested     []ConditionNode  // For parentheses grouping
	Position   int
}

func (n *ConditionNode) node() {}
func (n *ConditionNode) Pos() int { return n.Position }

// FieldNode represents a field assignment
type FieldNode struct {
	Name        string
	Value       string
	Constraints []string         // DDL constraints
	Expression  *ExpressionNode  // For expressions
	Position    int
}

func (n *FieldNode) node() {}
func (n *FieldNode) Pos() int { return n.Position }

// ExpressionNode represents expressions (binary, function, case)
type ExpressionNode struct {
	Type         string // BINARY, FUNCTION, CASEWHEN
	LeftOperand  string
	Operator     string
	RightOperand string
	LeftIsField  bool
	RightIsField bool
	FunctionName string
	FunctionArgs []string
	CaseConditions []CaseConditionNode
	CaseElse     string
	Position     int
}

func (n *ExpressionNode) node() {}
func (n *ExpressionNode) Pos() int { return n.Position }

// CaseConditionNode represents WHEN-THEN pair
type CaseConditionNode struct {
	Condition string
	ThenValue string
	Position  int
}

func (n *CaseConditionNode) node() {}
func (n *CaseConditionNode) Pos() int { return n.Position }

// OrderByNode represents ORDER BY clause
type OrderByNode struct {
	Field      string
	Direction  string // ASC, DESC
	Expression *ExpressionNode
	Position   int
}

func (n *OrderByNode) node() {}
func (n *OrderByNode) Pos() int { return n.Position }

// JoinNode represents JOIN clause
type JoinNode struct {
	Type       string // INNER, LEFT, RIGHT, FULL, CROSS
	Table      string
	LeftField  string
	RightField string
	Position   int
}

func (n *JoinNode) node() {}
func (n *JoinNode) Pos() int { return n.Position }

// AggregateNode represents aggregate functions
type AggregateNode struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Field    string
	Position int
}

func (n *AggregateNode) node() {}
func (n *AggregateNode) Pos() int { return n.Position }

// WindowNode represents window functions
type WindowNode struct {
	Function    string // ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
	Field       string
	Alias       string
	PartitionBy []string
	OrderBy     []OrderByNode
	Offset      int
	Buckets     int
	Position    int
}

func (n *WindowNode) node() {}
func (n *WindowNode) Pos() int { return n.Position }

// UpsertNode represents UPSERT operation
type UpsertNode struct {
	ConflictFields []string
	UpdateFields   []FieldNode
	Position       int
}

func (n *UpsertNode) node() {}
func (n *UpsertNode) Pos() int { return n.Position }

// SetOperationNode represents UNION, INTERSECT, EXCEPT
type SetOperationNode struct {
	Type       string // UNION, UNION ALL, INTERSECT, EXCEPT
	LeftQuery  *QueryNode
	RightQuery *QueryNode
	Position   int
}

func (n *SetOperationNode) node() {}
func (n *SetOperationNode) Pos() int { return n.Position }

// TransactionNode represents TCL operations
type TransactionNode struct {
	Operation      string // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, etc.
	SavepointName  string
	IsolationLevel string
	ReadOnly       bool
	Position       int
}

func (n *TransactionNode) node() {}
func (n *TransactionNode) Pos() int { return n.Position }

// PermissionNode represents DCL operations
type PermissionNode struct {
	Operation   string   // GRANT, REVOKE, CREATE USER, etc.
	Permissions []string
	Target      string
	RoleName    string
	UserName    string
	Password    string
	Roles       []string
	Position    int
}

func (n *PermissionNode) node() {}
func (n *PermissionNode) Pos() int { return n.Position }