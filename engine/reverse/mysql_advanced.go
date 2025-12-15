package reverse

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
)

// ============================================================================
// DQL: SET OPERATIONS (UNION/INTERSECT/EXCEPT)
// ============================================================================

func convertMySQLSetOpr(stmt *ast.SetOprStmt) (*models.Query, error) {
	if len(stmt.SelectList.Selects) < 2 {
		return nil, fmt.Errorf("%w: set operation requires at least 2 queries", ErrParseError)
	}

	// Default to UNION
	var opType models.SetOperationType = models.Union

	// Check the second select for the operation type
	// In pingcap parser, AfterSetOperator is set on the second (and subsequent) selects
	if selStmt, ok := stmt.SelectList.Selects[1].(*ast.SelectStmt); ok {
		if selStmt.AfterSetOperator != nil {
			switch *selStmt.AfterSetOperator {
			case ast.Union:
				opType = models.Union
			case ast.UnionAll:
				opType = models.UnionAll
			case ast.Intersect, ast.IntersectAll:
				opType = models.Intersect
			case ast.Except, ast.ExceptAll:
				opType = models.Except
			}
		}
	}

	// Convert left and right queries
	var leftQuery, rightQuery *models.Query
	var err error

	if sel, ok := stmt.SelectList.Selects[0].(*ast.SelectStmt); ok {
		leftQuery, err = convertMySQLSelect(sel)
		if err != nil {
			return nil, fmt.Errorf("left query: %w", err)
		}
	}

	if sel, ok := stmt.SelectList.Selects[1].(*ast.SelectStmt); ok {
		rightQuery, err = convertMySQLSelect(sel)
		if err != nil {
			return nil, fmt.Errorf("right query: %w", err)
		}
	}

	// Handle case where queries are nil
	if leftQuery == nil || rightQuery == nil {
		return nil, fmt.Errorf("%w: could not parse set operation queries", ErrParseError)
	}

	return &models.Query{
		Operation: "GET",
		Entity:    leftQuery.Entity,
		SetOperation: &models.SetOperation{
			Type:       opType,
			LeftQuery:  leftQuery,
			RightQuery: rightQuery,
		},
	}, nil
}

// ============================================================================
// DQL: CTE (Common Table Expression) - WITH clause
// ============================================================================

func extractMySQLCTE(stmt *ast.SelectStmt) (*models.CTE, *models.Query, error) {
	if stmt.With == nil || len(stmt.With.CTEs) == 0 {
		return nil, nil, nil
	}

	cte := stmt.With.CTEs[0]
	result := &models.CTE{
		Name:      cte.Name.O,
		Recursive: stmt.With.IsRecursive,
	}

	// Extract CTE query
	if cte.Query != nil {
		if selStmt, ok := cte.Query.Query.(*ast.SelectStmt); ok {
			result.Query, _ = convertMySQLSelect(selStmt)
		}
	}

	// Create main query (without CTE to avoid recursion)
	mainQuery := &models.Query{Operation: "GET"}
	if stmt.From != nil {
		mainQuery.Entity = extractMySQLTableName(stmt.From.TableRefs)
	}

	return result, mainQuery, nil
}

// ============================================================================
// DQL: WINDOW FUNCTIONS
// ============================================================================

func extractMySQLWindowFunction(wfExpr *ast.WindowFuncExpr, alias string) models.WindowFunction {
	fn := strings.ToUpper(wfExpr.Name)
	wf := models.WindowFunction{
		Function: models.WindowFunc(fn),
		Alias:    alias,
	}

	// Arguments (for LAG, LEAD, etc.)
	if len(wfExpr.Args) > 0 {
		wf.FieldExpr = mysqlExprToExpression(wfExpr.Args[0])
		// Check for offset (second arg)
		if len(wfExpr.Args) > 1 {
			if valExpr, ok := wfExpr.Args[1].(*test_driver.ValueExpr); ok {
				wf.Offset = int(valExpr.GetInt64())
			}
		}
	}

	// NTILE buckets
	if fn == "NTILE" && len(wfExpr.Args) > 0 {
		if valExpr, ok := wfExpr.Args[0].(*test_driver.ValueExpr); ok {
			wf.Buckets = int(valExpr.GetInt64())
		}
	}

	// OVER clause - WindowSpec is a value type, check if it has content
	// PARTITION BY
	if wfExpr.Spec.PartitionBy != nil {
		for _, item := range wfExpr.Spec.PartitionBy.Items {
			wf.PartitionBy = append(wf.PartitionBy, mysqlExprToExpression(item.Expr))
		}
	}

	// ORDER BY
	if wfExpr.Spec.OrderBy != nil {
		for _, item := range wfExpr.Spec.OrderBy.Items {
			dir := models.Asc
			if item.Desc {
				dir = models.Desc
			}
			wf.OrderBy = append(wf.OrderBy, models.OrderBy{
				FieldExpr: mysqlExprToExpression(item.Expr),
				Direction: dir,
			})
		}
	}

	return wf
}

// ============================================================================
// DQL: CASE expression
// ============================================================================

func mysqlCaseToExpression(expr *ast.CaseExpr) *models.Expression {
	caseExpr := &models.Expression{Type: "CASEWHEN"}

	// WHEN ... THEN clauses
	for _, when := range expr.WhenClauses {
		// Convert WHEN condition
		var cond *models.Condition
		if when.Expr != nil {
			conds, _ := mysqlExprToConditions(when.Expr)
			if len(conds) > 0 {
				cond = &conds[0]
			}
		}

		// Convert THEN result
		thenExpr := mysqlExprToExpression(when.Result)

		caseExpr.CaseConditions = append(caseExpr.CaseConditions, &models.CaseCondition{
			Condition: cond,
			ThenExpr:  thenExpr,
		})
	}

	// ELSE clause
	if expr.ElseClause != nil {
		caseExpr.CaseElse = mysqlExprToExpression(expr.ElseClause)
	}

	return caseExpr
}

// ============================================================================
// DQL: SUBQUERY handling
// ============================================================================

func mysqlSubqueryToExpression(subq *ast.SubqueryExpr) *models.Expression {
	if subq == nil || subq.Query == nil {
		return nil
	}

	expr := &models.Expression{Type: "SUBQUERY"}

	// Check for EXISTS
	if subq.Exists {
		expr.Value = "EXISTS"
	} else {
		expr.Value = "SUBQUERY"
	}

	return expr
}

// ============================================================================
// MySQL-specific: LOCK TABLES / UNLOCK TABLES
// ============================================================================

func convertMySQLLockTables(tables []string, lockType string) *models.Query {
	return &models.Query{
		Operation: "LOCK TABLES",
		Transaction: &models.Transaction{
			Operation: "LOCK TABLES",
		},
	}
}

func convertMySQLUnlockTables() *models.Query {
	return &models.Query{
		Operation: "UNLOCK TABLES",
		Transaction: &models.Transaction{
			Operation: "UNLOCK TABLES",
		},
	}
}

// ============================================================================
// MySQL-specific: USE database
// ============================================================================

func convertMySQLUse(dbName string) *models.Query {
	return &models.Query{
		Operation:    "USE",
		DatabaseName: dbName,
	}
}

// ============================================================================
// MySQL-specific: SET variable
// ============================================================================

func convertMySQLSet(variable string, value string) *models.Query {
	return &models.Query{
		Operation: "SET",
		Fields: []models.Field{{
			NameExpr:  FieldExpr(variable),
			ValueExpr: LiteralExpr(value),
		}},
	}
}

// ============================================================================
// TCL: SET TRANSACTION
// ============================================================================

func convertMySQLSetTransaction(isolationLevel string, readOnly bool) *models.Query {
	return &models.Query{
		Operation: "SET TRANSACTION",
		Transaction: &models.Transaction{
			Operation:      "SET TRANSACTION",
			IsolationLevel: isolationLevel,
			ReadOnly:       readOnly,
		},
	}
}

// ============================================================================
// DDL: CREATE/DROP TRIGGER (MySQL specific)
// ============================================================================

func convertMySQLCreateTrigger(tableName, triggerName string) *models.Query {
	return &models.Query{
		Operation: "CREATE TRIGGER",
		Entity:    TableToEntity(tableName),
		NewName:   triggerName,
	}
}

func convertMySQLDropTrigger(triggerName string) *models.Query {
	return &models.Query{
		Operation: "DROP TRIGGER",
		NewName:   triggerName,
	}
}

// ============================================================================
// DDL: CREATE/DROP FUNCTION (MySQL specific)
// ============================================================================

func convertMySQLCreateFunction(funcName string) *models.Query {
	return &models.Query{
		Operation: "CREATE FUNCTION",
		NewName:   funcName,
	}
}

func convertMySQLDropFunction(funcName string) *models.Query {
	return &models.Query{
		Operation: "DROP FUNCTION",
		NewName:   funcName,
	}
}

// ============================================================================
// DDL: DROP VIEW
// ============================================================================

func convertMySQLDropView(stmt *ast.DropTableStmt) (*models.Query, error) {
	// Note: In pingcap parser, DROP VIEW uses DropTableStmt with IsView=true
	var viewName string
	if len(stmt.Tables) > 0 {
		viewName = stmt.Tables[0].Name.O
	}
	return &models.Query{
		Operation: "DROP VIEW",
		ViewName:  viewName,
	}, nil
}

// ============================================================================
// DQL: Additional aggregate functions
// ============================================================================

func mysqlAggregateFuncToAggregation(aggExpr *ast.AggregateFuncExpr) *models.Aggregation {
	agg := &models.Aggregation{
		Function: models.AggregateFunc(strings.ToUpper(aggExpr.F)),
	}

	if len(aggExpr.Args) > 0 {
		agg.FieldExpr = mysqlExprToExpression(aggExpr.Args[0])
	} else {
		agg.FieldExpr = FieldExpr("*")
	}

	return agg
}

// ============================================================================
// DQL: Enhanced Window Functions
// ============================================================================

func extractMySQLWindowFunctionAdvanced(wfExpr *ast.WindowFuncExpr, alias string) models.WindowFunction {
	wf := models.WindowFunction{
		Function: models.WindowFunc(strings.ToUpper(wfExpr.Name)),
		Alias:    alias,
	}

	// Arguments
	if len(wfExpr.Args) > 0 {
		wf.FieldExpr = mysqlExprToExpression(wfExpr.Args[0])
	}

	// OVER clause - WindowSpec is a value type, check fields directly
	// PARTITION BY
	if wfExpr.Spec.PartitionBy != nil {
		for _, item := range wfExpr.Spec.PartitionBy.Items {
			wf.PartitionBy = append(wf.PartitionBy, mysqlExprToExpression(item.Expr))
		}
	}

	// ORDER BY
	if wfExpr.Spec.OrderBy != nil {
		for _, item := range wfExpr.Spec.OrderBy.Items {
			dir := models.Asc
			if item.Desc {
				dir = models.Desc
			}
			wf.OrderBy = append(wf.OrderBy, models.OrderBy{
				FieldExpr: mysqlExprToExpression(item.Expr),
				Direction: dir,
			})
		}
	}

	return wf
}