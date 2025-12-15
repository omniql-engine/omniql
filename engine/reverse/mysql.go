package reverse

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
)

// ============================================================================
// ENTRY POINT
// ============================================================================

func MySQLToQuery(sql string) (*models.Query, error) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseError, err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("%w: empty statement", ErrParseError)
	}

	switch stmt := stmts[0].(type) {
	// ==================== CRUD ====================
	case *ast.SelectStmt:
		return convertMySQLSelect(stmt)
	case *ast.SetOprStmt:
		return convertMySQLSetOpr(stmt)
	case *ast.InsertStmt:
		return convertMySQLInsert(stmt)
	case *ast.UpdateStmt:
		return convertMySQLUpdate(stmt)
	case *ast.DeleteStmt:
		return convertMySQLDelete(stmt)

	// ==================== DDL ====================
	case *ast.CreateTableStmt:
		return convertMySQLCreateTable(stmt)
	case *ast.DropTableStmt:
		return convertMySQLDropTable(stmt)
	case *ast.AlterTableStmt:
		return convertMySQLAlterTable(stmt)
	case *ast.TruncateTableStmt:
		return convertMySQLTruncate(stmt)
	case *ast.RenameTableStmt:
		return convertMySQLRenameTable(stmt)
	case *ast.CreateIndexStmt:
		return convertMySQLCreateIndex(stmt)
	case *ast.DropIndexStmt:
		return convertMySQLDropIndex(stmt)
	case *ast.CreateViewStmt:
		return convertMySQLCreateView(stmt)
	case *ast.CreateDatabaseStmt:
		return convertMySQLCreateDatabase(stmt)
	case *ast.DropDatabaseStmt:
		return convertMySQLDropDatabase(stmt)

	// ==================== TCL ====================
	case *ast.SetStmt:
		return convertMySQLSetStatement(stmt)
	case *ast.BeginStmt:
		return &models.Query{Operation: "BEGIN", Transaction: &models.Transaction{Operation: "BEGIN"}}, nil
	case *ast.CommitStmt:
		return &models.Query{Operation: "COMMIT", Transaction: &models.Transaction{Operation: "COMMIT"}}, nil
	case *ast.RollbackStmt:
		if stmt.SavepointName != "" {
			return &models.Query{Operation: "ROLLBACK TO", Transaction: &models.Transaction{Operation: "ROLLBACK TO", SavepointName: stmt.SavepointName}}, nil
		}
		return &models.Query{Operation: "ROLLBACK", Transaction: &models.Transaction{Operation: "ROLLBACK"}}, nil
	case *ast.SavepointStmt:
		return &models.Query{Operation: "SAVEPOINT", Transaction: &models.Transaction{Operation: "SAVEPOINT", SavepointName: stmt.Name}}, nil
	case *ast.ReleaseSavepointStmt:
		return &models.Query{Operation: "RELEASE SAVEPOINT", Transaction: &models.Transaction{Operation: "RELEASE SAVEPOINT", SavepointName: stmt.Name}}, nil

	// ==================== DCL ====================
	case *ast.GrantStmt:
		return convertMySQLGrant(stmt)
	case *ast.RevokeStmt:
		return convertMySQLRevoke(stmt)
	case *ast.CreateUserStmt:
		return convertMySQLCreateUser(stmt)
	case *ast.DropUserStmt:
		return convertMySQLDropUser(stmt)
	case *ast.AlterUserStmt:
		return convertMySQLAlterUser(stmt)
	case *ast.GrantRoleStmt:
		return convertMySQLAssignRole(stmt)
	case *ast.RevokeRoleStmt:
		return convertMySQLRevokeRole(stmt)

	default:
		return nil, fmt.Errorf("%w: unsupported MySQL statement type %T", ErrNotSupported, stmts[0])
	}
}

// ============================================================================
// CRUD: SELECT → GET
// ============================================================================

func convertMySQLSelect(stmt *ast.SelectStmt) (*models.Query, error) {
	query := &models.Query{Operation: "GET"}

	// Extract table name (entity)
	if stmt.From != nil {
		query.Entity = extractMySQLTableName(stmt.From.TableRefs)
	}

	// Handle CTE (WITH clause)
	if stmt.With != nil {
		cte, mainQuery, err := extractMySQLCTE(stmt)
		if err != nil {
			return nil, err
		}
		if cte != nil {
			query.CTE = cte
			query.CTE.MainQuery = mainQuery
		}
	}

	// Extract columns, aggregates, and window functions
	if stmt.Fields != nil {
		cols, agg, wfs := extractMySQLFields(stmt.Fields.Fields)
		if len(cols) > 0 {
			query.Columns = cols
		}
		if agg != nil {
			query.Operation = string(agg.Function)
			query.Aggregate = agg
		}
		if len(wfs) > 0 {
			query.WindowFunctions = wfs
		}

		// Check for column aliases (SelectColumns)
		selectCols := extractMySQLSelectColumns(stmt.Fields.Fields)
		if len(selectCols) > 0 {
			query.SelectColumns = selectCols
		}
	}

	// Extract conditions
	if stmt.Where != nil {
		conds, err := mysqlExprToConditions(stmt.Where)
		if err != nil {
			return nil, err
		}
		query.Conditions = conds
	}

	// Extract JOINs
	if stmt.From != nil {
		query.Joins = extractMySQLJoins(stmt.From.TableRefs)
	}

	// ORDER BY
	if stmt.OrderBy != nil {
		for _, item := range stmt.OrderBy.Items {
			dir := models.Asc
			if item.Desc {
				dir = models.Desc
			}
			query.OrderBy = append(query.OrderBy, models.OrderBy{
				FieldExpr: mysqlExprToExpression(item.Expr),
				Direction: dir,
			})
		}
	}

	// LIMIT & OFFSET
	if stmt.Limit != nil {
		if stmt.Limit.Count != nil {
			if val, ok := stmt.Limit.Count.(*test_driver.ValueExpr); ok {
				query.Limit = int(val.GetInt64())
			}
		}
		if stmt.Limit.Offset != nil {
			if val, ok := stmt.Limit.Offset.(*test_driver.ValueExpr); ok {
				query.Offset = int(val.GetInt64())
			}
		}
	}

	// DISTINCT
	if stmt.Distinct {
		query.Distinct = true
	}

	// GROUP BY
	if stmt.GroupBy != nil {
		for _, item := range stmt.GroupBy.Items {
			query.GroupBy = append(query.GroupBy, mysqlExprToExpression(item.Expr))
		}
	}

	// HAVING
	if stmt.Having != nil {
		conds, err := mysqlExprToConditions(stmt.Having.Expr)
		if err != nil {
			return nil, err
		}
		query.Having = conds
	}

	return query, nil
}

// ============================================================================
// CRUD: INSERT → CREATE / BULK INSERT / UPSERT / REPLACE
// ============================================================================

func convertMySQLInsert(stmt *ast.InsertStmt) (*models.Query, error) {
	entity := extractMySQLTableName(stmt.Table.TableRefs)

	// Extract column names
	var columns []string
	for _, col := range stmt.Columns {
		columns = append(columns, col.Name.O)
	}

	// Extract value lists
	var lists [][]ast.ExprNode
	for _, list := range stmt.Lists {
		lists = append(lists, list)
	}

	// Check for REPLACE
	if stmt.IsReplace {
		return convertMySQLReplace(entity, columns, lists)
	}

	// Check for ON DUPLICATE KEY UPDATE (UPSERT)
	if len(stmt.OnDuplicate) > 0 {
		return convertMySQLUpsert(entity, columns, lists, stmt.OnDuplicate)
	}

	// Single row INSERT → CREATE
	if len(lists) == 1 {
		fields := buildMySQLFieldsFromLists(columns, lists[0])
		return &models.Query{
			Operation: "CREATE",
			Entity:    entity,
			Fields:    fields,
		}, nil
	}

	// Multiple rows → BULK INSERT
	var bulkData [][]models.Field
	for _, list := range lists {
		bulkData = append(bulkData, buildMySQLFieldsFromLists(columns, list))
	}
	return &models.Query{
		Operation: "BULK INSERT",
		Entity:    entity,
		BulkData:  bulkData,
	}, nil
}

func convertMySQLReplace(entity string, columns []string, lists [][]ast.ExprNode) (*models.Query, error) {
	if len(lists) == 0 {
		return nil, fmt.Errorf("%w: REPLACE without values", ErrParseError)
	}
	fields := buildMySQLFieldsFromLists(columns, lists[0])
	return &models.Query{
		Operation: "REPLACE",
		Entity:    entity,
		Fields:    fields,
	}, nil
}

func convertMySQLUpsert(entity string, columns []string, lists [][]ast.ExprNode, onDup []*ast.Assignment) (*models.Query, error) {
	if len(lists) == 0 {
		return nil, fmt.Errorf("%w: UPSERT without values", ErrParseError)
	}

	fields := buildMySQLFieldsFromLists(columns, lists[0])

	var updateFields []models.Field
	for _, assign := range onDup {
		valueExpr := mysqlExprToExpression(assign.Expr)

		// Handle VALUES(column) function - extract just the column name
		if funcExpr, ok := assign.Expr.(*ast.FuncCallExpr); ok {
			if strings.ToUpper(funcExpr.FnName.O) == "VALUES" && len(funcExpr.Args) > 0 {
				if colExpr, ok := funcExpr.Args[0].(*ast.ColumnNameExpr); ok {
					valueExpr = LiteralExpr(colExpr.Name.Name.O)
				}
			}
		}
		// Also handle ValuesExpr (pingcap specific for VALUES())
		if valuesExpr, ok := assign.Expr.(*ast.ValuesExpr); ok {
			if valuesExpr.Column != nil {
				valueExpr = LiteralExpr(valuesExpr.Column.Name.Name.O)
			}
		}

		updateFields = append(updateFields, models.Field{
			NameExpr:  FieldExpr(assign.Column.Name.O),
			ValueExpr: valueExpr,
		})
	}

	return &models.Query{
		Operation: "UPSERT",
		Entity:    entity,
		Fields:    fields,
		Upsert:    &models.Upsert{UpdateFields: updateFields},
	}, nil
}

func buildMySQLFieldsFromLists(columns []string, values []ast.ExprNode) []models.Field {
	var fields []models.Field
	for i, val := range values {
		var name string
		if i < len(columns) {
			name = columns[i]
		}
		fields = append(fields, models.Field{
			NameExpr:  FieldExpr(name),
			ValueExpr: mysqlExprToExpression(val),
		})
	}
	return fields
}

// ============================================================================
// CRUD: UPDATE
// ============================================================================

func convertMySQLUpdate(stmt *ast.UpdateStmt) (*models.Query, error) {
	entity := extractMySQLTableName(stmt.TableRefs.TableRefs)

	var fields []models.Field
	for _, assign := range stmt.List {
		fields = append(fields, models.Field{
			NameExpr:  FieldExpr(assign.Column.Name.O),
			ValueExpr: mysqlExprToExpression(assign.Expr),
		})
	}

	query := &models.Query{
		Operation: "UPDATE",
		Entity:    entity,
		Fields:    fields,
	}

	if stmt.Where != nil {
		conds, err := mysqlExprToConditions(stmt.Where)
		if err != nil {
			return nil, err
		}
		query.Conditions = conds
	}

	return query, nil
}

// ============================================================================
// CRUD: DELETE
// ============================================================================

func convertMySQLDelete(stmt *ast.DeleteStmt) (*models.Query, error) {
	entity := extractMySQLTableName(stmt.TableRefs.TableRefs)

	query := &models.Query{
		Operation: "DELETE",
		Entity:    entity,
	}

	if stmt.Where != nil {
		conds, err := mysqlExprToConditions(stmt.Where)
		if err != nil {
			return nil, err
		}
		query.Conditions = conds
	}

	return query, nil
}

// ============================================================================
// DDL: TABLE OPERATIONS
// ============================================================================

func convertMySQLCreateTable(stmt *ast.CreateTableStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "CREATE TABLE",
		Entity:    TableToEntity(stmt.Table.Name.O),
	}, nil
}

func convertMySQLDropTable(stmt *ast.DropTableStmt) (*models.Query, error) {
	var entity string
	if len(stmt.Tables) > 0 {
		entity = TableToEntity(stmt.Tables[0].Name.O)
	}

	// Check if it's DROP VIEW (pingcap uses DropTableStmt for views too)
	if stmt.IsView {
		return &models.Query{
			Operation: "DROP VIEW",
			Entity:    entity,
		}, nil
	}

	return &models.Query{
		Operation: "DROP TABLE",
		Entity:    entity,
	}, nil
}

func convertMySQLAlterTable(stmt *ast.AlterTableStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "ALTER TABLE",
		Entity:    TableToEntity(stmt.Table.Name.O),
	}, nil
}

func convertMySQLTruncate(stmt *ast.TruncateTableStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "TRUNCATE TABLE",
		Entity:    TableToEntity(stmt.Table.Name.O),
	}, nil
}

func convertMySQLRenameTable(stmt *ast.RenameTableStmt) (*models.Query, error) {
	var entity, newName string
	if len(stmt.TableToTables) > 0 {
		entity = TableToEntity(stmt.TableToTables[0].OldTable.Name.O)
		newName = stmt.TableToTables[0].NewTable.Name.O
	}
	return &models.Query{
		Operation: "RENAME TABLE",
		Entity:    entity,
		NewName:   newName,
	}, nil
}

// ============================================================================
// DDL: INDEX OPERATIONS
// ============================================================================

func convertMySQLCreateIndex(stmt *ast.CreateIndexStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "CREATE INDEX",
		Entity:    TableToEntity(stmt.Table.Name.O),
		NewName:   stmt.IndexName,
	}, nil
}

func convertMySQLDropIndex(stmt *ast.DropIndexStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "DROP INDEX",
		Entity:    TableToEntity(stmt.Table.Name.O),
		NewName:   stmt.IndexName,
	}, nil
}

// ============================================================================
// DDL: VIEW OPERATIONS
// ============================================================================

func convertMySQLCreateView(stmt *ast.CreateViewStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "CREATE VIEW",
		ViewName:  stmt.ViewName.Name.O,
	}, nil
}

func convertMySQLAlterView(stmt *ast.AlterTableStmt) (*models.Query, error) {
	return &models.Query{
		Operation: "ALTER VIEW",
		ViewName:  stmt.Table.Name.O,
	}, nil
}

// ============================================================================
// DDL: DATABASE OPERATIONS
// ============================================================================

func convertMySQLCreateDatabase(stmt *ast.CreateDatabaseStmt) (*models.Query, error) {
	return &models.Query{
		Operation:    "CREATE DATABASE",
		DatabaseName: stmt.Name.O,
	}, nil
}

func convertMySQLDropDatabase(stmt *ast.DropDatabaseStmt) (*models.Query, error) {
	return &models.Query{
		Operation:    "DROP DATABASE",
		DatabaseName: stmt.Name.O,
	}, nil
}

// ============================================================================
// TCL: SET TRANSACTION
// ============================================================================

func convertMySQLSetStatement(stmt *ast.SetStmt) (*models.Query, error) {
	// Check if this is a SET TRANSACTION statement
	for _, v := range stmt.Variables {
		if v.Name == "transaction_isolation" || v.Name == "tx_isolation" {
			var level string
			if v.Value != nil {
				if val, ok := v.Value.(*test_driver.ValueExpr); ok {
					level = val.Datum.GetString()
				}
			}
			return &models.Query{
				Operation: "SET TRANSACTION",
				Transaction: &models.Transaction{
					Operation:      "SET TRANSACTION",
					IsolationLevel: level,
				},
			}, nil
		}
		if v.Name == "transaction_read_only" || v.Name == "tx_read_only" {
			return &models.Query{
				Operation: "SET TRANSACTION",
				Transaction: &models.Transaction{
					Operation: "SET TRANSACTION",
					ReadOnly:  true,
				},
			}, nil
		}
	}
	
	// Generic SET statement - return as unsupported or handle as needed
	return nil, fmt.Errorf("%w: unsupported SET statement", ErrNotSupported)
}

// ============================================================================
// DCL: PERMISSIONS
// ============================================================================

func convertMySQLGrant(stmt *ast.GrantStmt) (*models.Query, error) {
	var perms []string
	for _, priv := range stmt.Privs {
		perms = append(perms, priv.Priv.String())
	}

	var target string
	if len(stmt.Users) > 0 {
		target = stmt.Users[0].User.Username
	}

	var entity string
	if stmt.Level != nil && stmt.Level.TableName != "" {
		entity = TableToEntity(stmt.Level.TableName)
	}

	return &models.Query{
		Operation: "GRANT",
		Entity:    entity,
		Permission: &models.Permission{
			Operation:   "GRANT",
			Permissions: perms,
			Target:      target,
		},
	}, nil
}

func convertMySQLRevoke(stmt *ast.RevokeStmt) (*models.Query, error) {
	var perms []string
	for _, priv := range stmt.Privs {
		perms = append(perms, priv.Priv.String())
	}

	var target string
	if len(stmt.Users) > 0 {
		target = stmt.Users[0].User.Username
	}

	var entity string
	if stmt.Level != nil && stmt.Level.TableName != "" {
		entity = TableToEntity(stmt.Level.TableName)
	}

	return &models.Query{
		Operation: "REVOKE",
		Entity:    entity,
		Permission: &models.Permission{
			Operation:   "REVOKE",
			Permissions: perms,
			Target:      target,
		},
	}, nil
}

// ============================================================================
// DCL: USER MANAGEMENT
// ============================================================================

func convertMySQLCreateUser(stmt *ast.CreateUserStmt) (*models.Query, error) {
	var username string
	if len(stmt.Specs) > 0 {
		username = stmt.Specs[0].User.Username
	}
	return &models.Query{
		Operation:  "CREATE USER",
		Permission: &models.Permission{Operation: "CREATE USER", Target: username},
	}, nil
}

func convertMySQLDropUser(stmt *ast.DropUserStmt) (*models.Query, error) {
	var username string
	if len(stmt.UserList) > 0 {
		username = stmt.UserList[0].Username
	}
	return &models.Query{
		Operation:  "DROP USER",
		Permission: &models.Permission{Operation: "DROP USER", Target: username},
	}, nil
}

func convertMySQLAlterUser(stmt *ast.AlterUserStmt) (*models.Query, error) {
	var username string
	if len(stmt.Specs) > 0 {
		username = stmt.Specs[0].User.Username
	}
	return &models.Query{
		Operation:  "ALTER USER",
		Permission: &models.Permission{Operation: "ALTER USER", Target: username},
	}, nil
}

// ============================================================================
// DCL: ROLE MANAGEMENT
// ============================================================================

func convertMySQLAssignRole(stmt *ast.GrantRoleStmt) (*models.Query, error) {
	var roleName, username string
	if len(stmt.Roles) > 0 {
		roleName = stmt.Roles[0].Username
	}
	if len(stmt.Users) > 0 {
		username = stmt.Users[0].Username
	}
	return &models.Query{
		Operation:  "ASSIGN ROLE",
		Permission: &models.Permission{Operation: "ASSIGN ROLE", Target: username, RoleName: roleName},
	}, nil
}

func convertMySQLRevokeRole(stmt *ast.RevokeRoleStmt) (*models.Query, error) {
	var roleName, username string
	if len(stmt.Roles) > 0 {
		roleName = stmt.Roles[0].Username
	}
	if len(stmt.Users) > 0 {
		username = stmt.Users[0].Username
	}
	return &models.Query{
		Operation:  "REVOKE ROLE",
		Permission: &models.Permission{Operation: "REVOKE ROLE", Target: username, RoleName: roleName},
	}, nil
}

// Note: CREATE ROLE and DROP ROLE are not supported by pingcap parser
// MySQL 8.0+ supports them but the parser doesn't expose them as separate statement types

// ============================================================================
// EXPRESSION CONVERSION
// ============================================================================

func mysqlExprToExpression(expr ast.ExprNode) *models.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		return FieldExpr(e.Name.Name.O)

	case *test_driver.ValueExpr:
		return LiteralExpr(formatMySQLValue(e))

	case *ast.BinaryOperationExpr:
		return BinaryExpr(
			mysqlExprToExpression(e.L),
			mysqlOpToString(e.Op),
			mysqlExprToExpression(e.R),
		)

	case *ast.FuncCallExpr:
		var args []*models.Expression
		for _, arg := range e.Args {
			args = append(args, mysqlExprToExpression(arg))
		}
		return FunctionExpr(e.FnName.O, args...)

	case *ast.AggregateFuncExpr:
		var args []*models.Expression
		for _, arg := range e.Args {
			args = append(args, mysqlExprToExpression(arg))
		}
		return FunctionExpr(e.F, args...)

	case *ast.ParenthesesExpr:
		return mysqlExprToExpression(e.Expr)

	case *ast.UnaryOperationExpr:
		// Handle negative numbers
		if e.Op == opcode.Minus {
			innerExpr := mysqlExprToExpression(e.V)
			if innerExpr != nil && innerExpr.Type == "LITERAL" {
				return LiteralExpr("-" + innerExpr.Value)
			}
		}
		return mysqlExprToExpression(e.V)

	case *ast.IsNullExpr:
		return mysqlExprToExpression(e.Expr)

	case *ast.IsTruthExpr:
		return mysqlExprToExpression(e.Expr)

	case *ast.CaseExpr:
		return mysqlCaseToExpression(e)

	case *ast.SubqueryExpr:
		return mysqlSubqueryToExpression(e)

	default:
		return LiteralExpr(fmt.Sprintf("%v", expr))
	}
}

func mysqlOpToString(op opcode.Op) string {
	switch op {
	case opcode.Plus:
		return "+"
	case opcode.Minus:
		return "-"
	case opcode.Mul:
		return "*"
	case opcode.Div:
		return "/"
	case opcode.Mod:
		return "%"
	case opcode.EQ:
		return "="
	case opcode.NE:
		return "!="
	case opcode.LT:
		return "<"
	case opcode.GT:
		return ">"
	case opcode.LE:
		return "<="
	case opcode.GE:
		return ">="
	case opcode.LogicAnd:
		return "AND"
	case opcode.LogicOr:
		return "OR"
	case opcode.LogicXor:
		return "XOR"
	default:
		return op.String()
	}
}

func formatMySQLValue(val *test_driver.ValueExpr) string {
	d := val.Datum
	switch d.Kind() {
	case test_driver.KindInt64:
		return fmt.Sprintf("%d", d.GetInt64())
	case test_driver.KindUint64:
		return fmt.Sprintf("%d", d.GetUint64())
	case test_driver.KindFloat64:
		return fmt.Sprintf("%v", d.GetFloat64())
	case test_driver.KindString:
		return d.GetString()
	case test_driver.KindBytes:
		return string(d.GetBytes())
	default:
		return fmt.Sprintf("%v", d.GetValue())
	}
}

// ============================================================================
// CONDITIONS - ALL 15 OPERATORS
// ============================================================================

func mysqlExprToConditions(expr ast.ExprNode) ([]models.Condition, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch e.Op {
		case opcode.LogicAnd:
			left, err := mysqlExprToConditions(e.L)
			if err != nil {
				return nil, err
			}
			right, err := mysqlExprToConditions(e.R)
			if err != nil {
				return nil, err
			}
			if len(right) > 0 {
				right[0].Logic = "AND"
			}
			return append(left, right...), nil

		case opcode.LogicOr:
			left, err := mysqlExprToConditions(e.L)
			if err != nil {
				return nil, err
			}
			right, err := mysqlExprToConditions(e.R)
			if err != nil {
				return nil, err
			}
			if len(right) > 0 {
				right[0].Logic = "OR"
			}
			return append(left, right...), nil

		default:
			cond := buildMySQLCondition(e)
			return []models.Condition{cond}, nil
		}

	case *ast.PatternInExpr:
		cond := buildMySQLInCondition(e)
		return []models.Condition{cond}, nil

	case *ast.PatternLikeOrIlikeExpr:
		cond := buildMySQLLikeCondition(e)
		return []models.Condition{cond}, nil

	case *ast.BetweenExpr:
		cond := buildMySQLBetweenCondition(e)
		return []models.Condition{cond}, nil

	case *ast.IsNullExpr:
		cond := buildMySQLNullCondition(e)
		return []models.Condition{cond}, nil

	case *ast.IsTruthExpr:
		// Handle IS TRUE / IS FALSE
		// e.True is int64: 1 for TRUE, 0 for FALSE
		op := "="
		val := "1"
		if e.True == 0 {
			val = "0"
		}
		return []models.Condition{{
			FieldExpr: mysqlExprToExpression(e.Expr),
			Operator:  op,
			ValueExpr: LiteralExpr(val),
		}}, nil

	case *ast.ParenthesesExpr:
		return mysqlExprToConditions(e.Expr)

	default:
		return nil, fmt.Errorf("%w: unsupported condition type %T", ErrNotSupported, expr)
	}
}

func buildMySQLCondition(e *ast.BinaryOperationExpr) models.Condition {
	cond := models.Condition{
		FieldExpr: mysqlExprToExpression(e.L),
		ValueExpr: mysqlExprToExpression(e.R),
	}

	switch e.Op {
	case opcode.EQ:
		cond.Operator = "="
	case opcode.NE:
		cond.Operator = "!="
	case opcode.LT:
		cond.Operator = "<"
	case opcode.GT:
		cond.Operator = ">"
	case opcode.LE:
		cond.Operator = "<="
	case opcode.GE:
		cond.Operator = ">="
	default:
		cond.Operator = mysqlOpToString(e.Op)
	}

	return cond
}

func buildMySQLInCondition(e *ast.PatternInExpr) models.Condition {
	cond := models.Condition{
		FieldExpr: mysqlExprToExpression(e.Expr),
		Operator:  "IN",
	}
	if e.Not {
		cond.Operator = "NOT_IN"
	}

	for _, val := range e.List {
		cond.ValuesExpr = append(cond.ValuesExpr, mysqlExprToExpression(val))
	}

	return cond
}

func buildMySQLLikeCondition(e *ast.PatternLikeOrIlikeExpr) models.Condition {
	cond := models.Condition{
		FieldExpr: mysqlExprToExpression(e.Expr),
		Operator:  "LIKE",
		ValueExpr: mysqlExprToExpression(e.Pattern),
	}
	if e.Not {
		cond.Operator = "NOT_LIKE"
	}
	return cond
}

func buildMySQLBetweenCondition(e *ast.BetweenExpr) models.Condition {
	cond := models.Condition{
		FieldExpr:  mysqlExprToExpression(e.Expr),
		Operator:   "BETWEEN",
		ValueExpr:  mysqlExprToExpression(e.Left),
		Value2Expr: mysqlExprToExpression(e.Right),
	}
	if e.Not {
		cond.Operator = "NOT_BETWEEN"
	}
	return cond
}

func buildMySQLNullCondition(e *ast.IsNullExpr) models.Condition {
	cond := models.Condition{
		FieldExpr: mysqlExprToExpression(e.Expr),
		Operator:  "IS_NULL",
	}
	if e.Not {
		cond.Operator = "IS_NOT_NULL"
	}
	return cond
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func extractMySQLTableName(refs *ast.Join) string {
	if refs == nil {
		return ""
	}
	if refs.Left != nil {
		if ts, ok := refs.Left.(*ast.TableSource); ok {
			if tn, ok := ts.Source.(*ast.TableName); ok {
				return TableToEntity(tn.Name.O)
			}
		}
		if join, ok := refs.Left.(*ast.Join); ok {
			return extractMySQLTableName(join)
		}
	}
	return ""
}

func extractMySQLFields(fields []*ast.SelectField) ([]*models.Expression, *models.Aggregation, []models.WindowFunction) {
	var cols []*models.Expression
	var agg *models.Aggregation
	var wfs []models.WindowFunction

	for _, f := range fields {
		if f.WildCard != nil {
			continue // SELECT *
		}

		// Check for aggregate function
		if aggExpr, ok := f.Expr.(*ast.AggregateFuncExpr); ok {
			fn := strings.ToUpper(aggExpr.F)
			if fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX" {
				agg = &models.Aggregation{Function: models.AggregateFunc(fn)}
				// Handle COUNT(*) - check for star or empty args
				if len(aggExpr.Args) > 0 {
					// Check if it's a ColumnNameExpr with * or just use the expression
					if colExpr, ok := aggExpr.Args[0].(*ast.ColumnNameExpr); ok {
						agg.FieldExpr = FieldExpr(colExpr.Name.Name.O)
					} else {
						agg.FieldExpr = mysqlExprToExpression(aggExpr.Args[0])
					}
				} else {
					// COUNT(*) with no args means star
					agg.FieldExpr = FieldExpr("*")
				}
				continue
			}
		}

		// Check for window function
		if wfExpr, ok := f.Expr.(*ast.WindowFuncExpr); ok {
			wf := extractMySQLWindowFunction(wfExpr, f.AsName.O)
			wfs = append(wfs, wf)
			continue
		}

		cols = append(cols, mysqlExprToExpression(f.Expr))
	}

	return cols, agg, wfs
}

func extractMySQLSelectColumns(fields []*ast.SelectField) []models.SelectColumn {
	var selectCols []models.SelectColumn
	for _, f := range fields {
		if f.AsName.O != "" && f.Expr != nil {
			if colExpr, ok := f.Expr.(*ast.ColumnNameExpr); ok {
				selectCols = append(selectCols, models.SelectColumn{
					ExpressionObj: FieldExpr(colExpr.Name.Name.O),
					Alias:         f.AsName.O,
				})
			}
		}
	}
	return selectCols
}

func extractMySQLJoins(refs *ast.Join) []models.Join {
	var joins []models.Join
	if refs == nil || refs.Right == nil {
		return joins
	}

	join := models.Join{}

	// Get right table
	if ts, ok := refs.Right.(*ast.TableSource); ok {
		if tn, ok := ts.Source.(*ast.TableName); ok {
			join.Table = TableToEntity(tn.Name.O)
		}
	}

	// Get join type
	switch refs.Tp {
	case ast.LeftJoin:
		join.Type = models.LeftJoin
	case ast.RightJoin:
		join.Type = models.RightJoin
	case ast.CrossJoin:
		join.Type = models.CrossJoin
	default:
		join.Type = models.InnerJoin
	}

	// Get ON condition
	if refs.On != nil {
		if binOp, ok := refs.On.Expr.(*ast.BinaryOperationExpr); ok {
			join.LeftExpr = mysqlExprToExpression(binOp.L)
			join.RightExpr = mysqlExprToExpression(binOp.R)
		}
	}

	joins = append(joins, join)
	return joins
}