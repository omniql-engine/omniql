package postgres

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	
	"github.com/omniql-engine/omniql/mapping"  
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// ============================================================================
// NIL-SAFE HELPERS (TrueAST)
// ============================================================================

func getCondField(cond *pb.QueryCondition) string {
	if cond == nil || cond.FieldExpr == nil {
		return ""
	}
	return cond.FieldExpr.Value
}

func getCondValue(cond *pb.QueryCondition) string {
	if cond == nil || cond.ValueExpr == nil {
		return ""
	}
	return cond.ValueExpr.Value
}

func getFieldName(field *pb.QueryField) string {
	if field == nil || field.NameExpr == nil {
		return ""
	}
	return field.NameExpr.Value
}

func getFieldValue(field *pb.QueryField) string {
	if field == nil || field.ValueExpr == nil {
		return ""
	}
	return field.ValueExpr.Value
}

func getOrderByField(ob *pb.OrderByClause) string {
	if ob == nil || ob.FieldExpr == nil {
		return ""
	}
	return ob.FieldExpr.Value
}

func getJoinLeft(join *pb.JoinClause) string {
	if join == nil || join.LeftExpr == nil {
		return ""
	}
	return join.LeftExpr.Value
}

func getJoinRight(join *pb.JoinClause) string {
	if join == nil || join.RightExpr == nil {
		return ""
	}
	return join.RightExpr.Value
}

func getAggField(agg *pb.AggregateClause) string {
	if agg == nil || agg.FieldExpr == nil {
		return ""
	}
	return agg.FieldExpr.Value
}


// ============================================================================
// CRUD OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildWhereClause creates a parameterized WHERE clause
func BuildWhereClause(conditions []*pb.QueryCondition, startParamNum int) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}
	clause, args, _ := buildConditionsRecursive(conditions, startParamNum)
	return " WHERE " + clause, args
}

func buildConditionsRecursive(conditions []*pb.QueryCondition, startParamNum int) (string, []interface{}, int) {
	var parts []string
	var args []interface{}
	paramNum := startParamNum

	for i, cond := range conditions {
		var clause string
		var clauseArgs []interface{}
		var consumed int

		if len(cond.Nested) > 0 {
			nestedClause, nestedArgs, nestedConsumed := buildConditionsRecursive(cond.Nested, paramNum)
			clause = "(" + nestedClause + ")"
			clauseArgs = nestedArgs
			consumed = nestedConsumed
		} else {
			clause, clauseArgs, consumed = buildSingleCondition(cond, paramNum)
		}

		if i == 0 {
			parts = append(parts, clause)
		} else {
			logic := cond.Logic
			if logic == "" {
				logic = "AND"
			}
			parts = append(parts, logic+" "+clause)
		}

		args = append(args, clauseArgs...)
		paramNum += consumed
	}

	return strings.Join(parts, " "), args, paramNum - startParamNum
}

func buildSingleCondition(cond *pb.QueryCondition, paramNum int) (string, []interface{}, int) {
	// Build field expression (handles BINARY, FUNCTION, FIELD)
	field := BuildExpressionSQL(cond.FieldExpr)
	
	switch cond.Operator {
	case "IS_NULL":
		return fmt.Sprintf("%s IS NULL", field), nil, 0
	case "IS_NOT_NULL":
		return fmt.Sprintf("%s IS NOT NULL", field), nil, 0
	case "IN":
		return buildInClause(field, "IN", cond.ValuesExpr, paramNum)
	case "NOT_IN":
		return buildInClause(field, "NOT IN", cond.ValuesExpr, paramNum)
	case "BETWEEN":
		return buildBetweenClauseExpr(field, "BETWEEN", cond.ValueExpr, cond.Value2Expr, paramNum)
	case "NOT_BETWEEN":
		return buildBetweenClauseExpr(field, "NOT BETWEEN", cond.ValueExpr, cond.Value2Expr, paramNum)
	default:
		// Check if ValueExpr is a complex expression (BINARY/FUNCTION)
		if cond.ValueExpr != nil && (cond.ValueExpr.Type == "BINARY" || cond.ValueExpr.Type == "FUNCTION") {
			valueSQL := BuildExpressionSQL(cond.ValueExpr)
			return fmt.Sprintf("%s %s %s", field, cond.Operator, valueSQL), nil, 0
		}
		// Simple literal value - parameterize it
		value := getCondValue(cond)
		return fmt.Sprintf("%s %s $%d", field, cond.Operator, paramNum), []interface{}{value}, 1
	}
}

func buildInClause(field, operator string, values []*pb.Expression, startParam int) (string, []interface{}, int) {
	if len(values) == 0 {
		if operator == "IN" {
			return "1 = 0", nil, 0
		}
		return "1 = 1", nil, 0
	}

	placeholders := make([]string, len(values))
	args := make([]interface{}, len(values))
	for i, v := range values {
		placeholders[i] = fmt.Sprintf("$%d", startParam+i)
		args[i] = v.Value
	}

	return fmt.Sprintf("%s %s (%s)", field, operator, strings.Join(placeholders, ", ")), args, len(values)
}

func buildBetweenClause(field, operator, value1, value2 string, startParam int) (string, []interface{}, int) {
	return fmt.Sprintf("%s %s $%d AND $%d", field, operator, startParam, startParam+1), []interface{}{value1, value2}, 2
}

func buildBetweenClauseExpr(field, operator string, value1Expr, value2Expr *pb.Expression, startParam int) (string, []interface{}, int) {
    // Check if expressions are complex (BINARY/FUNCTION)
    if value1Expr != nil && (value1Expr.Type == "BINARY" || value1Expr.Type == "FUNCTION") {
        val1SQL := BuildExpressionSQL(value1Expr)
        val2SQL := BuildExpressionSQL(value2Expr)
        return fmt.Sprintf("%s %s %s AND %s", field, operator, val1SQL, val2SQL), nil, 0
    }
    // Simple literals - parameterize
    val1 := ""
    val2 := ""
    if value1Expr != nil {
        val1 = value1Expr.Value
    }
    if value2Expr != nil {
        val2 = value2Expr.Value
    }
    return fmt.Sprintf("%s %s $%d AND $%d", field, operator, startParam, startParam+1), []interface{}{val1, val2}, 2
}

func BuildSelectSQL(query *pb.RelationalQuery) (string, []interface{}) {
	selectClause := "SELECT"
	if query.Distinct {
		selectClause = "SELECT DISTINCT"
	}
	
	var args []interface{}
	paramNum := 1
	
	columns := "*"
	if len(query.SelectColumns) > 0 {
		var colParts []string
		for _, col := range query.SelectColumns {
			if col.ExpressionObj != nil && col.ExpressionObj.Type == "CASEWHEN" {
				caseSQL := "CASE"
				for _, cond := range col.ExpressionObj.CaseConditions {
					condSQL := buildConditionSQL(cond.Condition)
					// Check if THEN is an expression or literal
					if cond.ThenExpr != nil && (cond.ThenExpr.Type == "BINARY" || cond.ThenExpr.Type == "FUNCTION") {
						thenSQL := BuildExpressionSQL(cond.ThenExpr)
						caseSQL += fmt.Sprintf(" WHEN %s THEN %s", condSQL, thenSQL)
					} else {
						caseSQL += fmt.Sprintf(" WHEN %s THEN $%d", condSQL, paramNum)
						args = append(args, cond.ThenExpr.Value)
						paramNum++
					}
				}
				if col.ExpressionObj.CaseElse != nil {
					// Check if ELSE is an expression or literal
					if col.ExpressionObj.CaseElse.Type == "BINARY" || col.ExpressionObj.CaseElse.Type == "FUNCTION" {
						elseSQL := BuildExpressionSQL(col.ExpressionObj.CaseElse)
						caseSQL += fmt.Sprintf(" ELSE %s", elseSQL)
					} else {
						caseSQL += fmt.Sprintf(" ELSE $%d", paramNum)
						args = append(args, col.ExpressionObj.CaseElse.Value)
						paramNum++
					}
				}
				caseSQL += " END"
				if col.Alias != "" {
					caseSQL += " AS " + col.Alias
				}
				colParts = append(colParts, caseSQL)
			} else if col.ExpressionObj != nil && col.ExpressionObj.Type == "WINDOW" {
				windowSQL := buildWindowExprSQL(col.ExpressionObj)
				if col.Alias != "" {
					windowSQL += " AS " + col.Alias
				}
				colParts = append(colParts, windowSQL)
			} else {
				colStr := BuildExpressionSQL(col.ExpressionObj)
				if col.Alias != "" {
					colStr += " AS " + col.Alias
				}
				colParts = append(colParts, colStr)
			}
		}
		columns = strings.Join(colParts, ", ")
	} else if len(query.Columns) > 0 {
		var colStrs []string
		for _, col := range query.Columns {
			colStrs = append(colStrs, col.Value)
		}
		columns = strings.Join(colStrs, ", ")
	}
	
	sql := fmt.Sprintf("%s %s FROM %s", selectClause, columns, query.Table)
	whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
	sql += whereClause
	args = append(args, whereArgs...)

	// GROUP BY
	if len(query.GroupBy) > 0 {
		var groupByStrs []string
		for _, gb := range query.GroupBy {
			groupByStrs = append(groupByStrs, gb.Value)
		}
		sql += " GROUP BY " + strings.Join(groupByStrs, ", ")
	}

	// ORDER BY
	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		var orderParts []string
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", getOrderByField(ob), ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}

	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	if query.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", query.Offset)
	}
	
	return sql, args
}

func buildConditionSQL(cond *pb.QueryCondition) string {
	if cond == nil {
		return ""
	}
	return fmt.Sprintf("%s %s %s", getCondField(cond), cond.Operator, getCondValue(cond))
}

func buildWindowExprSQL(expr *pb.Expression) string {
	funcName := strings.ReplaceAll(expr.FunctionName, " ", "_")
	
	var funcCall string
	switch funcName {
	case "LAG", "LEAD":
		field := "id"
		for _, arg := range expr.FunctionArgs {
			if !strings.HasPrefix(arg.Value, "PARTITION:") && !strings.HasPrefix(arg.Value, "ORDER:") {
				field = arg.Value
				break
			}
		}
		funcCall = fmt.Sprintf("%s(%s)", funcName, field)
	case "NTILE":
		buckets := "4"
		for _, arg := range expr.FunctionArgs {
			if !strings.HasPrefix(arg.Value, "PARTITION:") && !strings.HasPrefix(arg.Value, "ORDER:") {
				buckets = arg.Value
				break
			}
		}
		funcCall = fmt.Sprintf("%s(%s)", funcName, buckets)
	default:
		funcCall = fmt.Sprintf("%s()", funcName)
	}
	
	var partitionParts, orderParts []string
	for _, arg := range expr.FunctionArgs {
		if strings.HasPrefix(arg.Value, "PARTITION:") {
			partitionParts = append(partitionParts, strings.TrimPrefix(arg.Value, "PARTITION:"))
		} else if strings.HasPrefix(arg.Value, "ORDER:") {
			parts := strings.Split(strings.TrimPrefix(arg.Value, "ORDER:"), ":")
			if len(parts) >= 2 {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", parts[0], parts[1]))
			} else if len(parts) == 1 {
				orderParts = append(orderParts, parts[0]+" ASC")
			}
		}
	}
	
	overClause := " OVER ("
	if len(partitionParts) > 0 {
		overClause += "PARTITION BY " + strings.Join(partitionParts, ", ")
		if len(orderParts) > 0 {
			overClause += " "
		}
	}
	if len(orderParts) > 0 {
		overClause += "ORDER BY " + strings.Join(orderParts, ", ")
	}
	overClause += ")"
	
	return funcCall + overClause
}

// BuildExpressionSQL converts an Expression to SQL
func BuildExpressionSQL(expr *pb.Expression) string {
	if expr == nil {
		return ""
	}
	switch expr.Type {
	case "BINARY":
		left := BuildExpressionSQL(expr.Left)
		right := BuildExpressionSQL(expr.Right)
		
		// Add parentheses around nested BINARY to preserve precedence
		if expr.Left != nil && expr.Left.Type == "BINARY" {
			left = "(" + left + ")"
		}
		if expr.Right != nil && expr.Right.Type == "BINARY" {
			right = "(" + right + ")"
		}
		
		return fmt.Sprintf("%s %s %s", left, expr.Operator, right)
	case "FUNCTION":
		var args []string
		for _, arg := range expr.FunctionArgs {
			args = append(args, BuildExpressionSQL(arg))
		}
		return fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(args, ", "))
	default:
		return expr.Value
	}
}

func BuildInsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var fields, placeholders []string
	var args []interface{}

	for i, field := range query.Fields {
		fields = append(fields, getFieldName(field))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, getFieldValue(field))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		query.Table, strings.Join(fields, ", "), strings.Join(placeholders, ", "))

	return sql, args
}

func BuildUpdateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var setParts []string
	var args []interface{}
	paramNum := 1
	
	for _, field := range query.Fields {
		fieldName := getFieldName(field)
		
		if field.ValueExpr != nil && field.ValueExpr.Type == "BINARY" {
			exprSQL := BuildExpressionSQL(field.ValueExpr)
			setParts = append(setParts, fmt.Sprintf("%s = %s", fieldName, exprSQL))
		
		} else if field.ValueExpr != nil && field.ValueExpr.Type == "FUNCTION" {
			exprSQL := BuildExpressionSQL(field.ValueExpr)
			setParts = append(setParts, fmt.Sprintf("%s = %s", fieldName, exprSQL))
		
		} else if field.ValueExpr != nil && field.ValueExpr.Type == "CASEWHEN" {
			caseSQL := "CASE"
			for _, cond := range field.ValueExpr.CaseConditions {
				condSQL := buildConditionSQL(cond.Condition)
				caseSQL += fmt.Sprintf(" WHEN %s THEN $%d", condSQL, paramNum)
				args = append(args, strings.Trim(cond.ThenExpr.Value, "'\""))
				paramNum++
			}
			if field.ValueExpr.CaseElse != nil {
				caseSQL += fmt.Sprintf(" ELSE $%d", paramNum)
				args = append(args, strings.Trim(field.ValueExpr.CaseElse.Value, "'\""))
				paramNum++
			}
			caseSQL += " END"
			setParts = append(setParts, fmt.Sprintf("%s = %s", fieldName, caseSQL))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = $%d", fieldName, paramNum))
			args = append(args, getFieldValue(field))
			paramNum++
		}
	}
	
	sql := fmt.Sprintf("UPDATE %s SET %s", query.Table, strings.Join(setParts, ", "))
	whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
	sql += whereClause
	args = append(args, whereArgs...)
	
	return sql, args
}

func isIdentifier(s string) bool {
	s = strings.TrimSpace(s)
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		return false
	}
	matched, _ := regexp.MatchString(`^-?\d+(\.\d+)?$`, s)
	return !matched
}

func BuildDeleteSQL(query *pb.RelationalQuery) (string, []interface{}) {
	sql := fmt.Sprintf("DELETE FROM %s", query.Table)
	whereClause, args := BuildWhereClause(query.Conditions, 1)
	sql += whereClause
	return sql, args
}

func BuildUpsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	if query.Upsert == nil {
		return BuildInsertSQL(query)
	}

	var fields, placeholders []string
	var args []interface{}

	for i, field := range query.Fields {
		fields = append(fields, getFieldName(field))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, getFieldValue(field))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		query.Table, strings.Join(fields, ", "), strings.Join(placeholders, ", "))

	if len(query.Upsert.ConflictFields) > 0 {
		var conflictFieldStrs []string
		for _, cf := range query.Upsert.ConflictFields {
			conflictFieldStrs = append(conflictFieldStrs, cf.Value)
		}
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", strings.Join(conflictFieldStrs, ", "))

		var updateParts []string
	for _, field := range query.Upsert.UpdateFields {
			fieldName := getFieldName(field)
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", fieldName, fieldName))
		}
		sql += strings.Join(updateParts, ", ")
	}

		return sql, args
}


func BuildBulkInsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	if len(query.BulkData) == 0 {
		return "", []interface{}{}
	}

	firstRow := query.BulkData[0]
	var fields []string
	for _, field := range firstRow.Fields {
		fields = append(fields, getFieldName(field))
	}

	var valueClauses []string
	var args []interface{}
	paramNum := 1

	for _, row := range query.BulkData {
		var placeholders []string
		for _, field := range row.Fields {
			placeholders = append(placeholders, fmt.Sprintf("$%d", paramNum))
			args = append(args, getFieldValue(field))
			paramNum++
		}
		valueClauses = append(valueClauses, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		query.Table, strings.Join(fields, ", "), strings.Join(valueClauses, ", "))

	return sql, args
}

// ============================================================================
// DCL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildGrantSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for GRANT")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user/role specified for GRANT")
	}
	privileges := TranslatePermissions(query.Permissions)
	return fmt.Sprintf("GRANT %s ON %s TO %s", strings.Join(privileges, ", "), query.Table, query.PermissionTarget), nil
}

func BuildRevokeSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for REVOKE")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user/role specified for REVOKE")
	}
	privileges := TranslatePermissions(query.Permissions)
	return fmt.Sprintf("REVOKE %s ON %s FROM %s", strings.Join(privileges, ", "), query.Table, query.PermissionTarget), nil
}

func BuildCreateUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for CREATE USER")
	}
	sql := fmt.Sprintf("CREATE USER %s", query.UserName)
	if query.Password != "" {
		sql += fmt.Sprintf(" WITH PASSWORD '%s'", query.Password)
	}
	return sql, nil
}

func BuildDropUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for DROP USER")
	}
	return fmt.Sprintf("DROP USER IF EXISTS %s", query.UserName), nil
}

func BuildAlterUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for ALTER USER")
	}
	sql := fmt.Sprintf("ALTER USER %s", query.UserName)
	if query.Password != "" {
		sql += fmt.Sprintf(" WITH PASSWORD '%s'", query.Password)
	}
	return sql, nil
}

func BuildCreateRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for CREATE ROLE")
	}
	return fmt.Sprintf("CREATE ROLE %s", query.RoleName), nil
}

func BuildDropRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for DROP ROLE")
	}
	return fmt.Sprintf("DROP ROLE IF EXISTS %s", query.RoleName), nil
}

func BuildAssignRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for ASSIGN ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for ASSIGN ROLE")
	}
	return fmt.Sprintf("GRANT %s TO %s", query.RoleName, query.UserName), nil
}

func BuildRevokeRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for REVOKE ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for REVOKE ROLE")
	}
	return fmt.Sprintf("REVOKE %s FROM %s", query.RoleName, query.UserName), nil
}

func TranslatePermissions(permissions []string) []string {
	var pgPrivileges []string
	for _, perm := range permissions {
		switch strings.ToUpper(perm) {
		case "READ":
			pgPrivileges = append(pgPrivileges, "SELECT")
		case "WRITE":
			pgPrivileges = append(pgPrivileges, "INSERT", "UPDATE")
		case "DELETE":
			pgPrivileges = append(pgPrivileges, "DELETE")
		case "ALL":
			pgPrivileges = append(pgPrivileges, "ALL PRIVILEGES")
		case "SELECT", "INSERT", "UPDATE", "TRUNCATE", "REFERENCES", "TRIGGER":
			pgPrivileges = append(pgPrivileges, perm)
		default:
			pgPrivileges = append(pgPrivileges, perm)
		}
	}
	return pgPrivileges
}

// ============================================================================
// DDL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildCreateTableSQL(query *pb.RelationalQuery) string {
	if len(query.Fields) == 0 {
		return ""
	}
	var columns []string
	for _, field := range query.Fields {
		columnDef := buildColumnDefinition(field.NameExpr.Value, field.ValueExpr.Value, field.Constraints)
		columns = append(columns, columnDef)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", query.Table, strings.Join(columns, ", "))
}

func BuildAlterTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.AlterAction == "" {
		return "", fmt.Errorf("no ALTER operation specified")
	}

	switch strings.ToUpper(query.AlterAction) {
	case "ADD_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for ADD_COLUMN")
		}
		colName := getFieldName(query.Fields[0])
		colType := getFieldValue(query.Fields[0])
		columnDef := buildColumnDefinition(colName, colType, query.Fields[0].Constraints)
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", query.Table, columnDef), nil

	case "DROP_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for DROP_COLUMN")
		}
		colName := getFieldName(query.Fields[0])
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", query.Table, colName), nil

	case "RENAME_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for RENAME_COLUMN")
		}
		oldName := getFieldName(query.Fields[0])
		newName := getFieldValue(query.Fields[0])
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", query.Table, oldName, newName), nil

	case "RENAME_TABLE":
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, query.NewName), nil

	default:
		return "", fmt.Errorf("unknown ALTER operation: %s", query.AlterAction)
	}
}

func BuildDropTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", query.Table)
}

func BuildTruncateTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", query.Table)
}

func BuildRenameTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.Table == "" {
		return "", fmt.Errorf("no table name specified")
	}
	if query.NewName == "" {
		return "", fmt.Errorf("no new table name specified")
	}
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, query.NewName), nil
}

func BuildCreateIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index details specified")
	}

	indexName := query.Fields[0].NameExpr.Value
	columnName := query.Fields[0].ValueExpr.Value
	
	indexType := "INDEX"
	if len(query.Fields[0].Constraints) > 0 {
		for _, constraint := range query.Fields[0].Constraints {
			if strings.ToUpper(constraint) == "UNIQUE" {
				indexType = "UNIQUE INDEX"
				break
			}
		}
	}
	
	return fmt.Sprintf("CREATE %s %s ON %s (%s)", indexType, indexName, query.Table, columnName), nil
}

func BuildDropIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index name specified")
	}
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", query.Fields[0].NameExpr.Value), nil
}

func BuildCreateDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("CREATE DATABASE %s", query.DatabaseName), nil
}

func BuildDropDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", query.DatabaseName), nil
}

// formatLiteral formats a value as SQL literal (for VIEW definitions)
func formatLiteral(v interface{}) string {
	s := fmt.Sprintf("%v", v)
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	upper := strings.ToUpper(s)
	if upper == "TRUE" || upper == "FALSE" {
		return upper
	}
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}

func BuildCreateViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no view query specified")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		viewSQL = strings.Replace(viewSQL, placeholder, formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func BuildDropViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	return fmt.Sprintf("DROP VIEW IF EXISTS %s", query.ViewName), nil
}

func BuildAlterViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no view query specified")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		viewSQL = strings.Replace(viewSQL, placeholder, formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func buildColumnDefinition(name, columnType string, constraints []string) string {
	baseType := columnType
	params := ""

	if idx := strings.Index(columnType, "("); idx != -1 {
		baseType = columnType[:idx]
		if endIdx := strings.Index(columnType, ")"); endIdx != -1 {
			params = columnType[idx : endIdx+1]
		}
	}

	pgType := baseType
	if mapping.TypeMap != nil && mapping.TypeMap["PostgreSQL"] != nil {
		if mappedType, exists := mapping.TypeMap["PostgreSQL"][strings.ToUpper(baseType)]; exists {
			pgType = mappedType
		}
	}

	columnDef := fmt.Sprintf("%s %s%s", name, pgType, params)

	if strings.ToUpper(baseType) == "AUTO" {
		return fmt.Sprintf("%s SERIAL PRIMARY KEY", name)
	}

	for _, constraint := range constraints {
		switch strings.ToUpper(constraint) {
		case "UNIQUE":
			columnDef += " UNIQUE"
		case "NOT_NULL":
			columnDef += " NOT NULL"
		case "PRIMARY_KEY":
			columnDef += " PRIMARY KEY"
		}
	}

	return columnDef
}

// ============================================================================
// DQL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildJoinSQL(query *pb.RelationalQuery) (string, []interface{}) {
	selectClause := "*"
	if len(query.Columns) > 0 {
		var colStrs []string
		for _, col := range query.Columns {
			colStrs = append(colStrs, col.Value)
		}
		selectClause = strings.Join(colStrs, ", ")
	}
	
	sql := fmt.Sprintf("SELECT %s FROM %s", selectClause, query.Table)
	var args []interface{}
	paramNum := 1

	for _, join := range query.Joins {
		joinType := strings.ToUpper(join.JoinType)
		sql += fmt.Sprintf(" %s JOIN %s", joinType, join.Table)
	if joinType != "CROSS" {
			sql += fmt.Sprintf(" ON %s = %s", getJoinLeft(join), getJoinRight(join))
		}
	}

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
		sql += whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

func BuildAggregateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	aggFunc := strings.ToUpper(query.Aggregate.Function)
	aggField := getAggField(query.Aggregate)
	
	var args []interface{}
	paramNum := 1
	
	needsSubquery := (query.Limit > 0 || query.Offset > 0) && len(query.GroupBy) == 0
	
	var innerSQL string
	if needsSubquery {
		innerSQL = fmt.Sprintf("SELECT * FROM %s", query.Table)
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
			innerSQL += whereClause
			args = append(args, whereArgs...)
			paramNum += len(whereArgs)
		}
		if len(query.OrderBy) > 0 {
			innerSQL += " ORDER BY "
			orderParts := []string{}
			for _, ob := range query.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", getOrderByField(ob), ob.Direction))
			}
			innerSQL += strings.Join(orderParts, ", ")
		}
		if query.Limit > 0 {
			innerSQL += fmt.Sprintf(" LIMIT %d", query.Limit)
		}
		if query.Offset > 0 {
			innerSQL += fmt.Sprintf(" OFFSET %d", query.Offset)
		}
	}
	
	var selectClause string
	if aggField == "" || aggField == "*" {
		selectClause = "SELECT COUNT(*)"
	} else {
		if query.Distinct {
			selectClause = fmt.Sprintf("SELECT %s(DISTINCT %s)", aggFunc, aggField)
		} else {
			selectClause = fmt.Sprintf("SELECT %s(%s)", aggFunc, aggField)
		}
		if len(query.GroupBy) > 0 {
			var groupByStrs []string
			for _, gb := range query.GroupBy {
				groupByStrs = append(groupByStrs, gb.Value)
			}
			selectClause += ", " + strings.Join(groupByStrs, ", ")
		}
	}
	
	var sql string
	if needsSubquery {
		sql = fmt.Sprintf("%s FROM (%s) AS subquery", selectClause, innerSQL)
	} else {
		sql = fmt.Sprintf("%s FROM %s", selectClause, query.Table)
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
			sql += whereClause
			args = append(args, whereArgs...)
			paramNum += len(whereArgs)
		}
	}
	
	if len(query.GroupBy) > 0 {
		var groupByStrs []string
		for _, gb := range query.GroupBy {
			groupByStrs = append(groupByStrs, gb.Value)
		}
		sql += " GROUP BY " + strings.Join(groupByStrs, ", ")
	}
	
	if len(query.Having) > 0 {
		havingClause, havingArgs := BuildHavingClause(query.Having, paramNum)
		sql += havingClause
		args = append(args, havingArgs...)
		paramNum += len(havingArgs)
	}
	
	if len(query.GroupBy) > 0 && len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", getOrderByField(ob), ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}
	
	if len(query.GroupBy) > 0 {
		if query.Limit > 0 {
			sql += fmt.Sprintf(" LIMIT %d", query.Limit)
		}
		if query.Offset > 0 {
			sql += fmt.Sprintf(" OFFSET %d", query.Offset)
		}
	}
	
	return sql, args
}

func BuildWindowFunctionSQL(query *pb.RelationalQuery) (string, []interface{}) {
	if len(query.WindowFunctions) == 0 {
		return "", []interface{}{}
	}

	selectParts := []string{"*"}

	for _, wf := range query.WindowFunctions {
		funcName := strings.ToUpper(wf.Function)
		funcName = strings.ReplaceAll(funcName, " ", "_")

		var windowFunc string
		switch funcName {
		case "LAG", "LEAD":
			field := wf.Alias
			if field == "" {
				field = "id"
			}
			offset := wf.Offset
			if offset == 0 {
				offset = 1
			}
			windowFunc = fmt.Sprintf("%s(%s, %d)", funcName, field, offset)
		case "NTILE":
			buckets := wf.Buckets
			if buckets == 0 {
				buckets = 4
			}
			windowFunc = fmt.Sprintf("%s(%d)", funcName, buckets)
		default:
			windowFunc = fmt.Sprintf("%s()", funcName)
		}

		overClause := " OVER ("
		if len(wf.PartitionBy) > 0 {
			var partitionStrs []string
			for _, pb := range wf.PartitionBy {
				partitionStrs = append(partitionStrs, pb.Value)
			}
			overClause += "PARTITION BY " + strings.Join(partitionStrs, ", ")
		}
		if len(wf.OrderBy) > 0 {
			if len(wf.PartitionBy) > 0 {
				overClause += " "
			}
			overClause += "ORDER BY "
			orderParts := []string{}
			for _, ob := range wf.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", getOrderByField(ob), ob.Direction))
			}
			overClause += strings.Join(orderParts, ", ")
		}
		overClause += ")"

		alias := wf.Alias
		if alias == "" {
			alias = strings.ToLower(funcName)
		}
		if funcName == "LAG" || funcName == "LEAD" {
			alias = strings.ToLower(funcName) + "_result"
		}

		selectParts = append(selectParts, fmt.Sprintf("%s %s AS %s", windowFunc, overClause, alias))
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), query.Table)
	var args []interface{}
	paramNum := 1

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
		sql += whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

func BuildCTESQL(query *pb.RelationalQuery) string {
	if query.Cte == nil {
		return ""
	}
	cteSQL, _ := BuildSelectSQL(query.Cte.CteQuery)
	return fmt.Sprintf("WITH %s AS (%s) %s", query.Cte.CteName, cteSQL, query.Table)
}

func BuildSubquerySQL(query *pb.RelationalQuery) (string, []interface{}) {
	subqueryType := strings.ToUpper(query.Subquery.SubqueryType)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE ", query.Table)
	var args []interface{}

	if len(query.Conditions) > 0 {
		whereParts := []string{}
		for _, cond := range query.Conditions {
			whereParts = append(whereParts, fmt.Sprintf("%s %s $%d", cond.FieldExpr.Value, cond.Operator, len(args)+1))
			args = append(args, cond.ValueExpr.Value)
		}
		sql += strings.Join(whereParts, " AND ") + " AND "
	}

	subField := query.Subquery.FieldExpr.Value
	subquerySQL, _ := BuildSelectSQL(query.Subquery.Subquery)

	if subqueryType == "IN" {
		sql += fmt.Sprintf("%s IN (%s)", subField, subquerySQL)
	} else if subqueryType == "EXISTS" {
		sql += fmt.Sprintf("EXISTS (%s)", subquerySQL)
	}

	return sql, args
}

func BuildLikeSQL(query *pb.RelationalQuery) (string, []interface{}) {
	sql := fmt.Sprintf("SELECT * FROM %s", query.Table)
	var args []interface{}
	paramNum := 1

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
		sql += whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

func BuildCaseSQL(query *pb.RelationalQuery) string {
	if len(query.SelectColumns) > 0 {
		for _, col := range query.SelectColumns {
			if col.ExpressionObj != nil && col.ExpressionObj.Type == "CASEWHEN" {
				caseSQL := "CASE"
				for _, when := range col.ExpressionObj.CaseConditions {
					condSQL := buildConditionSQL(when.Condition)
					caseSQL += fmt.Sprintf(" WHEN %s THEN %s", condSQL, when.ThenExpr.Value)
				}
				if col.ExpressionObj.CaseElse != nil {
					caseSQL += fmt.Sprintf(" ELSE %s", col.ExpressionObj.CaseElse.Value)
				}
				caseSQL += " END"
				alias := col.Alias
				if alias == "" {
					alias = "case_result"
				}
				return fmt.Sprintf("SELECT *, %s AS %s FROM %s", caseSQL, alias, query.Table)
			}
		}
	}
	return ""
}

func BuildSetOperationSQL(query *pb.RelationalQuery) (string, []interface{}) {
	if query.SetOperation == nil {
		return "", []interface{}{}
	}

	var allArgs []interface{}
	argOffset := 0

	leftSQL, leftArgs := BuildQuerySQL(query.SetOperation.LeftQuery, argOffset)
	allArgs = append(allArgs, leftArgs...)
	argOffset += len(leftArgs)

	rightSQL, rightArgs := BuildQuerySQL(query.SetOperation.RightQuery, argOffset)
	allArgs = append(allArgs, rightArgs...)

	operationType := strings.ToUpper(query.SetOperation.OperationType)
	if operationType == "UNION_ALL" {
		operationType = "UNION ALL"
	}

	return fmt.Sprintf("(%s) %s (%s)", leftSQL, operationType, rightSQL), allArgs
}

func BuildQuerySQL(query *pb.RelationalQuery, argOffset int) (string, []interface{}) {
	if query.Aggregate != nil {
		return BuildAggregateSQL(query)
	}

	sql := fmt.Sprintf("SELECT * FROM %s", query.Table)
	var args []interface{}
	paramNum := argOffset + 1

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
		sql += whereClause
		args = append(args, whereArgs...)
		paramNum += len(whereArgs)
	}

	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", getOrderByField(ob), ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}

	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	if query.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", query.Offset)
	}

	return sql, args
}

func BuildHavingClause(conditions []*pb.QueryCondition, startParamNum int) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}
	clause, args, _ := buildConditionsRecursive(conditions, startParamNum)
	return " HAVING " + clause, args
}

// ============================================================================
// TCL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildSetTransactionSQL(query *pb.RelationalQuery) (string, error) {
	parts := []string{"SET TRANSACTION"}
	var options []string

	if query.IsolationLevel != "" {
		options = append(options, fmt.Sprintf("ISOLATION LEVEL %s", TranslateIsolationLevel(query.IsolationLevel)))
	}
	if query.ReadOnly {
		options = append(options, "READ ONLY")
	}
	if len(options) == 0 {
		return "", fmt.Errorf("no transaction options specified")
	}

	parts = append(parts, strings.Join(options, " "))
	return strings.Join(parts, " "), nil
}

func BuildSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("SAVEPOINT %s", query.SavepointName), nil
}

func BuildRollbackToSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", query.SavepointName), nil
}

func BuildReleaseSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("RELEASE SAVEPOINT %s", query.SavepointName), nil
}

func TranslateIsolationLevel(level string) string {
	level = strings.ToUpper(strings.TrimSpace(level))
	switch level {
	case "READ_UNCOMMITTED", "READ UNCOMMITTED":
		return "READ UNCOMMITTED"
	case "READ_COMMITTED", "READ COMMITTED":
		return "READ COMMITTED"
	case "REPEATABLE_READ", "REPEATABLE READ":
		return "REPEATABLE READ"
	case "SERIALIZABLE":
		return "SERIALIZABLE"
	default:
		return "READ COMMITTED"
	}
}