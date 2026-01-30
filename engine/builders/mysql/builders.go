package mysql

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// DefaultMySQLUserHost is the default host for MySQL user operations.
const DefaultMySQLUserHost = "localhost"

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

// BuildSelectSQL creates parameterized SELECT query with expression support
// BuildSelectSQL creates parameterized SELECT query with expression support
func BuildSelectSQL(query *pb.RelationalQuery) (string, []interface{}) {
	selectClause := "SELECT"
	if query.Distinct {
		selectClause = "SELECT DISTINCT"
	}
	
	var args []interface{}
	
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
						caseSQL += fmt.Sprintf(" WHEN %s THEN ?", condSQL)
						args = append(args, cond.ThenExpr.Value)
					}
				}
				if col.ExpressionObj.CaseElse != nil {
					// Check if ELSE is an expression or literal
					if col.ExpressionObj.CaseElse.Type == "BINARY" || col.ExpressionObj.CaseElse.Type == "FUNCTION" {
						elseSQL := BuildExpressionSQL(col.ExpressionObj.CaseElse)
						caseSQL += fmt.Sprintf(" ELSE %s", elseSQL)
					} else {
						caseSQL += " ELSE ?"
						args = append(args, col.ExpressionObj.CaseElse.Value)
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
	
	sql := fmt.Sprintf("%s %s FROM `%s`", selectClause, columns, query.Table)
	
	whereClause, whereArgs := BuildWhereClause(query.Conditions)
	sql += whereClause
	args = append(args, whereArgs...)
	
	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.FieldExpr.Value, ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}
	
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
		if query.Offset > 0 {
			sql += fmt.Sprintf(" OFFSET %d", query.Offset)
		}
	} else if query.Offset > 0 {
		sql += fmt.Sprintf(" LIMIT 18446744073709551615 OFFSET %d", query.Offset)
	}

	return sql, args
}

func buildConditionSQL(cond *pb.QueryCondition) string {
	if cond == nil {
		return ""
	}
	field := BuildExpressionSQL(cond.FieldExpr)
	value := getCondValue(cond)
	// Quote string values (non-numeric)
	if _, err := strconv.ParseFloat(value, 64); err != nil {
		// Not a number - check if boolean
		upper := strings.ToUpper(value)
		if upper != "TRUE" && upper != "FALSE" && upper != "NULL" {
			value = fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
		}
	}
	return fmt.Sprintf("%s %s %s", field, cond.Operator, value)
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

// BuildInsertSQL creates parameterized INSERT query
func BuildInsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var fields, placeholders []string
	var args []interface{}

	for _, field := range query.Fields {
		fields = append(fields, getFieldName(field))
		placeholders = append(placeholders, "?")
		args = append(args, ConvertMySQLValue(getFieldValue(field)))
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		query.Table, strings.Join(fields, ", "), strings.Join(placeholders, ", "))

	return sql, args
}

// BuildUpdateSQL creates parameterized UPDATE query with expression support
func BuildUpdateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var setParts []string
	var args []interface{}

	for _, field := range query.Fields {
		fieldName := getFieldName(field)
		
		if field.ValueExpr != nil && (field.ValueExpr.Type == "BINARY" || field.ValueExpr.Type == "FUNCTION" || field.ValueExpr.Type == "CASEWHEN") {
			exprSQL := BuildExpressionSQL(field.ValueExpr)
			setParts = append(setParts, fmt.Sprintf("%s = %s", fieldName, exprSQL))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = ?", fieldName))
			args = append(args, ConvertMySQLValue(getFieldValue(field)))
		}
	}

	sql := fmt.Sprintf("UPDATE `%s` SET %s", query.Table, strings.Join(setParts, ", "))

	whereClause, whereArgs := BuildWhereClause(query.Conditions)
	sql += whereClause
	args = append(args, whereArgs...)

	return sql, args
}

// BuildDeleteSQL creates parameterized DELETE query
func BuildDeleteSQL(query *pb.RelationalQuery) (string, []interface{}) {
	sql := fmt.Sprintf("DELETE FROM `%s`", query.Table)
	whereClause, args := BuildWhereClause(query.Conditions)
	sql += whereClause
	return sql, args
}

// BuildUpsertSQL creates UPSERT using MySQL's ON DUPLICATE KEY UPDATE
func BuildUpsertSQL(query *pb.RelationalQuery) (string, []interface{}, error) {
	if query.Upsert == nil || len(query.Upsert.ConflictFields) == 0 {
		return "", nil, fmt.Errorf("UPSERT requires conflict fields")
	}

	var fields, placeholders []string
	var args []interface{}

	for _, field := range query.Fields {
		fieldName := getFieldName(field)
		fields = append(fields, fieldName)
		placeholders = append(placeholders, "?")
		args = append(args, ConvertMySQLValue(getFieldValue(field)))
	}

	var updateParts []string
	for _, field := range query.Fields {
		fieldName := field.NameExpr.Value
		isConflictField := false
		for _, cf := range query.Upsert.ConflictFields {
			if fieldName == cf.Value {
				isConflictField = true
				break
			}
		}
		if !isConflictField {
			updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", fieldName, fieldName))
		}
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		query.Table, strings.Join(fields, ", "), strings.Join(placeholders, ", "), strings.Join(updateParts, ", "))

	return sql, args, nil
}

// BuildBulkInsertSQL creates BULK INSERT using multi-row VALUES
func BuildBulkInsertSQL(query *pb.RelationalQuery) (string, []interface{}, error) {
	if len(query.BulkData) == 0 {
		return "", nil, fmt.Errorf("BULK_INSERT requires data rows")
	}

	firstRow := query.BulkData[0]
	var fields []string
	for _, field := range firstRow.Fields {
		fields = append(fields, getFieldName(field))
	}

	var valueClauses []string
	var args []interface{}

	for _, row := range query.BulkData {
		placeholders := make([]string, len(row.Fields))
		for i, field := range row.Fields {
			placeholders[i] = "?"
			args = append(args, ConvertMySQLValue(getFieldValue(field)))
		}
		valueClauses = append(valueClauses, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		query.Table, strings.Join(fields, ", "), strings.Join(valueClauses, ", "))

	return sql, args, nil
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// BuildWhereClause creates a parameterized WHERE clause
func BuildWhereClause(conditions []*pb.QueryCondition) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}
	clause, args, _ := buildConditionsRecursive(conditions)
	return " WHERE " + clause, args
}

func buildConditionsRecursive(conditions []*pb.QueryCondition) (string, []interface{}, int) {
	var parts []string
	var args []interface{}
	paramCount := 0

	for i, cond := range conditions {
		var clause string
		var clauseArgs []interface{}
		var consumed int

		if len(cond.Nested) > 0 {
			nestedClause, nestedArgs, nestedConsumed := buildConditionsRecursive(cond.Nested)
			clause = "(" + nestedClause + ")"
			clauseArgs = nestedArgs
			consumed = nestedConsumed
		} else {
			clause, clauseArgs, consumed = buildSingleCondition(cond)
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
		paramCount += consumed
	}

	return strings.Join(parts, " "), args, paramCount
}

func buildSingleCondition(cond *pb.QueryCondition) (string, []interface{}, int) {
	field := BuildExpressionSQL(cond.FieldExpr)
	value := getCondValue(cond)

	switch cond.Operator {
	case "IS_NULL":
		return fmt.Sprintf("%s IS NULL", field), nil, 0
	case "IS_NOT_NULL":
		return fmt.Sprintf("%s IS NOT NULL", field), nil, 0
	case "IN":
		return buildInClause(field, "IN", cond.ValuesExpr)
	case "NOT_IN":
		return buildInClause(field, "NOT IN", cond.ValuesExpr)
	case "BETWEEN":
		return buildBetweenClauseExpr(field, "BETWEEN", cond.ValueExpr, cond.Value2Expr)
	case "NOT_BETWEEN":
		return buildBetweenClauseExpr(field, "NOT BETWEEN", cond.ValueExpr, cond.Value2Expr)
	default:
		return fmt.Sprintf("%s %s ?", field, cond.Operator), []interface{}{ConvertMySQLValue(value)}, 1
	}
}

func buildInClause(field, operator string, values []*pb.Expression) (string, []interface{}, int) {
	if len(values) == 0 {
		if operator == "IN" {
			return "1 = 0", nil, 0
		}
		return "1 = 1", nil, 0
	}

	placeholders := make([]string, len(values))
	args := make([]interface{}, len(values))
	for i, v := range values {
		placeholders[i] = "?"
		args[i] = ConvertMySQLValue(v.Value)
	}

	return fmt.Sprintf("%s %s (%s)", field, operator, strings.Join(placeholders, ", ")), args, len(values)
}

func buildBetweenClause(field, operator, value1, value2 string) (string, []interface{}, int) {
	return fmt.Sprintf("%s %s ? AND ?", field, operator), []interface{}{ConvertMySQLValue(value1), ConvertMySQLValue(value2)}, 2
}

func buildBetweenClauseExpr(field, operator string, value1Expr, value2Expr *pb.Expression) (string, []interface{}, int) {
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
	return fmt.Sprintf("%s %s ? AND ?", field, operator), []interface{}{ConvertMySQLValue(val1), ConvertMySQLValue(val2)}, 2
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
	case "CASEWHEN":
		var caseParts []string
		caseParts = append(caseParts, "CASE")
		for _, cond := range expr.CaseConditions {
			thenValue := cond.ThenExpr.Value
			if _, err := strconv.Atoi(thenValue); err != nil {
				thenValue = fmt.Sprintf("'%s'", thenValue)
			}
			condSQL := buildConditionSQL(cond.Condition)
			caseParts = append(caseParts, fmt.Sprintf("WHEN %s THEN %s", condSQL, thenValue))
		}
		if expr.CaseElse != nil {
			elseValue := expr.CaseElse.Value
			if _, err := strconv.Atoi(elseValue); err != nil {
				elseValue = fmt.Sprintf("'%s'", elseValue)
			}
			caseParts = append(caseParts, fmt.Sprintf("ELSE %s", elseValue))
		}
		caseParts = append(caseParts, "END")
		return strings.Join(caseParts, " ")
	default:
		return expr.Value
	}
}

// ConvertMySQLValue converts values for MySQL compatibility
func ConvertMySQLValue(value string) interface{} {
	switch strings.ToLower(value) {
	case "true":
		return 1
	case "false":
		return 0
	default:
		return value
	}
}

func normalizeAggregateFunction(field string) string {
	aggregates := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}
	fieldUpper := strings.ToUpper(field)
	
	for _, agg := range aggregates {
		if strings.HasPrefix(fieldUpper, agg) {
			idx := strings.Index(field, "(")
			if idx == -1 {
				continue
			}
			endIdx := strings.LastIndex(field, ")")
			if endIdx == -1 {
				continue
			}
			arg := strings.TrimSpace(field[idx+1 : endIdx])
			return fmt.Sprintf("%s(%s)", agg, arg)
		}
	}
	return field
}

// ============================================================================
// DCL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildGrantSQL(query *pb.RelationalQuery, isRole bool) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for GRANT")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user specified for GRANT")
	}

	privileges := TranslatePermissions(query.Permissions)
	if isRole {
		return fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(privileges, ", "), query.Table, query.PermissionTarget), nil
	}
	return fmt.Sprintf("GRANT %s ON %s.* TO '%s'@'%s'", strings.Join(privileges, ", "), query.Table, query.PermissionTarget, DefaultMySQLUserHost), nil
}

func BuildRevokeSQL(query *pb.RelationalQuery, isRole bool) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for REVOKE")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user specified for REVOKE")
	}

	privileges := TranslatePermissions(query.Permissions)
	if isRole {
		return fmt.Sprintf("REVOKE %s ON %s.* FROM %s", strings.Join(privileges, ", "), query.Table, query.PermissionTarget), nil
	}
	return fmt.Sprintf("REVOKE %s ON %s.* FROM '%s'@'%s'", strings.Join(privileges, ", "), query.Table, query.PermissionTarget, DefaultMySQLUserHost), nil
}

func BuildCreateRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for CREATE_ROLE")
	}
	return fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", query.RoleName), nil
}

func BuildDropRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for DROP_ROLE")
	}
	return fmt.Sprintf("DROP ROLE IF EXISTS %s", query.RoleName), nil
}

func BuildAssignRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for ASSIGN_ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no user name specified for ASSIGN_ROLE")
	}
	return fmt.Sprintf("GRANT '%s' TO '%s'@'%s'", query.RoleName, query.UserName, DefaultMySQLUserHost), nil
}

func BuildRevokeRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for REVOKE_ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no user name specified for REVOKE_ROLE")
	}
	return fmt.Sprintf("REVOKE '%s' FROM '%s'@'%s'", query.RoleName, query.UserName, DefaultMySQLUserHost), nil
}

func BuildCreateUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for CREATE_USER")
	}
	if query.Password == "" {
		return "", fmt.Errorf("no password specified for CREATE_USER")
	}
	return fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s'", query.UserName, DefaultMySQLUserHost, query.Password), nil
}

func BuildDropUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for DROP_USER")
	}
	return fmt.Sprintf("DROP USER IF EXISTS '%s'@'%s'", query.UserName, DefaultMySQLUserHost), nil
}

func BuildAlterUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for ALTER_USER")
	}
	if query.Password == "" {
		return "", fmt.Errorf("no password specified for ALTER_USER")
	}
	return fmt.Sprintf("ALTER USER '%s'@'%s' IDENTIFIED BY '%s'", query.UserName, DefaultMySQLUserHost, query.Password), nil
}

func BuildGrantRoleToUserSQL(roleName, userName string) string {
	return fmt.Sprintf("GRANT '%s' TO '%s'@'%s'", roleName, userName, DefaultMySQLUserHost)
}

func TranslatePermissions(permissions []string) []string {
	var privileges []string
	for _, perm := range permissions {
		switch strings.ToUpper(perm) {
		case "READ", "SELECT":
			privileges = append(privileges, "SELECT")
		case "WRITE", "INSERT":
			privileges = append(privileges, "INSERT")
		case "UPDATE":
			privileges = append(privileges, "UPDATE")
		case "DELETE":
			privileges = append(privileges, "DELETE")
		case "ALL":
			privileges = append(privileges, "ALL PRIVILEGES")
		default:
			privileges = append(privileges, perm)
		}
	}
	return privileges
}

// ============================================================================
// DDL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildCreateTableSQL(query *pb.RelationalQuery, typeMap map[string]map[string]string) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no columns specified for CREATE TABLE")
	}

	var columns []string
	for _, field := range query.Fields {
		columnDef := TranslateColumn(field.NameExpr.Value, field.ValueExpr.Value, field.Constraints, typeMap)
		columns = append(columns, columnDef)
	}

	return fmt.Sprintf("CREATE TABLE `%s` (%s)", query.Table, strings.Join(columns, ", ")), nil
}

func BuildAlterTableSQL(query *pb.RelationalQuery, typeMap map[string]map[string]string) (string, error) {
	if query.AlterAction == "" {
		return "", fmt.Errorf("no ALTER operation specified")
	}

	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no column specified for ALTER TABLE")
	}

	field := query.Fields[0]
	columnName := getFieldName(field)
	columnValue := getFieldValue(field)

	switch strings.ToUpper(query.AlterAction) {
	case "ADD_COLUMN":
		if columnValue == "" {
			return "", fmt.Errorf("ADD_COLUMN requires column type")
		}
		columnDef := TranslateColumn(columnName, columnValue, field.Constraints, typeMap)
		return fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", query.Table, columnDef), nil
	case "DROP_COLUMN":
		return fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN %s", query.Table, columnName), nil
	case "RENAME_COLUMN":
		if columnValue == "" {
			return "", fmt.Errorf("RENAME_COLUMN requires new column name")
		}
		return fmt.Sprintf("ALTER TABLE `%s` RENAME COLUMN %s TO %s", query.Table, columnName, columnValue), nil
	case "MODIFY_COLUMN":
		if columnValue == "" {
			return "", fmt.Errorf("MODIFY_COLUMN requires new column type")
		}
		columnDef := TranslateColumn(columnName, columnValue, field.Constraints, typeMap)
		return fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN %s", query.Table, columnDef), nil
	default:
		return "", fmt.Errorf("unknown ALTER operation: %s", query.AlterAction)
	}
}

func BuildDropTableSQL(query *pb.RelationalQuery) (string, error) {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", query.Table), nil
}

func BuildCreateIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index details specified")
	}

	indexName := query.Fields[0].NameExpr.Value
	columnName := query.Fields[0].ValueExpr.Value

	// Check for UNIQUE constraint
	indexType := "INDEX"
	if len(query.Fields[0].Constraints) > 0 {
		for _, constraint := range query.Fields[0].Constraints {
			if strings.ToUpper(constraint) == "UNIQUE" {
				indexType = "UNIQUE INDEX"
				break
			}
		}
	}

	return fmt.Sprintf("CREATE %s %s ON `%s` (%s)", indexType, indexName, query.Table, columnName), nil
}

func BuildDropIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index name specified")
	}
	return fmt.Sprintf("DROP INDEX %s ON `%s`", query.Fields[0].NameExpr.Value, query.Table), nil
}

func BuildTruncateTableSQL(query *pb.RelationalQuery) (string, error) {
	return fmt.Sprintf("TRUNCATE TABLE `%s`", query.Table), nil
}

// formatLiteral converts a value to SQL literal format for VIEW definitions
func formatLiteral(v interface{}) string {
	s := fmt.Sprintf("%v", v)
	// Check if numeric
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	// Check if boolean
	upper := strings.ToUpper(s)
	if upper == "TRUE" {
		return "1"
	}
	if upper == "FALSE" {
		return "0"
	}
	// String - escape single quotes
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}

func BuildCreateViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for CREATE VIEW")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no query specified for CREATE VIEW")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	// Substitute ? placeholders with literal values
	for _, arg := range args {
		viewSQL = strings.Replace(viewSQL, "?", formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func BuildAlterViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for ALTER VIEW")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no query specified for ALTER VIEW")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	// Substitute ? placeholders with literal values
	for _, arg := range args {
		viewSQL = strings.Replace(viewSQL, "?", formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func BuildDropViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for DROP VIEW")
	}
	return fmt.Sprintf("DROP VIEW IF EXISTS %s", query.ViewName), nil
}

func BuildRenameTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.NewName == "" {
		return "", fmt.Errorf("no new name specified for RENAME TABLE")
	}
	return fmt.Sprintf("RENAME TABLE `%s` TO `%s`", query.Table, query.NewName), nil
}

func BuildCreateDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified for CREATE DATABASE")
	}
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", query.DatabaseName), nil
}

func BuildDropDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified for DROP DATABASE")
	}
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", query.DatabaseName), nil
}

func TranslateColumn(columnName, columnType string, constraints []string, typeMap map[string]map[string]string) string {
	baseType := columnType
	params := ""

	if idx := strings.Index(columnType, "("); idx != -1 {
		baseType = columnType[:idx]
		if endIdx := strings.Index(columnType, ")"); endIdx != -1 {
			params = columnType[idx : endIdx+1]
		}
	}

	mysqlType, exists := typeMap["MySQL"][strings.ToUpper(baseType)]
	if !exists {
		mysqlType = baseType
	}

	// Don't append params if mysqlType already has size
	var columnDef string
	// Don't append params if mysqlType already has size
	if strings.Contains(mysqlType, "(") {
		columnDef = fmt.Sprintf("%s %s", columnName, mysqlType)
	} else {
		columnDef = fmt.Sprintf("%s %s%s", columnName, mysqlType, params)
	}

	// Handle AUTO_INCREMENT PRIMARY KEY
	if strings.Contains(mysqlType, "AUTO_INCREMENT") {
		columnDef += " PRIMARY KEY"
	}

	// Handle constraints from AST
	for _, constraint := range constraints {
		switch strings.ToUpper(constraint) {
		case "UNIQUE":
			columnDef += " UNIQUE"
		case "NOT_NULL", "NOTNULL":
			columnDef += " NOT NULL"
		case "PRIMARY_KEY", "PRIMARYKEY":
			if !strings.Contains(columnDef, "PRIMARY KEY") {
				columnDef += " PRIMARY KEY"
			}
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
	
	for _, join := range query.Joins {
		joinType := strings.ToUpper(strings.Replace(join.JoinType, "_", " ", -1))
		if joinType == "CROSS" {
			sql += fmt.Sprintf(" CROSS JOIN %s", join.Table)
		} else if joinType == "FULL" {
			// MySQL doesn't support FULL JOIN - emulate with LEFT JOIN UNION RIGHT JOIN
			leftJoin := fmt.Sprintf("SELECT * FROM %s LEFT JOIN %s ON %s.%s = %s.%s",
				query.Table, join.Table, query.Table, join.LeftExpr.Value, join.Table, join.RightExpr.Value)
			rightJoin := fmt.Sprintf("SELECT * FROM %s RIGHT JOIN %s ON %s.%s = %s.%s WHERE %s.%s IS NULL",
				query.Table, join.Table, query.Table, join.LeftExpr.Value, join.Table, join.RightExpr.Value, query.Table, join.LeftExpr.Value)
			sql = fmt.Sprintf("(%s) UNION (%s)", leftJoin, rightJoin)
		} else {
			sql += fmt.Sprintf(" %s JOIN %s ON %s.%s = %s.%s", joinType, join.Table, query.Table, join.LeftExpr.Value, join.Table, join.RightExpr.Value)
		}
	}
	
	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions)
		sql += whereClause
		args = append(args, whereArgs...)
	}
	
	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.FieldExpr.Value, ob.Direction))
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

func BuildAggregateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var selectClause string
	var args []interface{}
	
	if query.Aggregate != nil {
		aggFunc := strings.ToUpper(query.Aggregate.Function)
		aggField := getAggField(query.Aggregate)
		
		if aggField == "" || aggField == "*" {
			if query.Distinct {
				selectClause = "SELECT COUNT(*)"
			} else {
				selectClause = fmt.Sprintf("SELECT %s(*)", aggFunc)
			}
			if len(query.GroupBy) > 0 {
				var groupByStrs []string
				for _, gb := range query.GroupBy {
					groupByStrs = append(groupByStrs, gb.Value)
				}
				selectClause += ", " + strings.Join(groupByStrs, ", ")
			}
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
	} else {
		selectClause = "SELECT COUNT(*)"
	}
	
	needsSubquery := (query.Limit > 0 || query.Offset > 0) && len(query.GroupBy) == 0
	var sql string
	
	if needsSubquery {
		innerSQL := fmt.Sprintf("SELECT * FROM `%s`", query.Table)
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions)
			innerSQL += whereClause
			args = append(args, whereArgs...)
		}
		if len(query.OrderBy) > 0 {
			innerSQL += " ORDER BY "
			orderParts := []string{}
			for _, ob := range query.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.FieldExpr.Value, ob.Direction))
			}
			innerSQL += strings.Join(orderParts, ", ")
		}
		if query.Offset > 0 && query.Limit == 0 {
			innerSQL += fmt.Sprintf(" LIMIT 18446744073709551615 OFFSET %d", query.Offset)
		} else if query.Limit > 0 && query.Offset > 0 {
			innerSQL += fmt.Sprintf(" LIMIT %d OFFSET %d", query.Limit, query.Offset)
		} else if query.Limit > 0 {
			innerSQL += fmt.Sprintf(" LIMIT %d", query.Limit)
		}
		sql = fmt.Sprintf("%s FROM (%s) AS subquery", selectClause, innerSQL)
	} else {
		sql = fmt.Sprintf("%s FROM `%s`", selectClause, query.Table)
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions)
			sql += whereClause
			args = append(args, whereArgs...)
		}
		if len(query.GroupBy) > 0 {
			var groupByStrs []string
			for _, gb := range query.GroupBy {
				groupByStrs = append(groupByStrs, gb.Value)
			}
			sql += " GROUP BY " + strings.Join(groupByStrs, ", ")
		}
		if len(query.Having) > 0 {
			havingClause, havingArgs := BuildHavingClause(query.Having)
			sql += havingClause
			args = append(args, havingArgs...)
		}
		if len(query.OrderBy) > 0 {
			sql += " ORDER BY "
			orderParts := []string{}
			for _, ob := range query.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.FieldExpr.Value, ob.Direction))
			}
			sql += strings.Join(orderParts, ", ")
		}
		if query.Offset > 0 && query.Limit == 0 {
			sql += fmt.Sprintf(" LIMIT 18446744073709551615 OFFSET %d", query.Offset)
		} else if query.Limit > 0 && query.Offset > 0 {
			sql += fmt.Sprintf(" LIMIT %d OFFSET %d", query.Limit, query.Offset)
		} else if query.Limit > 0 {
			sql += fmt.Sprintf(" LIMIT %d", query.Limit)
		}
	}
	
	return sql, args
}

func BuildWindowSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var selectParts []string
	selectParts = append(selectParts, "*")

	for _, wf := range query.WindowFunctions {
		windowFunc := strings.ToUpper(wf.Function)
		windowFunc = strings.ReplaceAll(windowFunc, " ", "_")
		
		var funcSQL string
		switch windowFunc {
		case "ROW_NUMBER":
			funcSQL = "ROW_NUMBER()"
		case "RANK":
			funcSQL = "RANK()"
		case "DENSE_RANK":
			funcSQL = "DENSE_RANK()"
		case "LAG":
			if wf.Alias != "" {
				funcSQL = fmt.Sprintf("LAG(%s)", wf.Alias)
			} else {
				funcSQL = "LAG(*)"
			}
		case "LEAD":
			if wf.Alias != "" {
				funcSQL = fmt.Sprintf("LEAD(%s)", wf.Alias)
			} else {
				funcSQL = "LEAD(*)"
			}
		case "NTILE":
			buckets := wf.Buckets
			if buckets <= 0 {
				buckets = 4
			}
			funcSQL = fmt.Sprintf("NTILE(%d)", buckets)
		default:
			funcSQL = fmt.Sprintf("%s()", windowFunc)
		}

		overClause := "OVER ("
		var overParts []string

		if len(wf.PartitionBy) > 0 {
			var partitionStrs []string
			for _, pb := range wf.PartitionBy {
				partitionStrs = append(partitionStrs, pb.Value)
			}
			overParts = append(overParts, fmt.Sprintf("PARTITION BY %s", strings.Join(partitionStrs, ", ")))
		}

		if len(wf.OrderBy) > 0 {
			orderParts := []string{}
			for _, ob := range wf.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.FieldExpr.Value, ob.Direction))
			}
			overParts = append(overParts, fmt.Sprintf("ORDER BY %s", strings.Join(orderParts, ", ")))
		}

		overClause += strings.Join(overParts, " ")
		overClause += ")"

		selectParts = append(selectParts, fmt.Sprintf("%s %s", funcSQL, overClause))
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), query.Table)
	var args []interface{}

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions)
		sql += whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

func BuildSetOperationSQL(query *pb.RelationalQuery) (string, []interface{}) {
	setOp := query.SetOperation
	leftSQL, leftArgs := BuildSimpleSelectSQL(setOp.LeftQuery)
	rightSQL, rightArgs := BuildSimpleSelectSQL(setOp.RightQuery)
	
	var operator string
	switch strings.ToUpper(setOp.OperationType) {
	case "UNION":
		operator = "UNION"
	case "UNION_ALL":
		operator = "UNION ALL"
	case "INTERSECT":
		operator = "INTERSECT"
	case "EXCEPT":
		operator = "EXCEPT"
	default:
		operator = "UNION"
	}
	
	sql := fmt.Sprintf("(%s) %s (%s)", leftSQL, operator, rightSQL)
	args := append(leftArgs, rightArgs...)
	return sql, args
}

func BuildSimpleSelectSQL(query *pb.RelationalQuery) (string, []interface{}) {
	sql := fmt.Sprintf("SELECT * FROM %s", query.Table)
	var args []interface{}
	
	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions)
		sql += whereClause
		args = append(args, whereArgs...)
	}
	return sql, args
}

func BuildHavingClause(conditions []*pb.QueryCondition) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}
	clause, args, _ := buildConditionsRecursive(conditions)
	return " HAVING " + clause, args
}

// ============================================================================
// TCL OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildSetTransactionOptionsSQL(query *pb.RelationalQuery) string {
	parts := []string{"SET TRANSACTION"}
	var options []string

	if query.IsolationLevel != "" {
		options = append(options, fmt.Sprintf("ISOLATION LEVEL %s", TranslateIsolationLevel(query.IsolationLevel)))
	}
	if query.ReadOnly {
		options = append(options, "READ ONLY")
	}
	if len(options) > 0 {
		parts = append(parts, strings.Join(options, " "))
	}
	return strings.Join(parts, " ")
}

func BuildSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("SAVEPOINT %s", savepointName), nil
}

func BuildRollbackToSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName), nil
}

func BuildReleaseSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
}

func BuildSetTransactionSQL(isolationLevel string) string {
	return "SET TRANSACTION ISOLATION LEVEL " + TranslateIsolationLevel(isolationLevel)
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
		return "REPEATABLE READ"
	}
}

// ============================================================================
// CTE OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildCTESQL(query *pb.RelationalQuery) (string, []interface{}) {
	if query.Cte == nil {
		return "", nil
	}
	cteSQL, params := BuildSelectSQL(query.Cte.CteQuery)
	return fmt.Sprintf("WITH %s AS (%s) SELECT * FROM %s", query.Cte.CteName, cteSQL, query.Cte.CteName), params
}

// ============================================================================
// SUBQUERY OPERATIONS - SQL BUILDERS
// ============================================================================

func BuildSubquerySQL(query *pb.RelationalQuery) (string, []interface{}) {
	if query.Subquery == nil {
		return "", nil
	}

	subqueryType := strings.ToUpper(query.Subquery.SubqueryType)
	subquerySQL, subArgs := BuildSelectSQL(query.Subquery.Subquery)

	// EXISTS is a standalone existence check
	if subqueryType == "EXISTS" {
		return fmt.Sprintf("SELECT EXISTS(%s)", subquerySQL), subArgs
	}

	// IN subquery requires outer table
	if query.Table == "" {
		return "", nil
	}

	sql := fmt.Sprintf("SELECT * FROM `%s` WHERE ", query.Table)
	var args []interface{}

	if len(query.Conditions) > 0 {
		whereParts := []string{}
		for _, cond := range query.Conditions {
			whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.FieldExpr.Value, cond.Operator))
			args = append(args, cond.ValueExpr.Value)
		}
		sql += strings.Join(whereParts, " AND ") + " AND "
	}

	subField := query.Subquery.FieldExpr.Value
    sql += fmt.Sprintf("%s IN (%s)", subField, subquerySQL)
	args = append(args, subArgs...)

	return sql, args
}