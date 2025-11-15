package mysql

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// ============================================================================
// CRUD OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildSelectSQL creates parameterized SELECT query with expression support
func BuildSelectSQL(query *pb.RelationalQuery) (string, []interface{}) {
	selectClause := "SELECT"
	if query.Distinct {
		selectClause = "SELECT DISTINCT"
	}
	
	var args []interface{}
	
	// Check for SelectColumns (expressions with aliases)
	columns := "*"
	if len(query.SelectColumns) > 0 {
		var colParts []string
		for _, col := range query.SelectColumns {
			// Check if this is a CASE WHEN expression
			if col.ExpressionObj != nil && col.ExpressionObj.ExpressionType == "CASEWHEN" {
				// Build CASE WHEN with parameterized values (MySQL uses ?)
				caseSQL := "CASE"
				for _, cond := range col.ExpressionObj.CaseConditions {
					caseSQL += fmt.Sprintf(" WHEN %s THEN ?", cond.Condition)
					args = append(args, cond.ThenValue)
				}
				if col.ExpressionObj.CaseElse != "" {
					caseSQL += " ELSE ?"
					args = append(args, col.ExpressionObj.CaseElse)
				}
				caseSQL += " END"
				
				if col.Alias != "" {
					caseSQL += " AS " + col.Alias
				}
				colParts = append(colParts, caseSQL)
			} else {
				// Regular expression (arithmetic, functions, etc.)
				colStr := col.Expression
				if col.Alias != "" {
					colStr += " AS " + col.Alias
				}
				colParts = append(colParts, colStr)
			}
		}
		columns = strings.Join(colParts, ", ")
	} else if len(query.Columns) > 0 {
		// Fallback to regular columns
		columns = strings.Join(query.Columns, ", ")
	}
	
	sql := fmt.Sprintf("%s %s FROM `%s`", selectClause, columns, query.Table)
	
	// Use helper for WHERE clause
	whereClause, whereArgs := BuildWhereClause(query.Conditions)
	sql += whereClause
	args = append(args, whereArgs...)
	
	// Add ORDER BY
	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}
	
	// Add LIMIT/OFFSET (MySQL requires LIMIT when using OFFSET)
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
		if query.Offset > 0 {
			sql += fmt.Sprintf(" OFFSET %d", query.Offset)
		}
	} else if query.Offset > 0 {
		// MySQL requires LIMIT when using OFFSET
		// Use max value as "unlimited"
		sql += fmt.Sprintf(" LIMIT 18446744073709551615 OFFSET %d", query.Offset)
	}

	return sql, args
}

// BuildInsertSQL creates parameterized INSERT query
func BuildInsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var fields []string
	var placeholders []string
	var args []interface{}

	for _, field := range query.Fields {
		fields = append(fields, field.Name)
		placeholders = append(placeholders, "?")
		args = append(args, ConvertMySQLValue(field.Value))
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "))

	return sql, args
}

// BuildUpdateSQL creates parameterized UPDATE query with expression support
func BuildUpdateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var setParts []string
	var args []interface{}

	for _, field := range query.Fields {
		// Check if field has an expression
		if expr := field.GetExpression(); expr != nil {
			// Build expression SQL
			exprSQL := BuildExpressionSQL(expr)
			setParts = append(setParts, fmt.Sprintf("%s = %s", field.Name, exprSQL))
			// Expressions don't add args (they reference columns directly)
		} else {
			// Regular value assignment
			setParts = append(setParts, fmt.Sprintf("%s = ?", field.Name))
			args = append(args, ConvertMySQLValue(field.Value))
		}
	}

	sql := fmt.Sprintf("UPDATE `%s` SET %s", query.Table, strings.Join(setParts, ", "))

	// Use helper for WHERE clause
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

	// Build INSERT part
	var fields []string
	var placeholders []string
	var args []interface{}

	for _, field := range query.Fields {
		fields = append(fields, field.Name)
		placeholders = append(placeholders, "?")
		args = append(args, ConvertMySQLValue(field.Value))
	}

	// Build ON DUPLICATE KEY UPDATE part
	var updateParts []string
	for _, field := range query.Fields {
		// Skip conflict fields in UPDATE clause
		isConflictField := false
		for _, cf := range query.Upsert.ConflictFields {
			if field.Name == cf {
				isConflictField = true
				break
			}
		}
		if !isConflictField {
			updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", field.Name, field.Name))
		}
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateParts, ", "))

	return sql, args, nil
}

// BuildBulkInsertSQL creates BULK INSERT using multi-row VALUES
func BuildBulkInsertSQL(query *pb.RelationalQuery) (string, []interface{}, error) {
	if len(query.BulkData) == 0 {
		return "", nil, fmt.Errorf("BULK_INSERT requires data rows")
	}

	// Get column names from first row
	firstRow := query.BulkData[0]
	var fields []string
	for _, field := range firstRow.Fields {
		fields = append(fields, field.Name)
	}

	// Build multi-row VALUES
	var valueClauses []string
	var args []interface{}

	for _, row := range query.BulkData {
		placeholders := make([]string, len(row.Fields))
		for i, field := range row.Fields {
			placeholders[i] = "?"
			args = append(args, ConvertMySQLValue(field.Value))
		}
		valueClauses = append(valueClauses, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(valueClauses, ", "))

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

	var whereClauses []string
	var args []interface{}

	for _, cond := range conditions {
		whereClauses = append(whereClauses, fmt.Sprintf("%s %s ?", cond.Field, cond.Operator))
		args = append(args, ConvertMySQLValue(cond.Value))
	}

	return " WHERE " + strings.Join(whereClauses, " AND "), args
}

// BuildExpressionSQL converts a FieldExpression to SQL
func BuildExpressionSQL(expr *pb.FieldExpression) string {
	switch expr.ExpressionType {
	case "BINARY":
		// Binary expression: value + 1, value * 2, etc.
		left := expr.LeftOperand
		if expr.LeftIsField {
			// It's a column reference, use as-is
			left = expr.LeftOperand
		} else {
			// It's a literal, wrap in quotes if string
			left = expr.LeftOperand
		}
		
		right := expr.RightOperand
		if expr.RightIsField {
			// It's a column reference
			right = expr.RightOperand
		} else {
			// It's a literal
			right = expr.RightOperand
		}
		
		return fmt.Sprintf("%s %s %s", left, expr.Operator, right)
		
	case "FUNCTION":
		// Function call: UPPER(name), LOWER(name), etc.
		return fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(expr.FunctionArgs, ", "))
		
	case "CASEWHEN":
		// CASE WHEN statement for UPDATE
		// Note: This embeds values directly (not parameterized) for UPDATE statements
		// SELECT with CASE WHEN is handled separately in BuildSelectSQL with proper parameterization
		var caseParts []string
		caseParts = append(caseParts, "CASE")
		
		for _, cond := range expr.CaseConditions {
			// Simple quoting for UPDATE statements
			thenValue := cond.ThenValue
			// If it's not a number, wrap in quotes
			if _, err := strconv.Atoi(thenValue); err != nil {
				thenValue = fmt.Sprintf("'%s'", thenValue)
			}
			caseParts = append(caseParts, fmt.Sprintf("WHEN %s THEN %s", cond.Condition, thenValue))
		}
		
		if expr.CaseElse != "" {
			elseValue := expr.CaseElse
			// If it's not a number, wrap in quotes
			if _, err := strconv.Atoi(elseValue); err != nil {
				elseValue = fmt.Sprintf("'%s'", elseValue)
			}
			caseParts = append(caseParts, fmt.Sprintf("ELSE %s", elseValue))
		}
		
		caseParts = append(caseParts, "END")
		return strings.Join(caseParts, " ")
		
	default:
		return expr.LeftOperand
	}
}

// ConvertMySQLValue converts values for MySQL compatibility
// Specifically handles boolean strings → integers
func ConvertMySQLValue(value string) interface{} {
	// Convert boolean strings to integers for MySQL BOOLEAN (TINYINT)
	switch strings.ToLower(value) {
	case "true":
		return 1
	case "false":
		return 0
	default:
		return value
	}
}

// ============================================================================
// DCL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildGrantSQL constructs GRANT statement for MySQL
func BuildGrantSQL(query *pb.RelationalQuery, isRole bool) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for GRANT")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user specified for GRANT")
	}

	privileges := TranslatePermissions(query.Permissions)

	var sql string
	if isRole {
		// GRANT to role: no quotes, no @localhost
		sql = fmt.Sprintf("GRANT %s ON %s.* TO %s",
			strings.Join(privileges, ", "),
			query.Table,
			query.PermissionTarget)
	} else {
		// GRANT to user: needs quotes and @localhost
		sql = fmt.Sprintf("GRANT %s ON %s.* TO '%s'@'localhost'",
			strings.Join(privileges, ", "),
			query.Table,
			query.PermissionTarget)
	}

	return sql, nil
}

// BuildRevokeSQL constructs REVOKE statement for MySQL
func BuildRevokeSQL(query *pb.RelationalQuery, isRole bool) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for REVOKE")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user specified for REVOKE")
	}

	privileges := TranslatePermissions(query.Permissions)

	var sql string
	if isRole {
		// REVOKE from role: no quotes, no @localhost
		sql = fmt.Sprintf("REVOKE %s ON %s.* FROM %s",
			strings.Join(privileges, ", "),
			query.Table,
			query.PermissionTarget)
	} else {
		// REVOKE from user: needs quotes and @localhost
		sql = fmt.Sprintf("REVOKE %s ON %s.* FROM '%s'@'localhost'",
			strings.Join(privileges, ", "),
			query.Table,
			query.PermissionTarget)
	}

	return sql, nil
}

// BuildCreateRoleSQL constructs CREATE ROLE statement for MySQL
func BuildCreateRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for CREATE_ROLE")
	}
	return fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", query.RoleName), nil
}

// BuildDropRoleSQL constructs DROP ROLE statement for MySQL
func BuildDropRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for DROP_ROLE")
	}
	return fmt.Sprintf("DROP ROLE IF EXISTS %s", query.RoleName), nil
}

// BuildAssignRoleSQL constructs GRANT role TO user statement for MySQL
func BuildAssignRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for ASSIGN_ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no user name specified for ASSIGN_ROLE")
	}
	return fmt.Sprintf("GRANT '%s' TO '%s'@'localhost'", query.RoleName, query.UserName), nil
}

// BuildRevokeRoleSQL constructs REVOKE role FROM user statement for MySQL
func BuildRevokeRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for REVOKE_ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no user name specified for REVOKE_ROLE")
	}
	return fmt.Sprintf("REVOKE '%s' FROM '%s'@'localhost'", query.RoleName, query.UserName), nil
}

// BuildCreateUserSQL constructs CREATE USER statement for MySQL
func BuildCreateUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for CREATE_USER")
	}
	if query.Password == "" {
		return "", fmt.Errorf("no password specified for CREATE_USER")
	}
	return fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED BY '%s'",
		query.UserName, query.Password), nil
}

// BuildDropUserSQL constructs DROP USER statement for MySQL
func BuildDropUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for DROP_USER")
	}
	return fmt.Sprintf("DROP USER IF EXISTS '%s'@'localhost'", query.UserName), nil
}

// BuildAlterUserSQL constructs ALTER USER statement for MySQL
func BuildAlterUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for ALTER_USER")
	}
	if query.Password == "" {
		return "", fmt.Errorf("no password specified for ALTER_USER")
	}
	return fmt.Sprintf("ALTER USER '%s'@'localhost' IDENTIFIED BY '%s'",
		query.UserName, query.Password), nil
}

// BuildGrantRoleToUserSQL constructs GRANT role TO user for role assignment in ALTER_USER
func BuildGrantRoleToUserSQL(roleName, userName string) string {
	return fmt.Sprintf("GRANT '%s' TO '%s'@'localhost'", roleName, userName)
}

// TranslatePermissions converts OQL permissions to MySQL privileges
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
			// Pass through unknown permissions (might be MySQL-specific)
			privileges = append(privileges, perm)
		}
	}
	return privileges
}

// ============================================================================
// DDL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildCreateTableSQL constructs CREATE TABLE statement for MySQL
func BuildCreateTableSQL(query *pb.RelationalQuery, typeMap map[string]map[string]string) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no columns specified for CREATE TABLE")
	}

	// Build column definitions
	var columns []string
	for _, field := range query.Fields {
		columnDef := TranslateColumn(field.Name, field.Value, typeMap)
		columns = append(columns, columnDef)
	}

	// Build CREATE TABLE SQL
	sql := fmt.Sprintf("CREATE TABLE `%s` (%s)",
		query.Table,
		strings.Join(columns, ", "))

	return sql, nil
}

// BuildAlterTableSQL constructs ALTER TABLE statement for MySQL
func BuildAlterTableSQL(query *pb.RelationalQuery, typeMap map[string]map[string]string) (string, error) {
	if len(query.Conditions) == 0 {
		return "", fmt.Errorf("no ALTER operation specified")
	}

	alterOp := query.Conditions[0].Field
	alterValue := query.Conditions[0].Value

	var sql string

	switch strings.ToUpper(alterOp) {
	case "ADD_COLUMN":
		// Parse: "column_name:TYPE"
		parts := strings.Split(alterValue, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid ADD_COLUMN format: expected 'name:type'")
		}
		columnDef := TranslateColumn(parts[0], parts[1], typeMap)
		sql = fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", query.Table, columnDef)

	case "DROP_COLUMN":
		sql = fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN %s", query.Table, alterValue)

	case "RENAME_COLUMN":
		// Parse: "old_name:new_name"
		parts := strings.Split(alterValue, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid RENAME_COLUMN format: expected 'old:new'")
		}
		// MySQL 8.0+ supports RENAME COLUMN (simpler than CHANGE COLUMN)
		sql = fmt.Sprintf("ALTER TABLE `%s` RENAME COLUMN %s TO %s",
			query.Table, parts[0], parts[1])

	case "RENAME_TABLE":
		sql = fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", query.Table, alterValue)

	default:
		return "", fmt.Errorf("unknown ALTER operation: %s", alterOp)
	}

	return sql, nil
}

// BuildDropTableSQL constructs DROP TABLE statement for MySQL
func BuildDropTableSQL(query *pb.RelationalQuery) (string, error) {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", query.Table), nil
}

// BuildCreateIndexSQL constructs CREATE INDEX statement for MySQL
func BuildCreateIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index details specified")
	}

	indexName := query.Fields[0].Name
	columnName := query.Fields[0].Value

	sql := fmt.Sprintf("CREATE INDEX %s ON `%s` (%s)",
		indexName, query.Table, columnName)

	return sql, nil
}

// BuildDropIndexSQL constructs DROP INDEX statement for MySQL
func BuildDropIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index name specified")
	}

	indexName := query.Fields[0].Name

	// MySQL requires table name for DROP INDEX
	sql := fmt.Sprintf("DROP INDEX %s ON `%s`", indexName, query.Table)

	return sql, nil
}

// BuildTruncateTableSQL constructs TRUNCATE TABLE statement for MySQL
func BuildTruncateTableSQL(query *pb.RelationalQuery) (string, error) {
	return fmt.Sprintf("TRUNCATE TABLE `%s`", query.Table), nil
}

// BuildCreateViewSQL constructs CREATE VIEW statement for MySQL
func BuildCreateViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for CREATE VIEW")
	}
	if query.ViewQuery == "" {
		return "", fmt.Errorf("no query specified for CREATE VIEW")
	}

	return fmt.Sprintf("CREATE VIEW %s AS %s", query.ViewName, query.ViewQuery), nil
}

// BuildAlterViewSQL constructs ALTER VIEW statement for MySQL
// Note: MySQL uses CREATE OR REPLACE VIEW for altering views
func BuildAlterViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for ALTER VIEW")
	}
	if query.ViewQuery == "" {
		return "", fmt.Errorf("no query specified for ALTER VIEW")
	}

	// MySQL: ALTER VIEW is same as CREATE OR REPLACE VIEW
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", query.ViewName, query.ViewQuery), nil
}

// BuildDropViewSQL constructs DROP VIEW statement for MySQL
func BuildDropViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified for DROP VIEW")
	}

	return fmt.Sprintf("DROP VIEW IF EXISTS %s", query.ViewName), nil
}

// BuildRenameTableSQL constructs RENAME TABLE statement for MySQL
func BuildRenameTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.NewName == "" {
		return "", fmt.Errorf("no new name specified for RENAME TABLE")
	}

	return fmt.Sprintf("RENAME TABLE `%s` TO `%s`", query.Table, query.NewName), nil
}

// BuildCreateDatabaseSQL constructs CREATE DATABASE statement for MySQL
func BuildCreateDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified for CREATE DATABASE")
	}

	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", query.DatabaseName), nil
}

// BuildDropDatabaseSQL constructs DROP DATABASE statement for MySQL
func BuildDropDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified for DROP DATABASE")
	}

	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", query.DatabaseName), nil
}

// TranslateColumn translates a column definition using TypeMap
func TranslateColumn(columnName string, columnType string, typeMap map[string]map[string]string) string {
	// Extract base type and parameters
	baseType := columnType
	params := ""

	if idx := strings.Index(columnType, "("); idx != -1 {
		baseType = columnType[:idx]
		endIdx := strings.Index(columnType, ")")
		if endIdx != -1 {
			params = columnType[idx : endIdx+1]
		}
	}

	// Look up MySQL type from TypeMap
	mysqlType, exists := typeMap["MySQL"][strings.ToUpper(baseType)]
	if !exists {
		mysqlType = baseType
	}

	// MySQL AUTO_INCREMENT requires PRIMARY KEY
	if strings.Contains(mysqlType, "AUTO_INCREMENT") {
		return fmt.Sprintf("%s %s%s PRIMARY KEY", columnName, mysqlType, params)
	}

	return fmt.Sprintf("%s %s%s", columnName, mysqlType, params)
}

// ============================================================================
// DQL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildJoinSQL constructs SQL for JOIN operations
func BuildJoinSQL(query *pb.RelationalQuery) (string, []interface{}) {
	// Build SELECT clause - use columns if specified, otherwise SELECT *
	selectClause := "*"
	if len(query.Columns) > 0 {
		selectClause = strings.Join(query.Columns, ", ")
	}
	
	sql := fmt.Sprintf("SELECT %s FROM %s", selectClause, query.Table)
	
	var args []interface{}
	
	// Add JOIN clauses
	for _, join := range query.Joins {
		joinType := strings.ToUpper(strings.Replace(join.JoinType, "_", " ", -1))
		
		// CROSS JOIN doesn't use ON clause - it's a Cartesian product
		if joinType == "CROSS" {
			sql += fmt.Sprintf(" CROSS JOIN %s", join.Table)
		} else {
			// Standard JOINs require ON clause
			sql += fmt.Sprintf(" %s JOIN %s ON %s = %s",
				joinType,
				join.Table,
				join.LeftField,  // Already "users.id" from translator
				join.RightField, // Already "projects.user_id" from translator
			)
		}
	}
	
	// Add WHERE clause
	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions)
		sql += whereClause
		args = append(args, whereArgs...)
	}
	
	// Add ORDER BY
	if len(query.OrderBy) > 0 {
		sql += " ORDER BY "
		orderParts := []string{}
		for _, ob := range query.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
		}
		sql += strings.Join(orderParts, ", ")
	}
	
	// Add LIMIT/OFFSET
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	if query.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", query.Offset)
	}
	
	return sql, args
}

// BuildAggregateSQL constructs SQL for aggregate operations (COUNT, SUM, AVG, MIN, MAX)
func BuildAggregateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var selectClause string
	var args []interface{}
	
	// Build aggregate function
	if query.Aggregate != nil {
		aggFunc := strings.ToUpper(query.Aggregate.Function)
		aggField := query.Aggregate.Field
		
		// Build SELECT clause with DISTINCT support
		if aggField == "" || aggField == "*" {
			// No field specified
			if query.Distinct {
				// COUNT DISTINCT requires a field - can't do COUNT(DISTINCT *)
				selectClause = "SELECT COUNT(*)"
			} else {
				selectClause = fmt.Sprintf("SELECT %s(*)", aggFunc)
			}
		} else {
			// Field specified
			if query.Distinct {
				// DISTINCT aggregation: COUNT(DISTINCT field), SUM(DISTINCT field), etc.
				selectClause = fmt.Sprintf("SELECT %s(DISTINCT %s)", aggFunc, aggField)
			} else {
				// Regular aggregation: COUNT(field), SUM(field), etc.
				selectClause = fmt.Sprintf("SELECT %s(%s)", aggFunc, aggField)
			}
			
			// Add GROUP BY fields to SELECT
			if len(query.GroupBy) > 0 {
				selectClause += ", " + strings.Join(query.GroupBy, ", ")
			}
		}
	} else {
		selectClause = "SELECT COUNT(*)"
	}
	
	// CRITICAL: LIMIT/OFFSET without GROUP BY requires subquery
	// "COUNT LIMIT 7" = count first 7 rows, not all rows
	needsSubquery := (query.Limit > 0 || query.Offset > 0) && len(query.GroupBy) == 0
	
	var sql string
	
	if needsSubquery {
		// Build inner query with LIMIT/OFFSET
		innerSQL := fmt.Sprintf("SELECT * FROM `%s`", query.Table)
		
		// WHERE clause in inner query
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions)
			innerSQL += whereClause
			args = append(args, whereArgs...)
		}
		
		// ORDER BY in inner query (if present)
		if len(query.OrderBy) > 0 {
			innerSQL += " ORDER BY "
			orderParts := []string{}
			for _, ob := range query.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
			}
			innerSQL += strings.Join(orderParts, ", ")
		}
		
		// LIMIT/OFFSET in inner query
		// MySQL: OFFSET requires LIMIT, so if only OFFSET, add large LIMIT
		if query.Offset > 0 && query.Limit == 0 {
			innerSQL += fmt.Sprintf(" LIMIT 18446744073709551615 OFFSET %d", query.Offset)
		} else if query.Limit > 0 && query.Offset > 0 {
			innerSQL += fmt.Sprintf(" LIMIT %d OFFSET %d", query.Limit, query.Offset)
		} else if query.Limit > 0 {
			innerSQL += fmt.Sprintf(" LIMIT %d", query.Limit)
		}
		
		// Aggregate over subquery
		sql = fmt.Sprintf("%s FROM (%s) AS subquery", selectClause, innerSQL)
	} else {
		// Normal aggregate (with GROUP BY or no LIMIT/OFFSET)
		sql = fmt.Sprintf("%s FROM `%s`", selectClause, query.Table)
		
		// WHERE clause
		if len(query.Conditions) > 0 {
			whereClause, whereArgs := BuildWhereClause(query.Conditions)
			sql += whereClause
			args = append(args, whereArgs...)
		}
		
		// GROUP BY clause
		if len(query.GroupBy) > 0 {
			sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
		}
		
		// HAVING clause
		if len(query.Having) > 0 {
			havingClause, havingArgs := BuildHavingClause(query.Having)
			sql += havingClause
			args = append(args, havingArgs...)
		}
		
		// ORDER BY clause (only for grouped results)
		if len(query.OrderBy) > 0 {
			sql += " ORDER BY "
			orderParts := []string{}
			for _, ob := range query.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
			}
			sql += strings.Join(orderParts, ", ")
		}
		
		// LIMIT/OFFSET (for grouped results)
		// MySQL: OFFSET requires LIMIT
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

// BuildWindowSQL constructs SQL for window functions
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
				buckets = 4  // Default to 4 buckets if not specified
			}
			funcSQL = fmt.Sprintf("NTILE(%d)", buckets)
		default:
			funcSQL = fmt.Sprintf("%s()", windowFunc)
		}

		// Build OVER clause
		overClause := "OVER ("
		var overParts []string

		if len(wf.PartitionBy) > 0 {
			overParts = append(overParts, fmt.Sprintf("PARTITION BY %s", strings.Join(wf.PartitionBy, ", ")))
		}

		if len(wf.OrderBy) > 0 {
			orderParts := []string{}
			for _, ob := range wf.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
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

// BuildSetOperationSQL constructs SQL for set operations (UNION, INTERSECT, EXCEPT)
func BuildSetOperationSQL(query *pb.RelationalQuery) (string, []interface{}) {
	setOp := query.SetOperation
	
	// Build left query
	leftSQL, leftArgs := BuildSimpleSelectSQL(setOp.LeftQuery)
	
	// Build right query
	rightSQL, rightArgs := BuildSimpleSelectSQL(setOp.RightQuery)
	
	// Combine with set operator
	var operator string
	switch strings.ToUpper(setOp.OperationType) {
	case "UNION":
		operator = "UNION"
	case "UNION_ALL":
		operator = "UNION ALL"
	case "INTERSECT":
		// MySQL doesn't support INTERSECT directly, use IN subquery
		operator = "INTERSECT"  // Will be converted later if needed
	case "EXCEPT":
		// MySQL doesn't support EXCEPT directly, use NOT IN subquery
		operator = "EXCEPT"  // Will be converted later if needed
	default:
		operator = "UNION"
	}
	
	sql := fmt.Sprintf("(%s) %s (%s)", leftSQL, operator, rightSQL)
	
	// Combine args
	args := append(leftArgs, rightArgs...)
	
	return sql, args
}

// BuildSimpleSelectSQL builds a simple SELECT for set operations
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

// BuildHavingClause creates HAVING clause with ? placeholders
func BuildHavingClause(conditions []*pb.QueryCondition) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}

	var havingClauses []string
	var args []interface{}

	for _, cond := range conditions {
		havingClauses = append(havingClauses, fmt.Sprintf("%s %s ?", cond.Field, cond.Operator))
		args = append(args, cond.Value)
	}

	return " HAVING " + strings.Join(havingClauses, " AND "), args
}

// ============================================================================
// TCL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildSetTransactionOptionsSQL builds SET TRANSACTION statement with options
func BuildSetTransactionOptionsSQL(query *pb.RelationalQuery) string {
	parts := []string{"SET TRANSACTION"}
	var options []string

	// Add isolation level
	if query.IsolationLevel != "" {
		isolationLevel := TranslateIsolationLevel(query.IsolationLevel)
		options = append(options, fmt.Sprintf("ISOLATION LEVEL %s", isolationLevel))
	}

	// Add read-only mode
	if query.ReadOnly {
		options = append(options, "READ ONLY")
	}

	if len(options) > 0 {
		parts = append(parts, strings.Join(options, " "))
	}

	return strings.Join(parts, " ")
}

// BuildSavepointSQL builds SAVEPOINT statement
func BuildSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("SAVEPOINT %s", savepointName), nil
}

// BuildRollbackToSavepointSQL builds ROLLBACK TO SAVEPOINT statement
func BuildRollbackToSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName), nil
}

// BuildReleaseSavepointSQL builds RELEASE SAVEPOINT statement
func BuildReleaseSavepointSQL(savepointName string) (string, error) {
	if savepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
}

// BuildSetTransactionSQL builds SET TRANSACTION ISOLATION LEVEL statement
func BuildSetTransactionSQL(isolationLevel string) string {
	return "SET TRANSACTION ISOLATION LEVEL " + TranslateIsolationLevel(isolationLevel)
}

// TranslateIsolationLevel translates isolation level to MySQL syntax
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
		return "REPEATABLE READ" // MySQL default
	}
}