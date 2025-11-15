package postgres

import (
	"fmt"
	"regexp"
	"strings"
	
	"github.com/omniql-engine/omniql/mapping"  // ← ADD THIS LINE
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// ============================================================================
// CRUD OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildWhereClause creates a parameterized WHERE clause
func BuildWhereClause(conditions []*pb.QueryCondition, startParamNum int) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}

	var whereClauses []string
	var args []interface{}
	paramNum := startParamNum

	for _, cond := range conditions {
		whereClauses = append(whereClauses, fmt.Sprintf("%s %s $%d", cond.Field, cond.Operator, paramNum))
		args = append(args, cond.Value)
		paramNum++
	}

	return " WHERE " + strings.Join(whereClauses, " AND "), args
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
			if col.ExpressionObj != nil && col.ExpressionObj.ExpressionType == "CASEWHEN" {
				caseSQL := "CASE"
				for _, cond := range col.ExpressionObj.CaseConditions {
					caseSQL += fmt.Sprintf(" WHEN %s THEN $%d", cond.Condition, paramNum)
					args = append(args, cond.ThenValue)
					paramNum++
				}
				if col.ExpressionObj.CaseElse != "" {
					caseSQL += fmt.Sprintf(" ELSE $%d", paramNum)
					args = append(args, col.ExpressionObj.CaseElse)
					paramNum++
				}
				caseSQL += " END"
				
				if col.Alias != "" {
					caseSQL += " AS " + col.Alias
				}
				colParts = append(colParts, caseSQL)
			} else {
				colStr := col.Expression
				if col.Alias != "" {
					colStr += " AS " + col.Alias
				}
				colParts = append(colParts, colStr)
			}
		}
		columns = strings.Join(colParts, ", ")
	} else if len(query.Columns) > 0 {
		columns = strings.Join(query.Columns, ", ")
	}
	
	sql := fmt.Sprintf("%s %s FROM %s", selectClause, columns, query.Table)
	whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
	sql += whereClause
	args = append(args, whereArgs...)
	
	if query.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", query.Limit)
	}
	
	if query.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", query.Offset)
	}
	
	return sql, args
}

func BuildInsertSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var fields []string
	var placeholders []string
	var args []interface{}

	for i, field := range query.Fields {
		fields = append(fields, field.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, field.Value)
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "))

	return sql, args
}

func BuildUpdateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	var setParts []string
	var args []interface{}
	paramNum := 1
	
	for _, field := range query.Fields {
		if expr := field.GetExpression(); expr != nil {
			switch expr.ExpressionType {
			case "BINARY":
				var left, right string
				
				if strings.HasPrefix(expr.LeftOperand, "(") && strings.HasSuffix(expr.LeftOperand, ")") {
					left = expr.LeftOperand
				} else if expr.LeftIsField {
					left = expr.LeftOperand
				} else {
					left = fmt.Sprintf("$%d", paramNum)
					args = append(args, expr.LeftOperand)
					paramNum++
				}
				
				if strings.HasPrefix(expr.RightOperand, "(") && strings.HasSuffix(expr.RightOperand, ")") {
					right = expr.RightOperand
				} else if expr.RightIsField {
					right = expr.RightOperand
				} else {
					right = fmt.Sprintf("$%d", paramNum)
					args = append(args, expr.RightOperand)
					paramNum++
				}
				
				setParts = append(setParts, fmt.Sprintf("%s = %s %s %s", 
					field.Name, left, expr.Operator, right))
			
			case "FUNCTION":
				var funcArgs []string
				for _, arg := range expr.FunctionArgs {
					if isIdentifier(arg) {
						funcArgs = append(funcArgs, arg)
					} else {
						funcArgs = append(funcArgs, fmt.Sprintf("$%d", paramNum))
						args = append(args, strings.Trim(arg, "'\""))
						paramNum++
					}
				}
				
				funcSQL := fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(funcArgs, ", "))
				setParts = append(setParts, fmt.Sprintf("%s = %s", field.Name, funcSQL))
			
			case "CASEWHEN":
				caseSQL := "CASE"
				
				for _, cond := range expr.CaseConditions {
					caseSQL += fmt.Sprintf(" WHEN %s THEN ", cond.Condition)
					caseSQL += fmt.Sprintf("$%d", paramNum)
					args = append(args, strings.Trim(cond.ThenValue, "'\""))
					paramNum++
				}
				
				if expr.CaseElse != "" {
					caseSQL += " ELSE "
					caseSQL += fmt.Sprintf("$%d", paramNum)
					args = append(args, strings.Trim(expr.CaseElse, "'\""))
					paramNum++
				}
				
				caseSQL += " END"
				setParts = append(setParts, fmt.Sprintf("%s = %s", field.Name, caseSQL))
			}
		} else {
			literal := field.GetLiteralValue()
			if literal == "" {
				literal = field.Value
			}
			
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field.Name, paramNum))
			args = append(args, literal)
			paramNum++
		}
	}
	
	sql := fmt.Sprintf("UPDATE %s SET %s", query.Table, strings.Join(setParts, ", "))
	
	whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
	sql += whereClause
	args = append(args, whereArgs...)
	
	return sql, args
}

// isIdentifier checks if a string is a column name or a literal value (private helper)
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

	var fields []string
	var placeholders []string
	var args []interface{}

	for i, field := range query.Fields {
		fields = append(fields, field.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, field.Value)
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "))

	if len(query.Upsert.ConflictFields) > 0 {
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ",
			strings.Join(query.Upsert.ConflictFields, ", "))

		var updateParts []string
		for _, field := range query.Upsert.UpdateFields {
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", field.Name, field.Name))
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
		fields = append(fields, field.Name)
	}

	var valueClauses []string
	var args []interface{}
	paramNum := 1

	for _, row := range query.BulkData {
		var placeholders []string
		for _, field := range row.Fields {
			placeholders = append(placeholders, fmt.Sprintf("$%d", paramNum))
			args = append(args, field.Value)
			paramNum++
		}
		valueClauses = append(valueClauses, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		query.Table,
		strings.Join(fields, ", "),
		strings.Join(valueClauses, ", "))

	return sql, args
}

// ============================================================================
// DCL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildGrantSQL constructs GRANT statement
func BuildGrantSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for GRANT")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user/role specified for GRANT")
	}

	privileges := TranslatePermissions(query.Permissions)
	return fmt.Sprintf("GRANT %s ON %s TO %s",
		strings.Join(privileges, ", "),
		query.Table,
		query.PermissionTarget), nil
}

// BuildRevokeSQL constructs REVOKE statement
func BuildRevokeSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Permissions) == 0 {
		return "", fmt.Errorf("no permissions specified for REVOKE")
	}
	if query.PermissionTarget == "" {
		return "", fmt.Errorf("no target user/role specified for REVOKE")
	}

	privileges := TranslatePermissions(query.Permissions)
	return fmt.Sprintf("REVOKE %s ON %s FROM %s",
		strings.Join(privileges, ", "),
		query.Table,
		query.PermissionTarget), nil
}

// BuildCreateUserSQL constructs CREATE USER statement
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

// BuildDropUserSQL constructs DROP USER statement
func BuildDropUserSQL(query *pb.RelationalQuery) (string, error) {
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for DROP USER")
	}
	return fmt.Sprintf("DROP USER IF EXISTS %s", query.UserName), nil
}

// BuildAlterUserSQL constructs ALTER USER statement
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

// BuildCreateRoleSQL constructs CREATE ROLE statement
func BuildCreateRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for CREATE ROLE")
	}
	return fmt.Sprintf("CREATE ROLE %s", query.RoleName), nil
}

// BuildDropRoleSQL constructs DROP ROLE statement
func BuildDropRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for DROP ROLE")
	}
	return fmt.Sprintf("DROP ROLE IF EXISTS %s", query.RoleName), nil
}

// BuildAssignRoleSQL constructs GRANT role TO user statement
func BuildAssignRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for ASSIGN ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for ASSIGN ROLE")
	}
	return fmt.Sprintf("GRANT %s TO %s", query.RoleName, query.UserName), nil
}

// BuildRevokeRoleSQL constructs REVOKE role FROM user statement
func BuildRevokeRoleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RoleName == "" {
		return "", fmt.Errorf("no role name specified for REVOKE ROLE")
	}
	if query.UserName == "" {
		return "", fmt.Errorf("no username specified for REVOKE ROLE")
	}
	return fmt.Sprintf("REVOKE %s FROM %s", query.RoleName, query.UserName), nil
}

// TranslatePermissions converts OQL permissions to PostgreSQL privileges (exported for reuse)
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

// BuildCreateTableSQL constructs SQL for CREATE TABLE
func BuildCreateTableSQL(query *pb.RelationalQuery) string {
	if len(query.Fields) == 0 {
		return ""
	}

	var columns []string
	for _, field := range query.Fields {
		columnDef := buildColumnDefinition(field.Name, field.Value, field.Constraints)
		columns = append(columns, columnDef)
	}

	return fmt.Sprintf("CREATE TABLE %s (%s)",
		query.Table,
		strings.Join(columns, ", "))
}

// BuildAlterTableSQL constructs SQL for ALTER TABLE
func BuildAlterTableSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Conditions) == 0 {
		return "", fmt.Errorf("no ALTER operation specified")
	}

	alterOp := query.Conditions[0].Field
	alterValue := query.Conditions[0].Value

	switch strings.ToUpper(alterOp) {
	case "ADD_COLUMN":
		parts := strings.Split(alterValue, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid ADD_COLUMN format: expected 'name:type'")
		}
		columnDef := buildColumnDefinition(parts[0], parts[1], nil)
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", query.Table, columnDef), nil

	case "DROP_COLUMN":
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", query.Table, alterValue), nil

	case "RENAME_COLUMN":
		parts := strings.Split(alterValue, ":")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid RENAME_COLUMN format: expected 'old:new'")
		}
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			query.Table, parts[0], parts[1]), nil

	case "RENAME_TABLE":
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, alterValue), nil

	default:
		return "", fmt.Errorf("unknown ALTER operation: %s", alterOp)
	}
}

// BuildDropTableSQL constructs SQL for DROP TABLE
func BuildDropTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", query.Table)
}

// BuildTruncateTableSQL constructs SQL for TRUNCATE TABLE
func BuildTruncateTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", query.Table)
}

// BuildRenameTableSQL constructs SQL for RENAME TABLE
func BuildRenameTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.Table == "" {
		return "", fmt.Errorf("no table name specified")
	}
	if query.NewName == "" {
		return "", fmt.Errorf("no new table name specified")
	}
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, query.NewName), nil
}

// BuildCreateIndexSQL constructs SQL for CREATE INDEX
func BuildCreateIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index details specified")
	}

	indexName := query.Fields[0].Name
	columnName := query.Fields[0].Value
	
	indexType := "INDEX"
	if len(query.Fields[0].Constraints) > 0 {
		for _, constraint := range query.Fields[0].Constraints {
			if strings.ToUpper(constraint) == "UNIQUE" {
				indexType = "UNIQUE INDEX"
				break
			}
		}
	}
	
	return fmt.Sprintf("CREATE %s %s ON %s (%s)",
		indexType, indexName, query.Table, columnName), nil
}

// BuildDropIndexSQL constructs SQL for DROP INDEX
func BuildDropIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index name specified")
	}

	indexName := query.Fields[0].Name
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName), nil
}

// BuildCreateDatabaseSQL constructs SQL for CREATE DATABASE
func BuildCreateDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("CREATE DATABASE %s", query.DatabaseName), nil
}

// BuildDropDatabaseSQL constructs SQL for DROP DATABASE
func BuildDropDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", query.DatabaseName), nil
}

// BuildCreateViewSQL constructs SQL for CREATE VIEW
func BuildCreateViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == "" {
		return "", fmt.Errorf("no view query specified")
	}
	return fmt.Sprintf("CREATE VIEW %s AS %s", query.ViewName, query.ViewQuery), nil
}

// BuildDropViewSQL constructs SQL for DROP VIEW
func BuildDropViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	return fmt.Sprintf("DROP VIEW IF EXISTS %s", query.ViewName), nil
}

// BuildAlterViewSQL constructs SQL for ALTER VIEW (CREATE OR REPLACE)
func BuildAlterViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == "" {
		return "", fmt.Errorf("no view query specified")
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", query.ViewName, query.ViewQuery), nil
}

// buildColumnDefinition translates column info to SQL column definition (private helper)
func buildColumnDefinition(name, columnType string, constraints []string) string {
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

	// Look up PostgreSQL type from TypeMap
	pgType := baseType
	if mapping.TypeMap != nil && mapping.TypeMap["PostgreSQL"] != nil {
		if mappedType, exists := mapping.TypeMap["PostgreSQL"][strings.ToUpper(baseType)]; exists {
			pgType = mappedType
		}
	}

	// Build base column definition
	columnDef := fmt.Sprintf("%s %s%s", name, pgType, params)

	// Add PRIMARY KEY for AUTO type
	if strings.ToUpper(baseType) == "AUTO" {
		columnDef = fmt.Sprintf("%s SERIAL PRIMARY KEY", name)
		return columnDef
	}

	// Add constraints
	for _, constraint := range constraints {
		constraintUpper := strings.ToUpper(constraint)
		switch constraintUpper {
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

// BuildJoinSQL constructs SQL for JOIN operations
func BuildJoinSQL(query *pb.RelationalQuery) (string, []interface{}) {
	selectClause := "*"
	if len(query.Columns) > 0 {
		selectClause = strings.Join(query.Columns, ", ")
	}
	
	sql := fmt.Sprintf("SELECT %s FROM %s", selectClause, query.Table)

	var args []interface{}
	paramNum := 1

	for _, join := range query.Joins {
		joinType := strings.ToUpper(join.JoinType)
		sql += fmt.Sprintf(" %s JOIN %s", joinType, join.Table)

		if joinType != "CROSS" {
			sql += fmt.Sprintf(" ON %s = %s", join.LeftField, join.RightField)
		}
	}

	if len(query.Conditions) > 0 {
		whereClause, whereArgs := BuildWhereClause(query.Conditions, paramNum)
		sql += whereClause
		args = append(args, whereArgs...)
	}

	return sql, args
}

// BuildAggregateSQL constructs SQL for aggregate operations
func BuildAggregateSQL(query *pb.RelationalQuery) (string, []interface{}) {
	fmt.Printf("🔍 DEBUG - Aggregate Function: %s\n", query.Aggregate.Function)
	fmt.Printf("🔍 DEBUG - Aggregate Field: %s\n", query.Aggregate.Field)
	fmt.Printf("🔍 DEBUG - GROUP BY: %v\n", query.GroupBy)
	fmt.Printf("🔍 DEBUG - DISTINCT: %v\n", query.Distinct)
	fmt.Printf("🔍 DEBUG - LIMIT: %d\n", query.Limit)
	fmt.Printf("🔍 DEBUG - OFFSET: %d\n", query.Offset)
	
	aggFunc := strings.ToUpper(query.Aggregate.Function)
	aggField := query.Aggregate.Field
	
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
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
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
		if query.Distinct {
			selectClause = "SELECT COUNT(*)"
		} else {
			selectClause = "SELECT COUNT(*)"
		}
	} else {
		if query.Distinct {
			selectClause = fmt.Sprintf("SELECT %s(DISTINCT %s)", aggFunc, aggField)
		} else {
			selectClause = fmt.Sprintf("SELECT %s(%s)", aggFunc, aggField)
		}
		
		if len(query.GroupBy) > 0 {
			selectClause += ", " + strings.Join(query.GroupBy, ", ")
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
		sql += " GROUP BY " + strings.Join(query.GroupBy, ", ")
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
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
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

// BuildWindowFunctionSQL constructs SQL for window functions
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
			overClause += "PARTITION BY " + strings.Join(wf.PartitionBy, ", ")
		}

		if len(wf.OrderBy) > 0 {
			if len(wf.PartitionBy) > 0 {
				overClause += " "
			}
			overClause += "ORDER BY "
			orderParts := []string{}
			for _, ob := range wf.OrderBy {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
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

// BuildCTESQL constructs SQL for CTEs
func BuildCTESQL(query *pb.RelationalQuery) string {
	if query.Cte == nil {
		return ""
	}

	sql := fmt.Sprintf("WITH %s AS (%s) %s",
		query.Cte.CteName,
		query.Cte.CteQuery,
		query.Table,
	)

	return sql
}

// BuildSubquerySQL constructs SQL for subqueries
func BuildSubquerySQL(query *pb.RelationalQuery) (string, []interface{}) {
	subqueryType := strings.ToUpper(query.Subquery.SubqueryType)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE ", query.Table)

	var args []interface{}

	if len(query.Conditions) > 0 {
		whereParts := []string{}
		for _, cond := range query.Conditions {
			whereParts = append(whereParts, fmt.Sprintf("%s %s $%d", cond.Field, cond.Operator, len(args)+1))
			args = append(args, cond.Value)
		}
		sql += strings.Join(whereParts, " AND ") + " AND "
	}

	if subqueryType == "IN" {
		sql += fmt.Sprintf("%s IN (%s)", query.Subquery.Field, query.Subquery.Subquery)
	} else if subqueryType == "EXISTS" {
		sql += fmt.Sprintf("EXISTS (%s)", query.Subquery.Subquery)
	}

	return sql, args
}

// BuildLikeSQL constructs SQL for LIKE operations
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

// BuildCaseSQL constructs SQL for CASE statements
func BuildCaseSQL(query *pb.RelationalQuery) string {
	if query.CaseWhen == nil {
		return ""
	}

	caseSQL := "CASE"

	for _, when := range query.CaseWhen.WhenClauses {
		caseSQL += fmt.Sprintf(" WHEN %s THEN %s", when.Condition, when.ThenValue)
	}

	if query.CaseWhen.ElseValue != "" {
		caseSQL += fmt.Sprintf(" ELSE %s", query.CaseWhen.ElseValue)
	}

	caseSQL += " END"

	alias := query.CaseWhen.Alias
	if alias == "" {
		alias = "case_result"
	}

	sql := fmt.Sprintf("SELECT *, %s AS %s FROM %s", caseSQL, alias, query.Table)

	return sql
}

// BuildSetOperationSQL constructs SQL for set operations
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

	sql := fmt.Sprintf("(%s) %s (%s)", leftSQL, operationType, rightSQL)

	return sql, allArgs
}

// BuildQuerySQL constructs a SELECT query from a RelationalQuery
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
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.Field, ob.Direction))
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

// BuildHavingClause constructs HAVING clause
func BuildHavingClause(conditions []*pb.QueryCondition, startParamNum int) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}

	var havingClauses []string
	var args []interface{}
	paramNum := startParamNum

	for _, cond := range conditions {
		havingClauses = append(havingClauses, fmt.Sprintf("%s %s $%d", cond.Field, cond.Operator, paramNum))
		args = append(args, cond.Value)
		paramNum++
	}

	return " HAVING " + strings.Join(havingClauses, " AND "), args
}


// ============================================================================
// TCL OPERATIONS - SQL BUILDERS
// ============================================================================

// BuildSetTransactionSQL constructs SET TRANSACTION statement for options
func BuildSetTransactionSQL(query *pb.RelationalQuery) (string, error) {
	parts := []string{"SET TRANSACTION"}
	var options []string

	// Add isolation level
	if query.IsolationLevel != "" {
		isolationLevel := TranslateIsolationLevel(query.IsolationLevel)
		options = append(options, fmt.Sprintf("ISOLATION LEVEL %s", isolationLevel))
	}

	// Add read-only/read-write mode
	if query.ReadOnly {
		options = append(options, "READ ONLY")
	}

	if len(options) == 0 {
		return "", fmt.Errorf("no transaction options specified")
	}

	parts = append(parts, strings.Join(options, " "))
	return strings.Join(parts, " "), nil
}

// BuildSavepointSQL constructs SAVEPOINT statement
func BuildSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("SAVEPOINT %s", query.SavepointName), nil
}

// BuildRollbackToSavepointSQL constructs ROLLBACK TO SAVEPOINT statement
func BuildRollbackToSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", query.SavepointName), nil
}

// BuildReleaseSavepointSQL constructs RELEASE SAVEPOINT statement
func BuildReleaseSavepointSQL(query *pb.RelationalQuery) (string, error) {
	if query.SavepointName == "" {
		return "", fmt.Errorf("savepoint name is required")
	}
	return fmt.Sprintf("RELEASE SAVEPOINT %s", query.SavepointName), nil
}

// TranslateIsolationLevel converts OQL isolation level to PostgreSQL format
func TranslateIsolationLevel(level string) string {
	// Normalize input
	level = strings.ToUpper(strings.TrimSpace(level))

	// PostgreSQL isolation levels:
	// - READ UNCOMMITTED (treated as READ COMMITTED by PostgreSQL)
	// - READ COMMITTED (default)
	// - REPEATABLE READ
	// - SERIALIZABLE
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
		return "READ COMMITTED" // Default
	}
}