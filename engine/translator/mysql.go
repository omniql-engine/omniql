package translator

import (
	"fmt"          // ✅ ADD THIS LINE
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
	"github.com/jinzhu/inflection"
)

// TranslateMySQL converts OQL Query to MySQL RelationalQuery
// Supports all 69 operations: CRUD (7) + DDL (14) + DQL (31) + TCL (8) + DCL (9)
// Key differences from PostgreSQL:
// - UPSERT: ON DUPLICATE KEY UPDATE (instead of ON CONFLICT)
// - Transaction: START TRANSACTION (instead of BEGIN)
// - REPLACE: Native MySQL operation
func TranslateMySQL(query *models.Query, tenantID string) (*pb.RelationalQuery, error) {
	operation := mapping.OperationMap["MySQL"][query.Operation]
	table := getMySQLTableName(query.Entity, query.Operation)
	conditions := mapMySQLConditions(query.Conditions)
	fields := mapMySQLFields(query.Fields)
	
	// DQL: Map existing fields
	joins := mapMySQLJoins(query.Joins)
	aggregate := mapMySQLAggregate(query.Aggregate)
	orderBy := mapMySQLOrderBy(query.OrderBy)
	having := mapMySQLConditions(query.Having)
	
	// DQL: Map new advanced fields
	windowFunctions := mapMySQLWindowFunctions(query.WindowFunctions)
	cte := mapMySQLCTE(query.CTE)
	subquery := mapMySQLSubquery(query.Subquery)
	pattern := query.Pattern
	caseWhen := mapMySQLCaseStatement(query.CaseStatement)
	setOperation, err := mapMySQLSetOperation(query.SetOperation, tenantID)
	if err != nil {
		return nil, err
	}
	
	// TCL: Map transaction fields
	var savepointName string
	var isolationLevel string
	var readOnly bool
	if query.Transaction != nil {
		savepointName = query.Transaction.SavepointName
		isolationLevel = query.Transaction.IsolationLevel
		readOnly = query.Transaction.ReadOnly
	}
	
	// DCL: Map permission fields
	var permissions []string
	var permissionTarget string
	var roleName string
	var userName string
	var password string
	var userRoles []string
	if query.Permission != nil {
		permissions = query.Permission.Permissions
		permissionTarget = query.Permission.Target
		roleName = query.Permission.RoleName
		userName = query.Permission.UserName
		password = query.Permission.Password
		userRoles = query.Permission.Roles
	}
	
	// CRUD: Map UPSERT and BULK INSERT
	upsert := mapMySQLUpsert(query.Upsert)
	bulkData := mapMySQLBulkData(query.BulkData)

	// DDL: Map view and database fields
	viewName := query.ViewName
	viewQuery := query.ViewQuery
	databaseName := query.DatabaseName

	// Apply same pluralization to NewName (for RENAME TABLE)
	newName := query.NewName
	if query.NewName != "" && query.Operation == "RENAME TABLE" {
		// Apply same table naming rule as the source table
		lookupOp := strings.ToUpper(strings.ReplaceAll(query.Operation, "_", " "))
		rule := mapping.TableNamingRules[lookupOp]
		if rule == "plural" {
			newName = inflection.Plural(strings.ToLower(query.NewName))
		} else {
			newName = strings.ToLower(query.NewName)
		}
	}
	
	return &pb.RelationalQuery{
		Operation:  operation,
		Table:      table,
		Conditions: conditions,
		Fields:     fields,
		Limit:      int32(query.Limit),
		Offset:     int32(query.Offset),
		Distinct:   query.Distinct,  
		
		// GROUP 3: DQL (existing)
		Joins:     joins,
		Columns:   query.Columns,
		SelectColumns: mapMySQLSelectColumns(query.SelectColumns),
		Aggregate: aggregate,
		OrderBy:   orderBy,
		GroupBy:   query.GroupBy,
		Having:    having,
		
		// GROUP 3: DQL (new advanced features)
		WindowFunctions: windowFunctions,
		Cte:             cte,
		Subquery:        subquery,
		Pattern:         pattern,
		CaseWhen:        caseWhen,
		SetOperation:    setOperation,
		
		// GROUP 4: TCL
		SavepointName:  savepointName,
		IsolationLevel: isolationLevel,
		ReadOnly:       readOnly,
		
		// GROUP 5: DCL
		Permissions:      permissions,
		PermissionTarget: permissionTarget,
		RoleName:         roleName,
		UserName:         userName,
		Password:         password,
		UserRoles:        userRoles,
		
		// CRUD Extensions
		Upsert:   upsert,
		BulkData: bulkData,
		
		// DDL Extensions
		ViewName:     viewName,
		ViewQuery:    viewQuery,
		DatabaseName: databaseName,
		NewName:      newName,
	}, nil
}

// ============================================================================
// CRUD & DDL (Existing)
// ============================================================================

func mapMySQLConditions(conditions []models.Condition) []*pb.QueryCondition {
	var pbConditions []*pb.QueryCondition
	for _, cond := range conditions {
		pbConditions = append(pbConditions, &pb.QueryCondition{
			Field:    convertMySQLConditionField(cond.Field),
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}
	return pbConditions
}

func convertMySQLConditionField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		tableName := strings.ToLower(parts[0]) + "s"
		return tableName + "." + parts[1]
	}
	return strings.ToLower(field)
}

func mapMySQLFields(fields []models.Field) []*pb.QueryField {
	var pbFields []*pb.QueryField
	
	for _, field := range fields {
		pbField := &pb.QueryField{
			Name:        field.Name,
			Constraints: field.Constraints,
		}
		
		// NEW: Handle expressions ONLY if Expression is set
		if field.Expression != nil {
			// Map CaseConditions
			var pbCaseConditions []*pb.CaseCondition
			for _, cc := range field.Expression.CaseConditions {
				pbCaseConditions = append(pbCaseConditions, &pb.CaseCondition{
					Condition: cc.Condition,
					ThenValue: cc.ThenValue,
				})
			}
			
			pbField.FieldType = &pb.QueryField_Expression{
				Expression: &pb.FieldExpression{
					ExpressionType: field.Expression.Type,
					LeftOperand:    field.Expression.LeftOperand,
					Operator:       field.Expression.Operator,
					RightOperand:   field.Expression.RightOperand,
					LeftIsField:    field.Expression.LeftIsField,
					RightIsField:   field.Expression.RightIsField,
					FunctionName:   field.Expression.FunctionName,
					FunctionArgs:   field.Expression.FunctionArgs,
					CaseConditions: pbCaseConditions,
					CaseElse:       field.Expression.CaseElse,
				},
			}
		} else {
			// BACKWARD COMPATIBILITY: Keep using old 'value' field
			// This is critical for CREATE TABLE where Value = type specification
			pbField.Value = field.Value
		}
		
		pbFields = append(pbFields, pbField)
	}
	
	return pbFields
}

// isExpression checks if a value is an expression (contains operators or functions)
func isExpression(value string) bool {
	value = strings.TrimSpace(value)
	
	// Check for arithmetic operators
	if strings.Contains(value, "+") || strings.Contains(value, "-") ||
	   strings.Contains(value, "*") || strings.Contains(value, "/") ||
	   strings.Contains(value, "%") {
		return true
	}
	
	// Check for function calls
	if strings.Contains(value, "(") && strings.Contains(value, ")") {
		return true
	}
	
	// Check for CASE WHEN
	if strings.Contains(strings.ToUpper(value), "CASE") {
		return true
	}
	
	return false
}

func getMySQLTableName(entity string, operation string) string {
	// Convert operation to the format used in TableNamingRules
	// e.g., "create_table" → "CREATE TABLE"
	lookupOp := strings.ToUpper(strings.ReplaceAll(operation, "_", " "))
	
	rule := mapping.TableNamingRules[lookupOp]
	
	if rule == "plural" {
		return inflection.Plural(strings.ToLower(entity))
	}
	
	if rule == "none" {
		return ""
	}
	
	// "exact" or any other value - return as-is (lowercased)
	return strings.ToLower(entity)
}

// ============================================================================
// GROUP 1: CRUD EXTENSIONS
// ============================================================================

// mapMySQLUpsert converts UPSERT to MySQL ON DUPLICATE KEY UPDATE
func mapMySQLUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	
	return &pb.UpsertClause{
		ConflictFields: upsert.ConflictFields,
		UpdateFields:   mapMySQLFields(upsert.UpdateFields),
		ConflictAction: "UPDATE", // MySQL: ON DUPLICATE KEY UPDATE
	}
}

// mapMySQLBulkData converts bulk insert rows
func mapMySQLBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	
	var pbBulkRows []*pb.BulkInsertRow
	for _, row := range bulkData {
		pbBulkRows = append(pbBulkRows, &pb.BulkInsertRow{
			Fields: mapMySQLFields(row),
		})
	}
	return pbBulkRows
}

// ============================================================================
// GROUP 3: DQL - JOINS (Existing)
// ============================================================================

func mapMySQLJoins(joins []models.Join) []*pb.JoinClause {
	var pbJoins []*pb.JoinClause
	for _, join := range joins {
		pbJoins = append(pbJoins, &pb.JoinClause{
			JoinType:   string(join.Type),
			Table:      strings.ToLower(join.Table) + "s",
			LeftField:  convertMySQLJoinField(join.LeftField),
			RightField: convertMySQLJoinField(join.RightField),
		})
	}
	return pbJoins
}

func convertMySQLJoinField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) != 2 {
		return field
	}
	
	tableName := strings.ToLower(parts[0]) + "s"
	columnName := parts[1]
	
	return tableName + "." + columnName
}

// ============================================================================
// GROUP 3: DQL - AGGREGATES (Existing)
// ============================================================================

func mapMySQLAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function: string(agg.Function),
		Field:    agg.Field,
	}
}

func mapMySQLOrderBy(orderBy []models.OrderBy) []*pb.OrderByClause {
	var pbOrderBy []*pb.OrderByClause
	for _, ob := range orderBy {
		pbOrderBy = append(pbOrderBy, &pb.OrderByClause{
			Field:     ob.Field,
			Direction: string(ob.Direction),
		})
	}
	return pbOrderBy
}

// ============================================================================
// GROUP 3: DQL - WINDOW FUNCTIONS (NEW)
// MySQL 8.0+ supports window functions
// ============================================================================

func mapMySQLWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	
	var pbWindows []*pb.WindowClause
	for _, wf := range windowFuncs {
		pbWindow := &pb.WindowClause{
			Function:    string(wf.Function),
			Alias:       wf.Alias,
			PartitionBy: wf.PartitionBy,
			OrderBy:     mapMySQLOrderBy(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		}
		
		// For LAG/LEAD, set the field
		if wf.Function == models.Lag || wf.Function == models.Lead {
			pbWindow.Alias = wf.Field
		}
		
		pbWindows = append(pbWindows, pbWindow)
	}
	
	return pbWindows
}

// ============================================================================
// GROUP 3: DQL - COMMON TABLE EXPRESSIONS (NEW)
// MySQL 8.0+ supports CTEs
// ============================================================================

func mapMySQLCTE(cte *models.CTE) *pb.CTEClause {
	if cte == nil {
		return nil
	}
	
	return &pb.CTEClause{
		CteName:   cte.Name,
		CteQuery:  cte.Query,
		Recursive: cte.Recursive,
	}
}

// ============================================================================
// GROUP 3: DQL - SUBQUERIES (NEW)
// ============================================================================

func mapMySQLSubquery(subquery *models.Subquery) *pb.SubqueryClause {
	if subquery == nil {
		return nil
	}
	
	return &pb.SubqueryClause{
		SubqueryType: subquery.Type,
		Field:        subquery.Field,
		Subquery:     subquery.Query,
		Alias:        subquery.Alias,
	}
}

// ============================================================================
// GROUP 3: DQL - CASE STATEMENTS (NEW)
// ============================================================================

func mapMySQLCaseStatement(caseStmt *models.CaseStatement) *pb.CaseClause {
	if caseStmt == nil {
		return nil
	}
	
	var pbWhenClauses []*pb.CaseWhen
	for _, when := range caseStmt.WhenClauses {
		pbWhenClauses = append(pbWhenClauses, &pb.CaseWhen{
			Condition: when.Condition,
			ThenValue: when.ThenValue,
		})
	}
	
	return &pb.CaseClause{
		WhenClauses: pbWhenClauses,
		ElseValue:   caseStmt.ElseValue,
		Alias:       caseStmt.Alias,
	}
}

// ============================================================================
// GROUP 3: DQL - SET OPERATIONS (NEW)
// ============================================================================

func mapMySQLSetOperation(setOp *models.SetOperation, tenantID string) (*pb.SetOperationClause, error) {
	if setOp == nil {
		return nil, nil
	}
	
	// Recursively translate left query
	leftUniversal, err := TranslateMySQL(setOp.LeftQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate left query: %w", err)
	}
	
	// Recursively translate right query
	rightUniversal, err := TranslateMySQL(setOp.RightQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate right query: %w", err)
	}
	
	return &pb.SetOperationClause{
		OperationType: string(setOp.Type),
		LeftQuery:     leftUniversal,
		RightQuery:    rightUniversal,
	}, nil
}

// ============================================================================
// GROUP 3: DQL - SELECT EXPRESSIONS (NEW)
// ============================================================================

func mapMySQLSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
	if len(selectCols) == 0 {
		return nil
	}
	
	var pbSelectCols []*pb.SelectColumn
	for _, col := range selectCols {
		pbCol := &pb.SelectColumn{
			Expression: col.Expression,
			Alias:      col.Alias,
		}
		
		// Map expression object if present
		if col.ExpressionObj != nil {
			var pbCaseConditions []*pb.CaseCondition
			for _, cc := range col.ExpressionObj.CaseConditions {
				pbCaseConditions = append(pbCaseConditions, &pb.CaseCondition{
					Condition: cc.Condition,
					ThenValue: cc.ThenValue,
				})
			}
			
			pbCol.ExpressionObj = &pb.FieldExpression{
				ExpressionType: col.ExpressionObj.Type,
				LeftOperand:    col.ExpressionObj.LeftOperand,
				Operator:       col.ExpressionObj.Operator,
				RightOperand:   col.ExpressionObj.RightOperand,
				LeftIsField:    col.ExpressionObj.LeftIsField,
				RightIsField:   col.ExpressionObj.RightIsField,
				FunctionName:   col.ExpressionObj.FunctionName,
				FunctionArgs:   col.ExpressionObj.FunctionArgs,
				CaseConditions: pbCaseConditions,
				CaseElse:       col.ExpressionObj.CaseElse,
			}
		}
		
		pbSelectCols = append(pbSelectCols, pbCol)
	}
	
	return pbSelectCols
}