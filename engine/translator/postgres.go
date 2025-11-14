package translator

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
	"github.com/jinzhu/inflection"
)

// TranslatePostgreSQL converts OQL Query to PostgreSQL RelationalQuery
// Supports all 69 operations: CRUD (7) + DDL (14) + DQL (31) + TCL (8) + DCL (9)
func TranslatePostgreSQL(query *models.Query, tenantID string) (*pb.RelationalQuery, error) {
	operation := mapping.OperationMap["PostgreSQL"][query.Operation]
	table := getPostgreSQLTableName(query.Entity, query.Operation)
	conditions := mapPostgreSQLConditions(query.Conditions)
	fields := mapPostgreSQLFields(query.Fields)
	
	// DQL: Map existing fields
	joins := mapPostgreSQLJoins(query.Joins)
	aggregate := mapPostgreSQLAggregate(query.Aggregate)
	orderBy := mapPostgreSQLOrderBy(query.OrderBy)
	having := mapPostgreSQLConditions(query.Having)
	
	// DQL: Map new advanced fields
	windowFunctions := mapPostgreSQLWindowFunctions(query.WindowFunctions)
	cte := mapPostgreSQLCTE(query.CTE)
	subquery := mapPostgreSQLSubquery(query.Subquery)
	pattern := query.Pattern
	caseWhen := mapPostgreSQLCaseStatement(query.CaseStatement)
	setOperation, err := mapPostgreSQLSetOperation(query.SetOperation, tenantID)
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
	upsert := mapPostgreSQLUpsert(query.Upsert)
	bulkData := mapPostgreSQLBulkData(query.BulkData)
	
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
		SelectColumns:  mapPostgreSQLSelectColumns(query.SelectColumns), 
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

func mapPostgreSQLConditions(conditions []models.Condition) []*pb.QueryCondition {
	var pbConditions []*pb.QueryCondition
	for _, cond := range conditions {
		pbConditions = append(pbConditions, &pb.QueryCondition{
			Field:    convertPostgreSQLConditionField(cond.Field),
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}
	return pbConditions
}

func convertPostgreSQLConditionField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		tableName := strings.ToLower(parts[0]) + "s"
		return tableName + "." + parts[1]
	}
	return strings.ToLower(field)
}

func mapPostgreSQLFields(fields []models.Field) []*pb.QueryField {
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

func getPostgreSQLTableName(entity string, operation string) string {
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

// mapPostgreSQLUpsert converts UPSERT to PostgreSQL ON CONFLICT
func mapPostgreSQLUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	
	return &pb.UpsertClause{
		ConflictFields: upsert.ConflictFields,
		UpdateFields:   mapPostgreSQLFields(upsert.UpdateFields),
		ConflictAction: "UPDATE", // PostgreSQL: ON CONFLICT DO UPDATE
	}
}

// mapPostgreSQLBulkData converts bulk insert rows
func mapPostgreSQLBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	
	var pbBulkRows []*pb.BulkInsertRow
	for _, row := range bulkData {
		pbBulkRows = append(pbBulkRows, &pb.BulkInsertRow{
			Fields: mapPostgreSQLFields(row),
		})
	}
	return pbBulkRows
}

// ============================================================================
// GROUP 3: DQL - JOINS (Existing)
// ============================================================================

func mapPostgreSQLJoins(joins []models.Join) []*pb.JoinClause {
	var pbJoins []*pb.JoinClause
	for _, join := range joins {
		pbJoins = append(pbJoins, &pb.JoinClause{
			JoinType:   string(join.Type),
			Table:      strings.ToLower(join.Table) + "s",
			LeftField:  convertPostgreSQLJoinField(join.LeftField),
			RightField: convertPostgreSQLJoinField(join.RightField),
		})
	}
	return pbJoins
}

func convertPostgreSQLJoinField(field string) string {
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

func mapPostgreSQLAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function: string(agg.Function),
		Field:    agg.Field,
	}
}

func mapPostgreSQLOrderBy(orderBy []models.OrderBy) []*pb.OrderByClause {
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
// ============================================================================

func mapPostgreSQLWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	
	var pbWindows []*pb.WindowClause
	for _, wf := range windowFuncs {
		pbWindow := &pb.WindowClause{
			Function:    string(wf.Function),
			Alias:       wf.Alias,
			PartitionBy: wf.PartitionBy,
			OrderBy:     mapPostgreSQLOrderBy(wf.OrderBy),
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
// ============================================================================

func mapPostgreSQLCTE(cte *models.CTE) *pb.CTEClause {
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

func mapPostgreSQLSubquery(subquery *models.Subquery) *pb.SubqueryClause {
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

func mapPostgreSQLCaseStatement(caseStmt *models.CaseStatement) *pb.CaseClause {
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

func mapPostgreSQLSetOperation(setOp *models.SetOperation, tenantID string) (*pb.SetOperationClause, error) {
	if setOp == nil {
		return nil, nil
	}
	
	// Recursively translate left query
	leftUniversal, err := TranslatePostgreSQL(setOp.LeftQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate left query: %w", err)
	}
	
	// Recursively translate right query
	rightUniversal, err := TranslatePostgreSQL(setOp.RightQuery, tenantID)
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

func mapPostgreSQLSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
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