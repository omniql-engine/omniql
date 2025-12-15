package translator

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	mysqlbuilders "github.com/omniql-engine/omniql/engine/builders/mysql"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
	"github.com/jinzhu/inflection"
)

// ============================================================================
// EXPRESSION MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLExpression(expr *models.Expression) *pb.Expression {
	if expr == nil {
		return nil
	}
	return &pb.Expression{
		Type:           expr.Type,
		Value:          expr.Value,
		Left:           mapMySQLExpression(expr.Left),
		Operator:       expr.Operator,
		Right:          mapMySQLExpression(expr.Right),
		FunctionName:   expr.FunctionName,
		FunctionArgs:   mapMySQLExpressions(expr.FunctionArgs),
		CaseConditions: mapMySQLCaseConditions(expr.CaseConditions),
		CaseElse:       mapMySQLExpression(expr.CaseElse),
	}
}

func mapMySQLExpressions(exprs []*models.Expression) []*pb.Expression {
	if len(exprs) == 0 {
		return nil
	}
	var result []*pb.Expression
	for _, expr := range exprs {
		result = append(result, mapMySQLExpression(expr))
	}
	return result
}

func mapMySQLCaseConditions(conditions []*models.CaseCondition) []*pb.CaseCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.CaseCondition
	for _, cc := range conditions {
		result = append(result, &pb.CaseCondition{
			Condition: mapMySQLCondition(cc.Condition),
			ThenExpr:  mapMySQLExpression(cc.ThenExpr),
		})
	}
	return result
}

func mapMySQLCondition(cond *models.Condition) *pb.QueryCondition {
	if cond == nil {
		return nil
	}
	return &pb.QueryCondition{
		FieldExpr:  mapMySQLExpression(cond.FieldExpr),
		Operator:   cond.Operator,
		ValueExpr:  mapMySQLExpression(cond.ValueExpr),
		Value2Expr: mapMySQLExpression(cond.Value2Expr),
		ValuesExpr: mapMySQLExpressions(cond.ValuesExpr),
		Logic:      cond.Logic,
		Nested:     mapMySQLConditions(cond.Nested),
	}
}

func mapMySQLConditions(conditions []models.Condition) []*pb.QueryCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.QueryCondition
	for _, cond := range conditions {
		result = append(result, mapMySQLCondition(&cond))
	}
	return result
}

func mapMySQLOrderByClauses(orderBy []models.OrderBy) []*pb.OrderByClause {
	if len(orderBy) == 0 {
		return nil
	}
	var result []*pb.OrderByClause
	for _, ob := range orderBy {
		result = append(result, &pb.OrderByClause{
			FieldExpr: mapMySQLExpression(ob.FieldExpr),
			Direction: string(ob.Direction),
		})
	}
	return result
}

// ============================================================================
// MAIN TRANSLATOR
// ============================================================================

func TranslateMySQL(query *models.Query, tenantID string) (*pb.RelationalQuery, error) {
	operation := mapping.OperationMap["MySQL"][query.Operation]
	table := getMySQLTableName(query.Entity, query.Operation)
	conditions := mapMySQLConditions(query.Conditions)
	fields := mapMySQLFields(query.Fields)
	
	// DQL: Map fields
	joins := mapMySQLJoins(query.Joins)
	aggregate := mapMySQLAggregate(query.Aggregate)
	orderBy := mapMySQLOrderByClauses(query.OrderBy)
	having := mapMySQLConditions(query.Having)
	
	// DQL: Advanced fields
	windowFunctions := mapMySQLWindowFunctions(query.WindowFunctions)
	cte := mapMySQLCTE(query.CTE, tenantID)
	subquery := mapMySQLSubquery(query.Subquery, tenantID)
	pattern := query.Pattern
	setOperation, err := mapMySQLSetOperation(query.SetOperation, tenantID)
	if err != nil {
		return nil, err
	}
	
	// TCL
	var savepointName, isolationLevel string
	var readOnly bool
	if query.Transaction != nil {
		savepointName = query.Transaction.SavepointName
		isolationLevel = query.Transaction.IsolationLevel
		readOnly = query.Transaction.ReadOnly
	}
	
	// DCL
	var permissions []string
	var permissionTarget, roleName, userName, password string
	var userRoles []string
	if query.Permission != nil {
		permissions = query.Permission.Permissions
		permissionTarget = query.Permission.Target
		roleName = query.Permission.RoleName
		userName = query.Permission.UserName
		password = query.Permission.Password
		userRoles = query.Permission.Roles
	}
	
	// CRUD extensions
	upsert := mapMySQLUpsert(query.Upsert)
	bulkData := mapMySQLBulkData(query.BulkData)

	// DDL
	viewName := query.ViewName
	viewQuery := mapMySQLViewQuery(query.ViewQuery, tenantID)
	databaseName := query.DatabaseName
	newName := query.NewName
	if query.NewName != "" && query.Operation == "RENAME TABLE" {
		lookupOp := strings.ToUpper(strings.ReplaceAll(query.Operation, "_", " "))
		rule := mapping.TableNamingRules[lookupOp]
		if rule == "plural" {
			newName = inflection.Plural(strings.ToLower(query.NewName))
		} else {
			newName = strings.ToLower(query.NewName)
		}
	}
	
	result := &pb.RelationalQuery{
		Operation:  operation,
		Table:      table,
		Conditions: conditions,
		Fields:     fields,
		Limit:      int32(query.Limit),
		Offset:     int32(query.Offset),
		Distinct:   query.Distinct,
		
		// DQL
		Joins:           joins,
		Columns:         mapMySQLExpressions(query.Columns),
		SelectColumns:   mapMySQLSelectColumns(query.SelectColumns),
		Aggregate:       aggregate,
		OrderBy:         orderBy,
		GroupBy:         mapMySQLExpressions(query.GroupBy),
		Having:          having,
		WindowFunctions: windowFunctions,
		Cte:             cte,
		Subquery:        subquery,
		Pattern:         pattern,
		SetOperation:    setOperation,
		
		// TCL
		SavepointName:  savepointName,
		IsolationLevel: isolationLevel,
		ReadOnly:       readOnly,
		
		// DCL
		Permissions:      permissions,
		PermissionTarget: permissionTarget,
		RoleName:         roleName,
		UserName:         userName,
		Password:         password,
		UserRoles:        userRoles,
		
		// CRUD Extensions
		Upsert:   upsert,
		BulkData: bulkData,
		
		// DDL
		ViewName:     viewName,
		ViewQuery:    viewQuery,
		DatabaseName: databaseName,
		NewName:      newName,
		AlterAction:  query.AlterAction,
	}
	
	result.Sql = buildMySQLString(result)
	return result, nil
}

// ============================================================================
// FIELD MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLFields(fields []models.Field) []*pb.QueryField {
	if len(fields) == 0 {
		return nil
	}
	var result []*pb.QueryField
	for _, field := range fields {
		result = append(result, &pb.QueryField{
			NameExpr:    mapMySQLExpression(field.NameExpr),
			ValueExpr:   mapMySQLExpression(field.ValueExpr),
			Constraints: field.Constraints,
		})
	}
	return result
}

func getMySQLTableName(entity string, operation string) string {
	lookupOp := strings.ToUpper(strings.ReplaceAll(operation, "_", " "))
	rule := mapping.TableNamingRules[lookupOp]
	
	if rule == "plural" {
		return inflection.Plural(strings.ToLower(entity))
	}
	if rule == "none" {
		return ""
	}
	return strings.ToLower(entity)
}

// ============================================================================
// CRUD EXTENSIONS (100% TrueAST)
// ============================================================================

func mapMySQLUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	return &pb.UpsertClause{
		ConflictFields: mapMySQLExpressions(upsert.ConflictFields),
		UpdateFields:   mapMySQLFields(upsert.UpdateFields),
		ConflictAction: "UPDATE",
	}
}

func mapMySQLBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	var result []*pb.BulkInsertRow
	for _, row := range bulkData {
		result = append(result, &pb.BulkInsertRow{
			Fields: mapMySQLFields(row),
		})
	}
	return result
}

// ============================================================================
// JOIN MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLJoins(joins []models.Join) []*pb.JoinClause {
	if len(joins) == 0 {
		return nil
	}
	var result []*pb.JoinClause
	for _, join := range joins {
		result = append(result, &pb.JoinClause{
			JoinType:  string(join.Type),
			Table:     strings.ToLower(join.Table) + "s",
			LeftExpr:  mapMySQLExpression(join.LeftExpr),
			RightExpr: mapMySQLExpression(join.RightExpr),
		})
	}
	return result
}

// ============================================================================
// AGGREGATE MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function:  string(agg.Function),
		FieldExpr: mapMySQLExpression(agg.FieldExpr),
	}
}

// ============================================================================
// WINDOW FUNCTIONS (100% TrueAST)
// ============================================================================

func mapMySQLWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	var result []*pb.WindowClause
	for _, wf := range windowFuncs {
		result = append(result, &pb.WindowClause{
			Function:    string(wf.Function),
			FieldExpr:   mapMySQLExpression(wf.FieldExpr),
			Alias:       wf.Alias,
			PartitionBy: mapMySQLExpressions(wf.PartitionBy),
			OrderBy:     mapMySQLOrderByClauses(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		})
	}
	return result
}

// ============================================================================
// CTE MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLCTE(cte *models.CTE, tenantID string) *pb.CTEClause {
	if cte == nil {
		return nil
	}
	var cteQuery *pb.RelationalQuery
	if cte.Query != nil {
		cteQuery, _ = TranslateMySQL(cte.Query, tenantID)
	}
	return &pb.CTEClause{
		CteName:   cte.Name,
		CteQuery:  cteQuery,
		Recursive: cte.Recursive,
	}
}

// ============================================================================
// SUBQUERY MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLSubquery(subquery *models.Subquery, tenantID string) *pb.SubqueryClause {
	if subquery == nil {
		return nil
	}
	var subqueryQuery *pb.RelationalQuery
	if subquery.Query != nil {
		subqueryQuery, _ = TranslateMySQL(subquery.Query, tenantID)
	}
	return &pb.SubqueryClause{
		SubqueryType: subquery.Type,
		FieldExpr:    mapMySQLExpression(subquery.FieldExpr),
		Subquery:     subqueryQuery,
		Alias:        subquery.Alias,
	}
}

// ============================================================================
// SET OPERATION MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLSetOperation(setOp *models.SetOperation, tenantID string) (*pb.SetOperationClause, error) {
	if setOp == nil {
		return nil, nil
	}
	leftQuery, err := TranslateMySQL(setOp.LeftQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate left query: %w", err)
	}
	rightQuery, err := TranslateMySQL(setOp.RightQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate right query: %w", err)
	}
	return &pb.SetOperationClause{
		OperationType: string(setOp.Type),
		LeftQuery:     leftQuery,
		RightQuery:    rightQuery,
	}, nil
}

// ============================================================================
// SELECT COLUMNS MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
	if len(selectCols) == 0 {
		return nil
	}
	var result []*pb.SelectColumn
	for _, col := range selectCols {
		result = append(result, &pb.SelectColumn{
			ExpressionObj: mapMySQLExpression(col.ExpressionObj),
			Alias:         col.Alias,
		})
	}
	return result
}

// ============================================================================
// VIEW QUERY MAPPING (100% TrueAST)
// ============================================================================

func mapMySQLViewQuery(viewQuery *models.Query, tenantID string) *pb.RelationalQuery {
	if viewQuery == nil {
		return nil
	}
	result, _ := TranslateMySQL(viewQuery, tenantID)
	return result
}

// ============================================================================
// SQL STRING BUILDER
// ============================================================================

func buildMySQLString(query *pb.RelationalQuery) string {
	operation := strings.ToLower(query.Operation)
	
	switch operation {
	case "select":
		sql, _ := mysqlbuilders.BuildSelectSQL(query)
		return sql
	case "insert":
		sql, _ := mysqlbuilders.BuildInsertSQL(query)
		return sql
	case "update":
		sql, _ := mysqlbuilders.BuildUpdateSQL(query)
		return sql
	case "delete":
		sql, _ := mysqlbuilders.BuildDeleteSQL(query)
		return sql
	case "upsert":
		sql, _, _ := mysqlbuilders.BuildUpsertSQL(query)
		return sql
	case "replace":
		sql, _ := mysqlbuilders.BuildInsertSQL(query)
		return strings.Replace(sql, "INSERT", "REPLACE", 1)
	case "bulk_insert":
		sql, _, _ := mysqlbuilders.BuildBulkInsertSQL(query)
		return sql
	case "create_table":
		sql, _ := mysqlbuilders.BuildCreateTableSQL(query, mapping.TypeMap)
		return sql
	case "alter_table":
		sql, _ := mysqlbuilders.BuildAlterTableSQL(query, mapping.TypeMap)
		return sql
	case "drop_table":
		sql, _ := mysqlbuilders.BuildDropTableSQL(query)
		return sql
	case "truncate_table":
		sql, _ := mysqlbuilders.BuildTruncateTableSQL(query)
		return sql
	case "alter_table_rename":
		sql, _ := mysqlbuilders.BuildRenameTableSQL(query)
		return sql
	case "create_index":
		sql, _ := mysqlbuilders.BuildCreateIndexSQL(query)
		return sql
	case "drop_index":
		sql, _ := mysqlbuilders.BuildDropIndexSQL(query)
		return sql
	case "create_database":
		sql, _ := mysqlbuilders.BuildCreateDatabaseSQL(query)
		return sql
	case "drop_database":
		sql, _ := mysqlbuilders.BuildDropDatabaseSQL(query)
		return sql
	case "create_view":
		sql, _ := mysqlbuilders.BuildCreateViewSQL(query)
		return sql
	case "drop_view":
		sql, _ := mysqlbuilders.BuildDropViewSQL(query)
		return sql
	case "alter_view":
		sql, _ := mysqlbuilders.BuildAlterViewSQL(query)
		return sql
	case "inner_join", "left_join", "right_join", "full_join", "cross_join":
		sql, _ := mysqlbuilders.BuildJoinSQL(query)
		return sql
	case "count", "sum", "avg", "min", "max":
		sql, _ := mysqlbuilders.BuildAggregateSQL(query)
		return sql
	case "row_number", "rank", "dense_rank", "lag", "lead", "ntile":
		sql, _ := mysqlbuilders.BuildWindowSQL(query)
		return sql
	case "union", "union_all", "intersect", "except":
		sql, _ := mysqlbuilders.BuildSetOperationSQL(query)
		return sql
	case "grant":
		sql, _ := mysqlbuilders.BuildGrantSQL(query, false)
		return sql
	case "revoke":
		sql, _ := mysqlbuilders.BuildRevokeSQL(query, false)
		return sql
	case "create_user":
		sql, _ := mysqlbuilders.BuildCreateUserSQL(query)
		return sql
	case "drop_user":
		sql, _ := mysqlbuilders.BuildDropUserSQL(query)
		return sql
	case "alter_user":
		sql, _ := mysqlbuilders.BuildAlterUserSQL(query)
		return sql
	case "create_role":
		sql, _ := mysqlbuilders.BuildCreateRoleSQL(query)
		return sql
	case "drop_role":
		sql, _ := mysqlbuilders.BuildDropRoleSQL(query)
		return sql
	case "assign_role":
		sql, _ := mysqlbuilders.BuildAssignRoleSQL(query)
		return sql
	case "revoke_role":
		sql, _ := mysqlbuilders.BuildRevokeRoleSQL(query)
		return sql
	case "begin", "start", "start_transaction":
    	return "START TRANSACTION"
	case "commit":
		return "COMMIT"
	case "rollback":
		return "ROLLBACK"
	case "savepoint":
		sql, _ := mysqlbuilders.BuildSavepointSQL(query.SavepointName)
		return sql
	case "rollback_to":
		sql, _ := mysqlbuilders.BuildRollbackToSavepointSQL(query.SavepointName)
		return sql
	case "release_savepoint":
		sql, _ := mysqlbuilders.BuildReleaseSavepointSQL(query.SavepointName)
		return sql
	case "set_transaction":
		return mysqlbuilders.BuildSetTransactionSQL(query.IsolationLevel)
	default:
		return ""
	}
}