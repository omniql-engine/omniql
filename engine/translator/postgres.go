package translator

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pgbuilders "github.com/omniql-engine/omniql/engine/builders/postgres"
	pb "github.com/omniql-engine/omniql/utilities/proto"
	"github.com/jinzhu/inflection"
)

// ============================================================================
// EXPRESSION MAPPING (100% TrueAST)
// ============================================================================

// mapExpression converts models.Expression to pb.Expression (100% TrueAST)
func mapExpression(expr *models.Expression) *pb.Expression {
	if expr == nil {
		return nil
	}
	
	return &pb.Expression{
		Type:           expr.Type,
		Value:          expr.Value,
		Left:           mapExpression(expr.Left),
		Operator:       expr.Operator,
		Right:          mapExpression(expr.Right),
		FunctionName:   expr.FunctionName,
		FunctionArgs:   mapExpressions(expr.FunctionArgs),
		CaseConditions: mapCaseConditions(expr.CaseConditions),
		CaseElse:       mapExpression(expr.CaseElse),
	}
}

// mapExpressions converts slice of expressions
func mapExpressions(exprs []*models.Expression) []*pb.Expression {
	if len(exprs) == 0 {
		return nil
	}
	var result []*pb.Expression
	for _, expr := range exprs {
		result = append(result, mapExpression(expr))
	}
	return result
}

// mapCaseConditions converts case conditions (100% TrueAST)
func mapCaseConditions(conditions []*models.CaseCondition) []*pb.CaseCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.CaseCondition
	for _, cc := range conditions {
		result = append(result, &pb.CaseCondition{
			Condition: mapCondition(cc.Condition),
			ThenExpr:  mapExpression(cc.ThenExpr),
		})
	}
	return result
}

// mapCondition converts single condition (100% TrueAST)
func mapCondition(cond *models.Condition) *pb.QueryCondition {
	if cond == nil {
		return nil
	}
	return &pb.QueryCondition{
		FieldExpr:  mapExpression(cond.FieldExpr),
		Operator:   cond.Operator,
		ValueExpr:  mapExpression(cond.ValueExpr),
		Value2Expr: mapExpression(cond.Value2Expr),
		ValuesExpr: mapExpressions(cond.ValuesExpr),
		Logic:      cond.Logic,
		Nested:     mapConditions(cond.Nested),
	}
}

// mapConditions converts slice of conditions (100% TrueAST)
func mapConditions(conditions []models.Condition) []*pb.QueryCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.QueryCondition
	for _, cond := range conditions {
		result = append(result, mapCondition(&cond))
	}
	return result
}

// mapOrderByClauses converts order by clauses
func mapOrderByClauses(orderBy []models.OrderBy) []*pb.OrderByClause {
	if len(orderBy) == 0 {
		return nil
	}
	var result []*pb.OrderByClause
	for _, ob := range orderBy {
		result = append(result, &pb.OrderByClause{
			FieldExpr: mapExpression(ob.FieldExpr),
			Direction: string(ob.Direction),
		})
	}
	return result
}

// ============================================================================
// MAIN TRANSLATOR
// ============================================================================

// TranslatePostgreSQL converts OQL Query to PostgreSQL RelationalQuery (100% TrueAST)
func TranslatePostgreSQL(query *models.Query, tenantID string) (*pb.RelationalQuery, error) {
	operation := mapping.OperationMap["PostgreSQL"][query.Operation]
	table := getPostgreSQLTableName(query.Entity, query.Operation)
	conditions := mapConditions(query.Conditions)
	fields := mapFields(query.Fields)
	
	// DQL: Map existing fields
	joins := mapJoins(query.Joins)
	aggregate := mapAggregate(query.Aggregate)
	orderBy := mapOrderByClauses(query.OrderBy)
	having := mapConditions(query.Having)
	
	// DQL: Map new advanced fields
	windowFunctions := mapWindowFunctions(query.WindowFunctions)
	cte := mapCTE(query.CTE, tenantID)
	subquery := mapSubquery(query.Subquery, tenantID)
	pattern := query.Pattern
	setOperation, err := mapSetOperation(query.SetOperation, tenantID)
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
	upsert := mapUpsert(query.Upsert)
	bulkData := mapBulkData(query.BulkData)
	
	// DDL: Map view and database fields
	viewName := query.ViewName
	viewQuery := mapViewQuery(query.ViewQuery, tenantID)
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
		
		// GROUP 3: DQL
		Joins:         joins,
		Columns:       mapExpressions(query.Columns),
		SelectColumns: mapSelectColumns(query.SelectColumns),
		Aggregate:     aggregate,
		OrderBy:       orderBy,
		GroupBy:       mapExpressions(query.GroupBy),
		Having:        having,
		
		// GROUP 3: DQL (advanced)
		WindowFunctions: windowFunctions,
		Cte:             cte,
		Subquery:        subquery,
		Pattern:         pattern,
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
		AlterAction:  query.AlterAction,

		// PostgreSQL DDL
		SequenceName:      query.SequenceName,
		SequenceStart:     query.SequenceStart,
		SequenceIncrement: query.SequenceIncrement,
		SequenceMin:       query.SequenceMin,
		SequenceMax:       query.SequenceMax,
		SequenceCache:     query.SequenceCache,
		SequenceCycle:     query.SequenceCycle,
		SequenceRestart:   query.SequenceRestart,

		ExtensionName: query.ExtensionName,

		SchemaName:  query.SchemaName,
		SchemaOwner: query.SchemaOwner,

		TypeName:     query.TypeName,
		TypeKind:     query.TypeKind,
		EnumValues:   query.EnumValues,
		EnumValue:    query.EnumValue,
		NewEnumValue: query.NewEnumValue,

		DomainName:       query.DomainName,
		DomainType:       query.DomainType,
		DomainDefault:    query.DomainDefault,
		DomainConstraint: query.DomainConstraint,

		FuncName:     query.FuncName,
		FuncBody:     query.FuncBody,
		FuncArgs:     query.FuncArgs,
		FuncReturns:  query.FuncReturns,
		FuncLanguage: query.FuncLanguage,
		FuncOwner:    query.FuncOwner,

		TriggerName:    query.TriggerName,
		TriggerTiming:  query.TriggerTiming,
		TriggerEvents:  query.TriggerEvents,
		TriggerForEach: query.TriggerForEach,

		PolicyName:  query.PolicyName,
		PolicyFor:   query.PolicyFor,
		PolicyTo:    query.PolicyTo,
		PolicyUsing: query.PolicyUsing,
		PolicyCheck: query.PolicyCheck,

		RuleName:   query.RuleName,
		RuleEvent:  query.RuleEvent,
		RuleAction: query.RuleAction,

		CommentTarget: query.CommentTarget,
		CommentText:   query.CommentText,

		Cascade: query.Cascade,
	}
	
	result.Sql = buildPostgreSQLString(result)
	
	return result, nil
}

// ============================================================================
// FIELD MAPPING (100% TrueAST)
// ============================================================================

func mapFields(fields []models.Field) []*pb.QueryField {
	if len(fields) == 0 {
		return nil
	}
	var result []*pb.QueryField
	for _, field := range fields {
		result = append(result, &pb.QueryField{
			NameExpr:    mapExpression(field.NameExpr),
			ValueExpr:   mapExpression(field.ValueExpr),
			Constraints: field.Constraints,
		})
	}
	return result
}

func getPostgreSQLTableName(entity string, operation string) string {
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

func mapUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	return &pb.UpsertClause{
		ConflictFields: mapExpressions(upsert.ConflictFields),
		UpdateFields:   mapFields(upsert.UpdateFields),
		ConflictAction: "UPDATE",
	}
}

func mapBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	var result []*pb.BulkInsertRow
	for _, row := range bulkData {
		result = append(result, &pb.BulkInsertRow{
			Fields: mapFields(row),
		})
	}
	return result
}

// ============================================================================
// JOIN MAPPING (100% TrueAST)
// ============================================================================

func mapJoins(joins []models.Join) []*pb.JoinClause {
	if len(joins) == 0 {
		return nil
	}
	var result []*pb.JoinClause
	for _, join := range joins {
		result = append(result, &pb.JoinClause{
			JoinType:  string(join.Type),
			Table:     strings.ToLower(join.Table) + "s",
			LeftExpr:  mapExpression(join.LeftExpr),
			RightExpr: mapExpression(join.RightExpr),
		})
	}
	return result
}

// ============================================================================
// AGGREGATE MAPPING (100% TrueAST)
// ============================================================================

func mapAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function:  string(agg.Function),
		FieldExpr: mapExpression(agg.FieldExpr),
	}
}

// ============================================================================
// WINDOW FUNCTIONS (100% TrueAST)
// ============================================================================

func mapWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	var result []*pb.WindowClause
	for _, wf := range windowFuncs {
		result = append(result, &pb.WindowClause{
			Function:    string(wf.Function),
			FieldExpr:   mapExpression(wf.FieldExpr),
			Alias:       wf.Alias,
			PartitionBy: mapExpressions(wf.PartitionBy),
			OrderBy:     mapOrderByClauses(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		})
	}
	return result
}

// ============================================================================
// CTE MAPPING (100% TrueAST)
// ============================================================================

func mapCTE(cte *models.CTE, tenantID string) *pb.CTEClause {
	if cte == nil {
		return nil
	}
	
	var cteQuery *pb.RelationalQuery
	if cte.Query != nil {
		cteQuery, _ = TranslatePostgreSQL(cte.Query, tenantID)
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

func mapSubquery(subquery *models.Subquery, tenantID string) *pb.SubqueryClause {
	if subquery == nil {
		return nil
	}
	
	var subqueryQuery *pb.RelationalQuery
	if subquery.Query != nil {
		subqueryQuery, _ = TranslatePostgreSQL(subquery.Query, tenantID)
	}
	
	return &pb.SubqueryClause{
		SubqueryType: subquery.Type,
		FieldExpr:    mapExpression(subquery.FieldExpr),
		Subquery:     subqueryQuery,
		Alias:        subquery.Alias,
	}
}

// ============================================================================
// SET OPERATION MAPPING (100% TrueAST)
// ============================================================================

func mapSetOperation(setOp *models.SetOperation, tenantID string) (*pb.SetOperationClause, error) {
	if setOp == nil {
		return nil, nil
	}
	
	leftQuery, err := TranslatePostgreSQL(setOp.LeftQuery, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate left query: %w", err)
	}
	
	rightQuery, err := TranslatePostgreSQL(setOp.RightQuery, tenantID)
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

func mapSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
	if len(selectCols) == 0 {
		return nil
	}
	var result []*pb.SelectColumn
	for _, col := range selectCols {
		result = append(result, &pb.SelectColumn{
			ExpressionObj: mapExpression(col.ExpressionObj),
			Alias:         col.Alias,
		})
	}
	return result
}

// ============================================================================
// VIEW QUERY MAPPING (100% TrueAST)
// ============================================================================

func mapViewQuery(viewQuery *models.Query, tenantID string) *pb.RelationalQuery {
	if viewQuery == nil {
		return nil
	}
	result, _ := TranslatePostgreSQL(viewQuery, tenantID)
	return result
}

// ============================================================================
// SQL STRING BUILDER (unchanged)
// ============================================================================

func buildPostgreSQLString(query *pb.RelationalQuery) string {
	operation := strings.ToLower(query.Operation)
	
	switch operation {
	case "select":
		sql, _ := pgbuilders.BuildSelectSQL(query)
		return sql
	case "insert":
		sql, _ := pgbuilders.BuildInsertSQL(query)
		return sql
	case "update":
		sql, _ := pgbuilders.BuildUpdateSQL(query)
		return sql
	case "delete":
		sql, _ := pgbuilders.BuildDeleteSQL(query)
		return sql
	case "upsert", "replace":
		sql, _ := pgbuilders.BuildUpsertSQL(query)
		return sql
	case "bulk_insert":
		sql, _ := pgbuilders.BuildBulkInsertSQL(query)
		return sql
	case "create_table":
		return pgbuilders.BuildCreateTableSQL(query)
	case "alter_table":
		sql, _ := pgbuilders.BuildAlterTableSQL(query)
		return sql
	case "drop_table":
		return pgbuilders.BuildDropTableSQL(query)
	case "truncate_table":
		return pgbuilders.BuildTruncateTableSQL(query)
	case "alter_table_rename":
		sql, _ := pgbuilders.BuildRenameTableSQL(query)
		return sql
	case "create_index":
		sql, _ := pgbuilders.BuildCreateIndexSQL(query)
		return sql
	case "drop_index":
		sql, _ := pgbuilders.BuildDropIndexSQL(query)
		return sql
	case "create_database":
		sql, _ := pgbuilders.BuildCreateDatabaseSQL(query)
		return sql
	case "drop_database":
		sql, _ := pgbuilders.BuildDropDatabaseSQL(query)
		return sql
	case "create_view":
		sql, _ := pgbuilders.BuildCreateViewSQL(query)
		return sql
	case "drop_view":
		sql, _ := pgbuilders.BuildDropViewSQL(query)
		return sql
	case "alter_view":
		sql, _ := pgbuilders.BuildAlterViewSQL(query)
		return sql
	case "inner_join", "left_join", "right_join", "full_join", "cross_join":
		sql, _ := pgbuilders.BuildJoinSQL(query)
		return sql
	case "count", "sum", "avg", "min", "max":
		sql, _ := pgbuilders.BuildAggregateSQL(query)
		return sql
	case "row_number", "rank", "dense_rank", "lag", "lead", "ntile":
		sql, _ := pgbuilders.BuildWindowFunctionSQL(query)
		return sql
	case "with":
		sql, _ := pgbuilders.BuildCTESQL(query)
		return sql
	case "subquery", "exists":
		sql, _ := pgbuilders.BuildSubquerySQL(query)
		return sql
	case "like":
		sql, _ := pgbuilders.BuildLikeSQL(query)
		return sql
	case "case":
		return pgbuilders.BuildCaseSQL(query)
	case "union", "union_all", "intersect", "except":
		sql, _ := pgbuilders.BuildSetOperationSQL(query)
		return sql
	case "grant":
		sql, _ := pgbuilders.BuildGrantSQL(query)
		return sql
	case "revoke":
		sql, _ := pgbuilders.BuildRevokeSQL(query)
		return sql
	case "create_user":
		sql, _ := pgbuilders.BuildCreateUserSQL(query)
		return sql
	case "drop_user":
		sql, _ := pgbuilders.BuildDropUserSQL(query)
		return sql
	case "alter_user":
		sql, _ := pgbuilders.BuildAlterUserSQL(query)
		return sql
	case "create_role":
		sql, _ := pgbuilders.BuildCreateRoleSQL(query)
		return sql
	case "drop_role":
		sql, _ := pgbuilders.BuildDropRoleSQL(query)
		return sql
	case "assign_role":
		sql, _ := pgbuilders.BuildAssignRoleSQL(query)
		return sql
	case "revoke_role":
		sql, _ := pgbuilders.BuildRevokeRoleSQL(query)
		return sql
	case "begin", "start":
		return "BEGIN"
	case "commit":
		return "COMMIT"
	case "rollback":
		return "ROLLBACK"
	case "savepoint":
		sql, _ := pgbuilders.BuildSavepointSQL(query)
		return sql
	case "rollback_to":
		sql, _ := pgbuilders.BuildRollbackToSavepointSQL(query)
		return sql
	case "release_savepoint":
		sql, _ := pgbuilders.BuildReleaseSavepointSQL(query)
		return sql
	case "set_transaction":
		sql, _ := pgbuilders.BuildSetTransactionSQL(query)
		return sql

	// PostgreSQL-specific DDL
	case "create_sequence":
		sql, _ := pgbuilders.BuildCreateSequenceSQL(query)
		return sql
	case "alter_sequence":
		sql, _ := pgbuilders.BuildAlterSequenceSQL(query)
		return sql
	case "drop_sequence":
		sql, _ := pgbuilders.BuildDropSequenceSQL(query)
		return sql
	case "create_extension":
		sql, _ := pgbuilders.BuildCreateExtensionSQL(query)
		return sql
	case "drop_extension":
		sql, _ := pgbuilders.BuildDropExtensionSQL(query)
		return sql
	case "create_schema":
		sql, _ := pgbuilders.BuildCreateSchemaSQL(query)
		return sql
	case "drop_schema":
		sql, _ := pgbuilders.BuildDropSchemaSQL(query)
		return sql
	case "create_type":
		sql, _ := pgbuilders.BuildCreateTypeSQL(query)
		return sql
	case "alter_type":
		sql, _ := pgbuilders.BuildAlterTypeSQL(query)
		return sql
	case "drop_type":
		sql, _ := pgbuilders.BuildDropTypeSQL(query)
		return sql
	case "create_domain":
		sql, _ := pgbuilders.BuildCreateDomainSQL(query)
		return sql
	case "drop_domain":
		sql, _ := pgbuilders.BuildDropDomainSQL(query)
		return sql
	case "create_function":
		sql, _ := pgbuilders.BuildCreateFunctionSQL(query)
		return sql
	case "alter_function":
		sql, _ := pgbuilders.BuildAlterFunctionSQL(query)
		return sql
	case "drop_function":
		sql, _ := pgbuilders.BuildDropFunctionSQL(query)
		return sql
	case "create_trigger":
		sql, _ := pgbuilders.BuildCreateTriggerSQL(query)
		return sql
	case "drop_trigger":
		sql, _ := pgbuilders.BuildDropTriggerSQL(query)
		return sql
	case "create_policy":
		sql, _ := pgbuilders.BuildCreatePolicySQL(query)
		return sql
	case "drop_policy":
		sql, _ := pgbuilders.BuildDropPolicySQL(query)
		return sql
	case "create_rule":
		sql, _ := pgbuilders.BuildCreateRuleSQL(query)
		return sql
	case "drop_rule":
		sql, _ := pgbuilders.BuildDropRuleSQL(query)
		return sql
	case "comment_on":
		sql, _ := pgbuilders.BuildCommentOnSQL(query)
		return sql
	default:
		return ""
	}
}