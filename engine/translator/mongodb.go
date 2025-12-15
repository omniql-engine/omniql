package translator

import (
	"encoding/json"
	"fmt"
	"strings"
	
	"github.com/omniql-engine/omniql/mapping"
	mongobuilders "github.com/omniql-engine/omniql/engine/builders/mongodb"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
	
	"github.com/jinzhu/inflection"
	"go.mongodb.org/mongo-driver/bson"
)

// ============================================================================
// EXPRESSION MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBExpression(expr *models.Expression) *pb.Expression {
	if expr == nil {
		return nil
	}
	return &pb.Expression{
		Type:           expr.Type,
		Value:          expr.Value,
		Left:           mapMongoDBExpression(expr.Left),
		Operator:       expr.Operator,
		Right:          mapMongoDBExpression(expr.Right),
		FunctionName:   expr.FunctionName,
		FunctionArgs:   mapMongoDBExpressions(expr.FunctionArgs),
		CaseConditions: mapMongoDBCaseConditions(expr.CaseConditions),
		CaseElse:       mapMongoDBExpression(expr.CaseElse),
	}
}

func mapMongoDBExpressions(exprs []*models.Expression) []*pb.Expression {
	if len(exprs) == 0 {
		return nil
	}
	var result []*pb.Expression
	for _, expr := range exprs {
		result = append(result, mapMongoDBExpression(expr))
	}
	return result
}

func mapMongoDBCaseConditions(conditions []*models.CaseCondition) []*pb.CaseCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.CaseCondition
	for _, cc := range conditions {
		result = append(result, &pb.CaseCondition{
			Condition: mapMongoDBCondition(cc.Condition),
			ThenExpr:  mapMongoDBExpression(cc.ThenExpr),
		})
	}
	return result
}

func mapMongoDBCondition(cond *models.Condition) *pb.QueryCondition {
	if cond == nil {
		return nil
	}
	
	// Convert field expression and apply MongoDB operator conversion
	fieldExpr := mapMongoDBExpression(cond.FieldExpr)
	
	return &pb.QueryCondition{
		FieldExpr:  fieldExpr,
		Operator:   convertMongoDBOperator(cond.Operator),
		ValueExpr:  mapMongoDBExpression(cond.ValueExpr),
		Value2Expr: mapMongoDBExpression(cond.Value2Expr),
		ValuesExpr: mapMongoDBExpressions(cond.ValuesExpr),
		Logic:      cond.Logic,
		Nested:     mapMongoDBConditions(cond.Nested),
	}
}

func mapMongoDBConditions(conditions []models.Condition) []*pb.QueryCondition {
	if len(conditions) == 0 {
		return nil
	}
	var result []*pb.QueryCondition
	for _, cond := range conditions {
		result = append(result, mapMongoDBCondition(&cond))
	}
	return result
}

func mapMongoDBOrderByClauses(orderBy []models.OrderBy) []*pb.OrderByClause {
	if len(orderBy) == 0 {
		return nil
	}
	var result []*pb.OrderByClause
	for _, ob := range orderBy {
		direction := "1"
		if ob.Direction == "DESC" || ob.Direction == models.Desc {
			direction = "-1"
		}
		result = append(result, &pb.OrderByClause{
			FieldExpr: mapMongoDBExpression(ob.FieldExpr),
			Direction: direction,
		})
	}
	return result
}

// ============================================================================
// MAIN TRANSLATOR
// ============================================================================

func TranslateMongoDB(query *models.Query, tenantID string) (*pb.DocumentQuery, error) {
	operation := mapping.OperationMap["MongoDB"][query.Operation]
	collection := getMongoDBCollectionName(query.Entity, query.Operation)
	conditions := mapMongoDBConditions(query.Conditions)
	fields := mapMongoDBFields(query.Fields)
	
	joins := mapMongoDBJoins(query.Joins)
	aggregate := mapMongoDBAggregate(query.Aggregate)
	orderBy := mapMongoDBOrderByClauses(query.OrderBy)
	windowFunctions := mapMongoDBWindowFunctions(query.WindowFunctions)
	pattern := query.Pattern
	
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
	upsert := mapMongoDBUpsert(query.Upsert)
	bulkData := mapMongoDBBulkData(query.BulkData)
	viewName := query.ViewName
	viewQuery := mapMongoDBViewQuery(query.ViewQuery, tenantID)
	databaseName := query.DatabaseName

	// SET OPERATIONS
	if query.SetOperation != nil {
		operation = mapping.OperationMap["MongoDB"][string(query.SetOperation.Type)]
		
		if query.SetOperation.Type == models.Intersect {
			leftConditions := mapMongoDBConditions(query.SetOperation.LeftQuery.Conditions)
			rightConditions := mapMongoDBConditions(query.SetOperation.RightQuery.Conditions)
			conditions = append(leftConditions, rightConditions...)
		} else if query.SetOperation.Type == models.Except {
			leftConditions := mapMongoDBConditions(query.SetOperation.LeftQuery.Conditions)
			rightConditions := mapMongoDBConditions(query.SetOperation.RightQuery.Conditions)
			
			conditions = leftConditions
			for _, rightCond := range rightConditions {
				negatedCond := &pb.QueryCondition{
					FieldExpr: rightCond.FieldExpr,
					Operator:  negateOperator(rightCond.Operator),
					ValueExpr: rightCond.ValueExpr,
				}
				conditions = append(conditions, negatedCond)
			}
		}
	}
	
	result := &pb.DocumentQuery{
		Operation:  operation,
		Collection: collection,
		Conditions: conditions,
		Fields:     fields,
		Limit:      int32(query.Limit),
		Skip:       int32(query.Offset),
		
		Joins:           joins,
		Columns:         mapMongoDBExpressions(query.Columns),
		SelectColumns:   mapMongoDBSelectColumns(query.SelectColumns),
		Aggregate:       aggregate,
		OrderBy:         orderBy,
		GroupBy:         mapMongoDBExpressions(query.GroupBy),
		Having:          mapMongoDBConditions(query.Having),
		Distinct:        query.Distinct,
		
		WindowFunctions: windowFunctions,
		Pattern:         pattern,
		
		SavepointName:  savepointName,
		IsolationLevel: isolationLevel,
		ReadOnly:       readOnly,
		
		Permissions:      permissions,
		PermissionTarget: permissionTarget,
		RoleName:         roleName,
		UserName:         userName,
		Password:         password,
		UserRoles:        userRoles,
		
		Upsert:       upsert,
		BulkData:     bulkData,
		ViewName:     viewName,
		ViewQuery:    viewQuery,
		DatabaseName: databaseName,
		NewName:      query.NewName,
	}

	result.Query = buildMongoDBString(result)
	return result, nil
}

// ============================================================================
// FIELD MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBFields(fields []models.Field) []*pb.QueryField {
	if len(fields) == 0 {
		return nil
	}
	var result []*pb.QueryField
	for _, field := range fields {
		result = append(result, &pb.QueryField{
			NameExpr:    mapMongoDBExpression(field.NameExpr),
			ValueExpr:   mapMongoDBExpression(field.ValueExpr),
			Constraints: field.Constraints,
		})
	}
	return result
}

func getMongoDBCollectionName(entity string, operation string) string {
	rule := mapping.TableNamingRules[operation]
	if rule == "plural" {
		return inflection.Plural(strings.ToLower(entity))
	}
	if rule == "none" {
		return ""
	}
	return strings.ToLower(entity)
}

// ============================================================================
// OPERATOR CONVERSION
// ============================================================================

func convertMongoDBOperator(operator string) string {
	switch operator {
	case "=":
		return "$eq"
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	case "!=":
		return "$ne"
	case "IN":
		return "$in"
	case "NOT_IN":
		return "$nin"
	case "BETWEEN":
		return "BETWEEN"
	case "NOT_BETWEEN":
		return "NOT_BETWEEN"
	case "IS_NULL":
		return "IS_NULL"
	case "IS_NOT_NULL":
		return "IS_NOT_NULL"
	case "LIKE":
		return "$regex"
	default:
		return operator
	}
}

func negateOperator(operator string) string {
	switch operator {
	case "$gt":
		return "$lte"
	case "$gte":
		return "$lt"
	case "$lt":
		return "$gte"
	case "$lte":
		return "$gt"
	case "$eq":
		return "$ne"
	case "$ne":
		return "$eq"
	default:
		return operator
	}
}

// ============================================================================
// CRUD EXTENSIONS (100% TrueAST)
// ============================================================================

func mapMongoDBUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	return &pb.UpsertClause{
		ConflictFields: mapMongoDBExpressions(upsert.ConflictFields),
		UpdateFields:   mapMongoDBFields(upsert.UpdateFields),
		ConflictAction: "UPSERT",
	}
}

func mapMongoDBBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	var result []*pb.BulkInsertRow
	for _, row := range bulkData {
		result = append(result, &pb.BulkInsertRow{
			Fields: mapMongoDBFields(row),
		})
	}
	return result
}

// ============================================================================
// JOIN MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBJoins(joins []models.Join) []*pb.JoinClause {
	if len(joins) == 0 {
		return nil
	}
	var result []*pb.JoinClause
	for _, join := range joins {
		joinType := string(join.Type)
		if joinType != "LEFT" && joinType != "INNER" {
			joinType = "LEFT"
		}
		result = append(result, &pb.JoinClause{
			JoinType:  joinType,
			Table:     strings.ToLower(join.Table) + "s",
			LeftExpr:  mapMongoDBExpression(join.LeftExpr),
			RightExpr: mapMongoDBExpression(join.RightExpr),
		})
	}
	return result
}

// ============================================================================
// AGGREGATE MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function:  convertMongoDBAggregateFunction(string(agg.Function)),
		FieldExpr: mapMongoDBExpression(agg.FieldExpr),
	}
}

func convertMongoDBAggregateFunction(function string) string {
	switch strings.ToUpper(function) {
	case "COUNT":
		return "count"
	case "SUM":
		return "sum"
	case "AVG":
		return "avg"
	case "MIN":
		return "min"
	case "MAX":
		return "max"
	default:
		return strings.ToLower(function)
	}
}

// ============================================================================
// WINDOW FUNCTIONS (100% TrueAST)
// ============================================================================

func mapMongoDBWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	var result []*pb.WindowClause
	for _, wf := range windowFuncs {
		alias := wf.Alias
		if alias == "" {
			alias = strings.ToLower(string(wf.Function)) + "_result"
		}
		result = append(result, &pb.WindowClause{
			Function:    convertMongoDBWindowFunction(string(wf.Function)),
			FieldExpr:   mapMongoDBExpression(wf.FieldExpr),
			Alias:       alias,
			PartitionBy: mapMongoDBExpressions(wf.PartitionBy),
			OrderBy:     mapMongoDBOrderByClauses(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		})
	}
	return result
}

func convertMongoDBWindowFunction(function string) string {
	switch function {
	case "ROW NUMBER", "ROW_NUMBER":
		return "$documentNumber"
	case "RANK":
		return "$rank"
	case "DENSE RANK", "DENSE_RANK":
		return "$denseRank"
	case "LAG":
		return "$shift"
	case "LEAD":
		return "$shift"
	default:
		return function
	}
}

// ============================================================================
// SELECT COLUMNS MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
	if len(selectCols) == 0 {
		return nil
	}
	var result []*pb.SelectColumn
	for _, col := range selectCols {
		result = append(result, &pb.SelectColumn{
			ExpressionObj: mapMongoDBExpression(col.ExpressionObj),
			Alias:         col.Alias,
		})
	}
	return result
}

// ============================================================================
// VIEW QUERY MAPPING (100% TrueAST)
// ============================================================================

func mapMongoDBViewQuery(viewQuery *models.Query, tenantID string) *pb.DocumentQuery {
	if viewQuery == nil {
		return nil
	}
	result, _ := TranslateMongoDB(viewQuery, tenantID)
	return result
}

// ============================================================================
// QUERY STRING BUILDER
// ============================================================================

func buildMongoDBString(query *pb.DocumentQuery) string {
	operation := strings.ToLower(query.Operation)
	
	switch operation {
	case "find":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		cmd := bson.M{"find": query.Collection, "filter": filter}
		
		if query.Limit > 0 {
			cmd["limit"] = query.Limit
		}
		if query.Skip > 0 {
			cmd["skip"] = query.Skip
		}
		if len(query.OrderBy) > 0 {
			sortStage := mongobuilders.BuildMongoDBSortStage(query.OrderBy)
			if sortFields, ok := sortStage["$sort"]; ok {
				cmd["sort"] = sortFields
			} else {
				cmd["sort"] = sortStage
			}
		}
    
    jsonBytes, _ := json.Marshal(cmd)
    return string(jsonBytes)
		
	case "insertone":
		doc := mongobuilders.BuildMongoDocument(query.Fields)
		jsonBytes, _ := json.Marshal(bson.M{"insertOne": query.Collection, "document": doc})
		return string(jsonBytes)
		
	case "updateone":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		update := mongobuilders.BuildMongoSimpleUpdate(query.Fields)
		jsonBytes, _ := json.Marshal(bson.M{"updateOne": query.Collection, "filter": filter, "update": update})
		return string(jsonBytes)
		
	case "deleteone":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"deleteOne": query.Collection, "filter": filter})
		return string(jsonBytes)
		
	case "insertmany":
		docs := []bson.M{}
		for _, row := range query.BulkData {
			doc := mongobuilders.BuildMongoDocument(row.Fields)
			docs = append(docs, doc)
		}
		jsonBytes, _ := json.Marshal(bson.M{"insertMany": query.Collection, "documents": docs})
		return string(jsonBytes)
		
	case "replaceone":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		doc := mongobuilders.BuildMongoDocument(query.Fields)
		jsonBytes, _ := json.Marshal(bson.M{"replaceOne": query.Collection, "filter": filter, "replacement": doc})
		return string(jsonBytes)
		
	case "createcollection":
		jsonBytes, _ := json.Marshal(bson.M{"create": query.Collection})
		return string(jsonBytes)
		
	case "dropcollection":
		jsonBytes, _ := json.Marshal(bson.M{"drop": query.Collection})
		return string(jsonBytes)
		
	case "renamecollection":
		jsonBytes, _ := json.Marshal(bson.M{"renameCollection": query.Collection, "to": query.NewName})
		return string(jsonBytes)
		
	case "deletemany":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"deleteMany": query.Collection, "filter": filter})
		return string(jsonBytes)
		
	case "create_index":
		jsonBytes, _ := json.Marshal(bson.M{"createIndexes": query.Collection})
		return string(jsonBytes)
		
	case "drop_index":
		jsonBytes, _ := json.Marshal(bson.M{"dropIndexes": query.Collection})
		return string(jsonBytes)
		
	case "use":
		return fmt.Sprintf(`{"use": "%s"}`, query.DatabaseName)
		
	case "drop_database":
		return fmt.Sprintf(`{"dropDatabase": "%s"}`, query.DatabaseName)
		
	case "create_view":
		cmd, _ := mongobuilders.BuildCreateViewCommand(query.ViewName, query.Collection)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "drop_view":
		jsonBytes, _ := json.Marshal(bson.M{"drop": query.ViewName})
		return string(jsonBytes)
		
	case "alter_view":
		cmd, _ := mongobuilders.BuildCreateViewCommand(query.ViewName, query.Collection)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "lookup":
		pipeline := mongobuilders.BuildMongoDBJoinPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	case "count", "sum", "avg", "min", "max":
		pipeline := mongobuilders.BuildMongoDBAggregatePipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	case "row_number", "rank", "dense_rank", "shift", "ntile":
		pipeline, _ := mongobuilders.BuildWindowFunctionPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	case "unionwith", "intersect", "setdifference":
		pipeline, _ := mongobuilders.BuildSetOperationPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	case "group":
		pipeline := mongobuilders.BuildMongoDBAggregatePipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	case "sort":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		sort := mongobuilders.BuildMongoDBSortStage(query.OrderBy)
		jsonBytes, _ := json.Marshal(bson.M{"find": query.Collection, "filter": filter, "sort": sort})
		return string(jsonBytes)
		
	case "match":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"$match": filter})
		return string(jsonBytes)
		
	case "distinct":
		field := ""
		if len(query.Columns) > 0 {
			field = query.Columns[0].Value
		}
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"distinct": query.Collection, "key": field, "query": filter})
		return string(jsonBytes)
		
	case "limit":
		return fmt.Sprintf(`{"limit": %d}`, query.Limit)
		
	case "skip":
		return fmt.Sprintf(`{"skip": %d}`, query.Skip)
		
	case "regex":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"find": query.Collection, "filter": filter})
		return string(jsonBytes)
		
	case "cond":
		return `{"$cond": "see aggregation pipeline"}`
		
	case "start_transaction":
		return `{"startTransaction": true}`
		
	case "commit":
		return `{"commitTransaction": true}`
		
	case "abort":
		return `{"abortTransaction": true}`
		
	case "set_transaction":
		return fmt.Sprintf(`{"startTransaction": {"readConcern": {"level": "%s"}}}`, query.IsolationLevel)
		
	case "create_user":
		cmd, _ := mongobuilders.BuildCreateUserCommand(query.UserName, query.Password)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "drop_user":
		cmd, _ := mongobuilders.BuildDropUserCommand(query.UserName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "alter_user":
		cmd, _ := mongobuilders.BuildAlterUserCommand(query.UserName, query.Password)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "create_role":
		cmd, _ := mongobuilders.BuildCreateRoleCommand(query.RoleName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "drop_role":
		cmd, _ := mongobuilders.BuildDropRoleCommand(query.RoleName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "grant_role":
		cmd, _ := mongobuilders.BuildGrantRoleCommand(query.UserName, query.RoleName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "revoke_role":
		cmd, _ := mongobuilders.BuildRevokeRoleCommand(query.UserName, query.RoleName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "grant":
		cmd, _ := mongobuilders.BuildGrantCommand(query.UserName, query.Permissions, query.PermissionTarget, query.DatabaseName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "revoke":
		cmd, _ := mongobuilders.BuildRevokeCommand(query.UserName, query.Permissions, query.PermissionTarget, query.DatabaseName)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	default:
		return ""
	}
}