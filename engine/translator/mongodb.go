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

// TranslateMongoDB converts OQL Query to MongoDB DocumentQuery
func TranslateMongoDB(query *models.Query, tenantID string) (*pb.DocumentQuery, error) {

	operation := mapping.OperationMap["MongoDB"][query.Operation]

	collection := getMongoDBCollectionName(query.Entity, query.Operation)
	conditions := mapMongoDBConditions(query.Conditions)
	fields := mapMongoDBFields(query.Fields)
	
	joins := mapMongoDBJoins(query.Joins)
	aggregate := mapMongoDBAggregate(query.Aggregate)
	orderBy := mapMongoDBOrderBy(query.OrderBy)
	windowFunctions := mapMongoDBWindowFunctions(query.WindowFunctions)
	pattern := query.Pattern
	caseWhen := mapMongoDBCaseStatement(query.CaseStatement)
	
	var savepointName string
	var isolationLevel string
	var readOnly bool
	if query.Transaction != nil {
		savepointName = query.Transaction.SavepointName
		isolationLevel = query.Transaction.IsolationLevel
		readOnly = query.Transaction.ReadOnly
	}
	
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
	
	upsert := mapMongoDBUpsert(query.Upsert)
	bulkData := mapMongoDBBulkData(query.BulkData)
	viewName := query.ViewName
	viewQuery := query.ViewQuery
	databaseName := query.DatabaseName

	// SET OPERATIONS: Combine conditions for INTERSECT and EXCEPT
	if query.SetOperation != nil {
		operation = mapping.OperationMap["MongoDB"][string(query.SetOperation.Type)]
		
		if query.SetOperation.Type == models.Intersect {
			// INTERSECT: Combine conditions with AND logic
			leftConditions := mapMongoDBConditions(query.SetOperation.LeftQuery.Conditions)
			rightConditions := mapMongoDBConditions(query.SetOperation.RightQuery.Conditions)
			conditions = append(leftConditions, rightConditions...)
		} else if query.SetOperation.Type == models.Except {
			// EXCEPT: First query conditions AND NOT second query conditions
			leftConditions := mapMongoDBConditions(query.SetOperation.LeftQuery.Conditions)
			rightConditions := mapMongoDBConditions(query.SetOperation.RightQuery.Conditions)
			
			conditions = leftConditions
			
			// Negate right conditions by flipping operators
			for _, rightCond := range rightConditions {
				negatedCond := &pb.QueryCondition{
					Field:    rightCond.Field,
					Operator: negateOperator(rightCond.Operator),
					Value:    rightCond.Value,
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
		
		Joins:     joins,
		Columns:   query.Columns,
		SelectColumns:  mapMongoDBSelectColumns(query.SelectColumns),
		Aggregate: aggregate,
		OrderBy:   orderBy,
		GroupBy:   query.GroupBy,
		Having:    mapMongoDBConditions(query.Having),   
		Distinct:  query.Distinct,                         
		
		WindowFunctions: windowFunctions,
		Pattern:         pattern,
		CaseWhen:        caseWhen,
		
		SavepointName:  savepointName,
		IsolationLevel: isolationLevel,
		ReadOnly:       readOnly,
		
		Permissions:      permissions,
		PermissionTarget: permissionTarget,
		RoleName:         roleName,
		UserName:         userName,
		Password:         password,
		UserRoles:        userRoles,
		
		Upsert:   upsert,
		BulkData: bulkData,
		
		ViewName:     viewName,
		ViewQuery:    viewQuery,
		DatabaseName: databaseName,
		NewName:      query.NewName,
	}

		// ✨ Populate Query field for OmniQL users
	result.Query = buildMongoDBString(result)

	return result, nil
}

func mapMongoDBConditions(conditions []models.Condition) []*pb.QueryCondition {
	var pbConditions []*pb.QueryCondition
	for _, cond := range conditions {
		pbConditions = append(pbConditions, &pb.QueryCondition{
			Field:    convertMongoDBField(cond.Field),
			Operator: convertMongoDBOperator(cond.Operator),
			Value:    cond.Value,
		})
	}
	return pbConditions
}

func convertMongoDBField(field string) string {
	return field
}

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
	case "LIKE":
		return "$regex"
	default:
		return operator
	}
}

func mapMongoDBFields(fields []models.Field) []*pb.QueryField {
	var pbFields []*pb.QueryField
	for _, field := range fields {
		pbField := &pb.QueryField{
			Name:        field.Name,
			Value:       field.Value,
			Constraints: field.Constraints,  // ← ADD THIS LINE!
		}
		
		// Handle expressions (added support for UPDATE with expressions)
		if field.Expression != nil {
			pbField.FieldType = &pb.QueryField_Expression{
				Expression: mapMongoDBExpression(field.Expression),
			}
		} else {
			pbField.FieldType = &pb.QueryField_LiteralValue{
				LiteralValue: field.Value,
			}
		}
		
		pbFields = append(pbFields, pbField)
	}
	return pbFields
}

// mapMongoDBExpression converts OQL expressions to protobuf FieldExpression
func mapMongoDBExpression(expr *models.FieldExpression) *pb.FieldExpression {
	if expr == nil {
		return nil
	}
	
	pbExpr := &pb.FieldExpression{
		ExpressionType: string(expr.Type),
		LeftOperand:    expr.LeftOperand,
		Operator:       expr.Operator,
		RightOperand:   expr.RightOperand,
		LeftIsField:    expr.LeftIsField,
		RightIsField:   expr.RightIsField,
		FunctionName:   expr.FunctionName,
		FunctionArgs:   expr.FunctionArgs,
	}
	
	// ✅ ADD THIS: Copy CaseConditions for CASE WHEN expressions
	if expr.Type == "CASEWHEN" {
		for _, cc := range expr.CaseConditions {
			pbExpr.CaseConditions = append(pbExpr.CaseConditions, &pb.CaseCondition{
				Condition: cc.Condition,
				ThenValue: cc.ThenValue,
			})
		}
		pbExpr.CaseElse = expr.CaseElse
	}
	
	return pbExpr
}

// mapMongoDBSelectColumns converts OQL SelectColumns to protobuf SelectColumns
func mapMongoDBSelectColumns(selectCols []models.SelectColumn) []*pb.SelectColumn {
	if len(selectCols) == 0 {
		return nil
	}
	
	var pbSelectCols []*pb.SelectColumn
	for _, col := range selectCols {
		pbCol := &pb.SelectColumn{
			Expression: col.Expression,
			Alias:      col.Alias,
		}
		
		// Map the expression object if present
		if col.ExpressionObj != nil {
			pbCol.ExpressionObj = mapMongoDBExpression(col.ExpressionObj)
		}
		
		pbSelectCols = append(pbSelectCols, pbCol)
	}
	
	return pbSelectCols
}

func getMongoDBCollectionName(entity string, operation string) string {
	rule := mapping.TableNamingRules[operation]
	
	if rule == "plural" {
		return inflection.Plural(strings.ToLower(entity))  // ← CHANGE THIS LINE
	}
	
	if rule == "none" {
		return ""
	}
	
	return strings.ToLower(entity)
}

func mapMongoDBUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	
	return &pb.UpsertClause{
		ConflictFields: upsert.ConflictFields,
		UpdateFields:   mapMongoDBFields(upsert.UpdateFields),
		ConflictAction: "UPSERT",
	}
}

func mapMongoDBBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	
	var pbBulkRows []*pb.BulkInsertRow
	for _, row := range bulkData {
		pbBulkRows = append(pbBulkRows, &pb.BulkInsertRow{
			Fields: mapMongoDBFields(row),
		})
	}
	return pbBulkRows
}

func mapMongoDBJoins(joins []models.Join) []*pb.JoinClause {
	var pbJoins []*pb.JoinClause
	for _, join := range joins {
		joinType := string(join.Type)
		if joinType != "LEFT" && joinType != "INNER" {
			joinType = "LEFT"
		}
		
		pbJoins = append(pbJoins, &pb.JoinClause{
			JoinType:   joinType,
			Table:      strings.ToLower(join.Table) + "s",
			LeftField:  convertMongoDBJoinField(join.LeftField),
			RightField: convertMongoDBJoinField(join.RightField),
		})
	}
	return pbJoins
}

func convertMongoDBJoinField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		return parts[1]
	}
	return field
}

func mapMongoDBAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	
	return &pb.AggregateClause{
		Function: convertMongoDBAggregateFunction(string(agg.Function)),
		Field:    agg.Field,
	}
}

func convertMongoDBAggregateFunction(function string) string {
	// Return lowercase function name, NOT MongoDB operator
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

func mapMongoDBOrderBy(orderBy []models.OrderBy) []*pb.OrderByClause {
	var pbOrderBy []*pb.OrderByClause
	for _, ob := range orderBy {
		direction := "1"
		if ob.Direction == models.Descending {
			direction = "-1"
		}
		
		pbOrderBy = append(pbOrderBy, &pb.OrderByClause{
			Field:     ob.Field,
			Direction: direction,
		})
	}
	return pbOrderBy
}

func mapMongoDBWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	
	var pbWindows []*pb.WindowClause
	for _, wf := range windowFuncs {
		alias := wf.Alias
		
		if wf.Function == models.Lag || wf.Function == models.Lead {
			if wf.Field != "" {
				alias = wf.Field
			}
		}
		
		if alias == "" {
			alias = strings.ToLower(string(wf.Function)) + "_result"
		}
		
		pbWindow := &pb.WindowClause{
			Function:    convertMongoDBWindowFunction(string(wf.Function)),
			Alias:       alias,
			PartitionBy: wf.PartitionBy,
			OrderBy:     mapMongoDBOrderBy(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		}
		
		pbWindows = append(pbWindows, pbWindow)
	}
	
	return pbWindows
}

func convertMongoDBWindowFunction(function string) string {
	switch function {
	case "ROW NUMBER":
		return "$documentNumber"
	case "RANK":
		return "$rank"
	case "DENSE RANK":
		return "$denseRank"
	case "LAG":
		return "$shift"
	case "LEAD":
		return "$shift"
	default:
		return function
	}
}

func mapMongoDBCaseStatement(caseStmt *models.CaseStatement) *pb.CaseClause {
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

// negateOperator flips an operator for EXCEPT operation
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

// buildMongoDBString generates MongoDB query JSON for OmniQL users
func buildMongoDBString(query *pb.DocumentQuery) string {
	operation := strings.ToLower(query.Operation)
	
	switch operation {
	// CRUD Operations
	case "find":
		filter := mongobuilders.BuildMongoFilter(query.Conditions)
		jsonBytes, _ := json.Marshal(bson.M{"find": query.Collection, "filter": filter})
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
		
	// DDL Operations
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
		collection, _ := mongobuilders.ExtractCollectionNameFromQuery(query.ViewQuery)
		cmd, _ := mongobuilders.BuildCreateViewCommand(query.ViewName, collection)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	case "drop_view":
		jsonBytes, _ := json.Marshal(bson.M{"drop": query.ViewName})
		return string(jsonBytes)
		
	case "alter_view":
		collection, _ := mongobuilders.ExtractCollectionNameFromQuery(query.ViewQuery)
		cmd, _ := mongobuilders.BuildCreateViewCommand(query.ViewName, collection)
		jsonBytes, _ := json.Marshal(cmd)
		return string(jsonBytes)
		
	// DQL Operations - Joins
	case "lookup":
		pipeline := mongobuilders.BuildMongoDBJoinPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	// DQL Operations - Aggregates
	case "count", "sum", "avg", "min", "max":
		pipeline := mongobuilders.BuildMongoDBAggregatePipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	// DQL Operations - Window Functions
	case "row_number", "rank", "dense_rank", "shift", "ntile":
		pipeline, _ := mongobuilders.BuildWindowFunctionPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	// DQL Operations - Set Operations
	case "unionwith", "intersect", "setdifference":
		pipeline, _ := mongobuilders.BuildSetOperationPipeline(query)
		jsonBytes, _ := json.Marshal(bson.M{"aggregate": query.Collection, "pipeline": pipeline})
		return string(jsonBytes)
		
	// DQL Operations - Others
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
			field = query.Columns[0]
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
		// CASE WHEN - handled in aggregation pipeline
		return `{"$cond": "see aggregation pipeline"}`
		
	// TCL Operations
	case "start_transaction":
		return `{"startTransaction": true}`
		
	case "commit":
		return `{"commitTransaction": true}`
		
	case "abort":
		return `{"abortTransaction": true}`
		
	case "set_transaction":
		return fmt.Sprintf(`{"startTransaction": {"readConcern": {"level": "%s"}}}`, query.IsolationLevel)
		
	// DCL Operations
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
		return "" // Unknown operation
	}
}