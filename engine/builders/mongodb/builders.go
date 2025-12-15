package mongodb

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"        
	"go.mongodb.org/mongo-driver/mongo/readconcern"    
	"go.mongodb.org/mongo-driver/mongo/writeconcern"   

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
// FILTER BUILDING
// ============================================================================

// BuildMongoFilter builds a BSON filter from conditions with full operator support
func BuildMongoFilter(conditions []*pb.QueryCondition) bson.M {
	if len(conditions) == 0 {
		return bson.M{}
	}

	hasOrLogic := false
	for _, cond := range conditions {
		if cond.Logic == "OR" {
			hasOrLogic = true
			break
		}
	}

	if hasOrLogic || hasNestedConditions(conditions) {
		return buildFilterRecursive(conditions)
	}

	filter := bson.M{}
	for _, cond := range conditions {
		addConditionToFilter(filter, cond)
	}
	return filter
}

func hasNestedConditions(conditions []*pb.QueryCondition) bool {
	for _, cond := range conditions {
		if len(cond.Nested) > 0 {
			return true
		}
	}
	return false
}

func buildFilterRecursive(conditions []*pb.QueryCondition) bson.M {
	if len(conditions) == 0 {
		return bson.M{}
	}

	if len(conditions) == 1 {
		cond := conditions[0]
		if len(cond.Nested) > 0 {
			return buildFilterRecursive(cond.Nested)
		}
		return buildSingleConditionFilter(cond)
	}

	var andGroups []bson.M
	var currentOrGroup []bson.M

	for i, cond := range conditions {
		var condFilter bson.M
		if len(cond.Nested) > 0 {
			condFilter = buildFilterRecursive(cond.Nested)
		} else {
			condFilter = buildSingleConditionFilter(cond)
		}

		if i == 0 || cond.Logic == "" || cond.Logic == "AND" {
			if len(currentOrGroup) > 0 {
				if len(currentOrGroup) == 1 {
					andGroups = append(andGroups, currentOrGroup[0])
				} else {
					andGroups = append(andGroups, bson.M{"$or": currentOrGroup})
				}
				currentOrGroup = nil
			}
			andGroups = append(andGroups, condFilter)
		} else if cond.Logic == "OR" {
			if len(andGroups) > 0 {
				lastAnd := andGroups[len(andGroups)-1]
				andGroups = andGroups[:len(andGroups)-1]
				currentOrGroup = append(currentOrGroup, lastAnd)
			}
			currentOrGroup = append(currentOrGroup, condFilter)
		}
	}

	if len(currentOrGroup) > 0 {
		if len(currentOrGroup) == 1 {
			andGroups = append(andGroups, currentOrGroup[0])
		} else {
			andGroups = append(andGroups, bson.M{"$or": currentOrGroup})
		}
	}

	if len(andGroups) == 1 {
		return andGroups[0]
	}
	return bson.M{"$and": andGroups}
}

func buildSingleConditionFilter(cond *pb.QueryCondition) bson.M {
	// TrueAST: Handle BINARY/FUNCTION expressions via AST traversal
	if cond.FieldExpr != nil && (cond.FieldExpr.Type == "BINARY" || cond.FieldExpr.Type == "FUNCTION") {
		leftExpr := BuildMongoExpressionFromAST(cond.FieldExpr)
		rightValue := ParseMongoValue(cond.ValueExpr.Value)

		var compOp string
		switch cond.Operator {
		case "$gt", ">":
			compOp = "$gt"
		case "$gte", ">=":
			compOp = "$gte"
		case "$lt", "<":
			compOp = "$lt"
		case "$lte", "<=":
			compOp = "$lte"
		case "$ne", "!=":
			compOp = "$ne"
		default:
			compOp = "$eq"
		}
		return bson.M{"$expr": bson.M{compOp: bson.A{leftExpr, rightValue}}}
	}

	field := cond.FieldExpr.Value
	operator := cond.Operator

	// Legacy fallback for string-based expressions
	if IsFieldExpression(field) {
		return bson.M{"$expr": BuildExprCondition(cond)}
	}

	switch operator {
	case "IS_NULL":
		return bson.M{field: bson.M{"$eq": nil}}
	case "IS_NOT_NULL":
		return bson.M{field: bson.M{"$ne": nil}}
	case "$in":
		values := exprSliceToStrings(cond.ValuesExpr)
		return bson.M{field: bson.M{"$in": parseMongoValues(values)}}
	case "$nin":
		values := exprSliceToStrings(cond.ValuesExpr)
		return bson.M{field: bson.M{"$nin": parseMongoValues(values)}}
	case "BETWEEN":
		v1 := ParseMongoValue(cond.ValueExpr.Value)
		v2 := ParseMongoValue(cond.Value2Expr.Value)
		return bson.M{field: bson.M{"$gte": v1, "$lte": v2}}
	case "NOT_BETWEEN":
		v1 := ParseMongoValue(cond.ValueExpr.Value)
		v2 := ParseMongoValue(cond.Value2Expr.Value)
		return bson.M{"$or": bson.A{
			bson.M{field: bson.M{"$lt": v1}},
			bson.M{field: bson.M{"$gt": v2}},
		}}
	case "$eq":
		return bson.M{field: ParseMongoValue(cond.ValueExpr.Value)}
	case "$regex":
		pattern := cond.ValueExpr.Value
		pattern = strings.ReplaceAll(pattern, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		return bson.M{field: bson.M{"$regex": pattern, "$options": "i"}}
	default:
		return bson.M{field: bson.M{operator: ParseMongoValue(cond.ValueExpr.Value)}}
	}
}

// exprSliceToStrings extracts Value strings from Expression slice
func exprSliceToStrings(exprs []*pb.Expression) []string {
	result := make([]string, len(exprs))
	for i, e := range exprs {
		result[i] = e.Value
	}
	return result
}

func parseMongoValues(values []string) bson.A {
	result := bson.A{}
	for _, v := range values {
		result = append(result, ParseMongoValue(v))
	}
	return result
}

func addConditionToFilter(filter bson.M, cond *pb.QueryCondition) {
	singleFilter := buildSingleConditionFilter(cond)
	for k, v := range singleFilter {
		filter[k] = v
	}
}

// ============================================================================
// DOCUMENT BUILDING
// ============================================================================

func BuildMongoDocument(fields []*pb.QueryField) bson.M {
	document := bson.M{}
	for _, field := range fields {
		document[field.NameExpr.Value] = ParseMongoValue(field.ValueExpr.Value)
	}
	return document
}

// ============================================================================
// UPDATE BUILDING - SIMPLE
// ============================================================================

func BuildMongoSimpleUpdate(fields []*pb.QueryField) bson.M {
	update := bson.M{}
	setFields := bson.M{}
	incFields := bson.M{}
	mulFields := bson.M{}
	
	for _, field := range fields {
		fieldName := field.NameExpr.Value
		
		if field.ValueExpr != nil && field.ValueExpr.Type == "BINARY" {
			expr := field.ValueExpr
			if expr.Left != nil && expr.Left.Type == "FIELD" && expr.Right != nil && expr.Right.Type != "FIELD" {
				rightValue := ParseMongoValue(expr.Right.Value)
				
				switch expr.Operator {
				case "+":
					incFields[fieldName] = rightValue
				case "-":
					if numValue, ok := rightValue.(int); ok {
						incFields[fieldName] = -numValue
					}
				case "*":
					mulFields[fieldName] = rightValue
				case "/":
					if numValue, ok := rightValue.(int); ok && numValue != 0 {
						mulFields[fieldName] = 1.0 / float64(numValue)
					}
				default:
					setFields[fieldName] = field.ValueExpr.Value
				}
			} else {
				setFields[fieldName] = field.ValueExpr.Value
			}
		} else {
			setFields[fieldName] = ParseMongoValue(field.ValueExpr.Value)
		}
	}
	
	if len(setFields) > 0 {
		update["$set"] = setFields
	}
	if len(incFields) > 0 {
		update["$inc"] = incFields
	}
	if len(mulFields) > 0 {
		update["$mul"] = mulFields
	}
	
	if len(update) == 0 {
		update["$set"] = bson.M{}
	}
	
	return update
}

// ============================================================================
// UPDATE BUILDING - PIPELINE
// ============================================================================

func BuildMongoPipelineUpdate(fields []*pb.QueryField) mongo.Pipeline {
	setStage := bson.M{}
	
	for _, field := range fields {
		fieldName := field.NameExpr.Value
		
		if field.ValueExpr == nil {
			setStage[fieldName] = ""
			continue
		}
		
		switch field.ValueExpr.Type {
		case "BINARY":
			result := BuildMongoBinaryExpression(field.ValueExpr)
			log.Printf("🔍 Field '%s' built as: %+v", fieldName, result)
			setStage[fieldName] = result
		case "FUNCTION":
			setStage[fieldName] = BuildMongoFunctionExpression(field.ValueExpr)
		case "CASEWHEN":
			setStage[fieldName] = BuildMongoCaseWhenExpression(field.ValueExpr)
		default:
			setStage[fieldName] = field.ValueExpr.Value
		}
	}
	
	log.Printf("🔍 Final pipeline $set stage: %+v", setStage)
	
	return mongo.Pipeline{
		{{Key: "$set", Value: setStage}},
	}
}

// BuildMongoExpressionFromAST converts TrueAST Expression to MongoDB expression
func BuildMongoExpressionFromAST(expr *pb.Expression) interface{} {
	if expr == nil {
		return nil
	}
	switch expr.Type {
	case "BINARY":
		return BuildMongoBinaryExpression(expr)
	case "FUNCTION":
		return BuildMongoFunctionExpression(expr)
	case "FIELD":
		return "$" + expr.Value
	case "LITERAL":
		return ParseMongoValue(expr.Value)
	default:
		if expr.Value != "" {
			return "$" + expr.Value
		}
		return nil
	}
}

func BuildMongoBinaryExpression(expr *pb.Expression) interface{} {
	leftValue := buildOperand(expr.Left)
	rightValue := buildOperand(expr.Right)
	
	switch expr.Operator {
	case "+":
		return bson.M{"$add": bson.A{leftValue, rightValue}}
	case "-":
		return bson.M{"$subtract": bson.A{leftValue, rightValue}}
	case "*":
		return bson.M{"$multiply": bson.A{leftValue, rightValue}}
	case "/":
		return bson.M{"$divide": bson.A{leftValue, rightValue}}
	case "%":
		return bson.M{"$mod": bson.A{leftValue, rightValue}}
	default:
		return leftValue
	}
}

func buildOperand(expr *pb.Expression) interface{} {
	if expr == nil {
		return nil
	}
	switch expr.Type {
	case "FIELD":
		return "$" + expr.Value
	case "BINARY":
		return BuildMongoBinaryExpression(expr)
	case "FUNCTION":
		return BuildMongoFunctionExpression(expr)
	case "LITERAL":
		return ParseMongoValue(expr.Value)
	default:
		return ParseMongoValue(expr.Value)
	}
}

func BuildMongoFunctionExpression(expr *pb.Expression) interface{} {
	funcName := strings.ToUpper(expr.FunctionName)
	
	var args []interface{}
	for _, arg := range expr.FunctionArgs {
		if arg.Type == "FIELD" {
			args = append(args, "$"+arg.Value)
		} else {
			args = append(args, ParseMongoValue(arg.Value))
		}
	}
	
	switch funcName {
	case "UPPER":
		if len(args) > 0 {
			return bson.M{"$toUpper": args[0]}
		}
	case "LOWER":
		if len(args) > 0 {
			return bson.M{"$toLower": args[0]}
		}
	case "CONCAT":
		return bson.M{"$concat": args}
	case "LENGTH":
		if len(args) > 0 {
			return bson.M{"$strLenCP": args[0]}
		}
	case "ABS":
		if len(args) > 0 {
			return bson.M{"$abs": args[0]}
		}
	case "ROUND":
		if len(args) > 0 {
			return bson.M{"$round": bson.A{args[0], 0}}
		}
	}
	
	if len(args) > 0 {
		return args[0]
	}
	return ""
}

func BuildMongoCaseWhenExpression(expr *pb.Expression) interface{} {
	branches := bson.A{}
	
	for _, caseCondition := range expr.CaseConditions {
		condExpr := ParseMongoConditionExpression(caseCondition.Condition)
		
		// Check if THEN is an expression or literal
		var thenValue interface{}
		if caseCondition.ThenExpr != nil && (caseCondition.ThenExpr.Type == "BINARY" || caseCondition.ThenExpr.Type == "FUNCTION") {
			thenValue = BuildMongoExpressionFromAST(caseCondition.ThenExpr)
		} else {
			thenValue = ParseMongoValue(caseCondition.ThenExpr.Value)
		}
		
		branch := bson.M{
			"case": condExpr,
			"then": thenValue,
		}
		branches = append(branches, branch)
	}
	
	if len(branches) == 0 {
		return ""
	}
	
	switchExpr := bson.M{"branches": branches}
	
	if expr.CaseElse != nil {
		// Check if ELSE is an expression or literal
		if expr.CaseElse.Type == "BINARY" || expr.CaseElse.Type == "FUNCTION" {
			switchExpr["default"] = BuildMongoExpressionFromAST(expr.CaseElse)
		} else {
			switchExpr["default"] = ParseMongoValue(expr.CaseElse.Value)
		}
	}
	
	return bson.M{"$switch": switchExpr}
}

func ParseMongoConditionExpression(cond *pb.QueryCondition) interface{} {
	if cond == nil {
		return true
	}
	
	leftValue := "$" + cond.FieldExpr.Value
	rightValue := ParseMongoValue(cond.ValueExpr.Value)
	
	switch cond.Operator {
	case ">=", "$gte":
		return bson.M{"$gte": bson.A{leftValue, rightValue}}
	case "<=", "$lte":
		return bson.M{"$lte": bson.A{leftValue, rightValue}}
	case ">", "$gt":
		return bson.M{"$gt": bson.A{leftValue, rightValue}}
	case "<", "$lt":
		return bson.M{"$lt": bson.A{leftValue, rightValue}}
	case "=", "$eq":
		return bson.M{"$eq": bson.A{leftValue, rightValue}}
	case "!=", "$ne":
		return bson.M{"$ne": bson.A{leftValue, rightValue}}
	default:
		return bson.M{"$eq": bson.A{leftValue, rightValue}}
	}
}

func BuildMongoProjectionExpression(expr *pb.Expression) interface{} {
	switch expr.Type {
	case "BINARY":
		return BuildMongoBinaryExpression(expr)
	case "FUNCTION":
		return BuildMongoFunctionExpression(expr)
	case "CASEWHEN":
		return BuildMongoCaseWhenExpression(expr)
	default:
		return nil
	}
}

// ============================================================================
// WHERE EXPRESSION SUPPORT
// ============================================================================

func IsFieldExpression(field string) bool {
	expressionOps := []string{" * ", " / ", " + ", " - ", " % "}
	for _, op := range expressionOps {
		if strings.Contains(field, op) {
			return true
		}
	}
	if strings.Contains(field, "(") && strings.Contains(field, ")") {
		return true
	}
	return false
}

func BuildExprCondition(cond *pb.QueryCondition) interface{} {
	leftExpr := ParseFieldExpression(cond.FieldExpr.Value)
	rightValue := ParseMongoValue(cond.ValueExpr.Value)
	
	var comparisonOp string
	switch cond.Operator {
	case "$gt":
		comparisonOp = "$gt"
	case "$gte":
		comparisonOp = "$gte"
	case "$lt":
		comparisonOp = "$lt"
	case "$lte":
		comparisonOp = "$lte"
	case "$ne":
		comparisonOp = "$ne"
	default:
		comparisonOp = "$eq"
	}
	
	return bson.M{comparisonOp: bson.A{leftExpr, rightValue}}
}

func ParseFieldExpression(fieldStr string) interface{} {
	fieldStr = strings.TrimSpace(fieldStr)
	
	if strings.Contains(fieldStr, "(") && strings.Contains(fieldStr, ")") {
		openIdx := strings.Index(fieldStr, "(")
		if openIdx > 0 && IsLetter(rune(fieldStr[openIdx-1])) {
			return ParseFunctionExpressionInWhere(fieldStr)
		}
	}
	
	for _, op := range []string{" * ", " / ", " % "} {
		if idx := strings.Index(fieldStr, op); idx != -1 {
			left := strings.TrimSpace(fieldStr[:idx])
			right := strings.TrimSpace(fieldStr[idx+len(op):])
			
			var mongoOp string
			switch op {
			case " * ":
				mongoOp = "$multiply"
			case " / ":
				mongoOp = "$divide"
			case " % ":
				mongoOp = "$mod"
			}
			
			return bson.M{mongoOp: bson.A{ParseFieldExpression(left), ParseFieldExpression(right)}}
		}
	}
	
	for _, op := range []string{" + ", " - "} {
		if idx := strings.Index(fieldStr, op); idx != -1 {
			left := strings.TrimSpace(fieldStr[:idx])
			right := strings.TrimSpace(fieldStr[idx+len(op):])
			
			var mongoOp string
			switch op {
			case " + ":
				mongoOp = "$add"
			case " - ":
				mongoOp = "$subtract"
			}
			
			return bson.M{mongoOp: bson.A{ParseFieldExpression(left), ParseFieldExpression(right)}}
		}
	}
	
	val := ParseMongoValue(fieldStr)
	if _, ok := val.(int); ok {
		return val
	}
	if _, ok := val.(float64); ok {
		return val
	}
	
	return "$" + fieldStr
}

func ParseFunctionExpressionInWhere(fieldStr string) interface{} {
	openIdx := strings.Index(fieldStr, "(")
	if openIdx == -1 {
		return "$" + fieldStr
	}
	
	funcName := strings.ToUpper(strings.TrimSpace(fieldStr[:openIdx]))
	closeIdx := strings.LastIndex(fieldStr, ")")
	if closeIdx == -1 {
		return "$" + fieldStr
	}
	
	argsStr := strings.TrimSpace(fieldStr[openIdx+1 : closeIdx])
	
	var args []interface{}
	if argsStr != "" {
		rawArgs := strings.Split(argsStr, ",")
		for _, arg := range rawArgs {
			arg = strings.TrimSpace(arg)
			val := ParseMongoValue(arg)
			if _, ok := val.(int); ok {
				args = append(args, val)
			} else if _, ok := val.(float64); ok {
				args = append(args, val)
			} else {
				args = append(args, "$"+arg)
			}
		}
	}
	
	switch funcName {
	case "UPPER":
		if len(args) > 0 {
			return bson.M{"$toUpper": args[0]}
		}
	case "LOWER":
		if len(args) > 0 {
			return bson.M{"$toLower": args[0]}
		}
	case "LENGTH":
		if len(args) > 0 {
			return bson.M{"$strLenCP": args[0]}
		}
	case "ABS":
		if len(args) > 0 {
			return bson.M{"$abs": args[0]}
		}
	}
	
	if len(args) > 0 {
		return args[0]
	}
	return "$" + fieldStr
}

func IsLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// ============================================================================
// VALUE PARSING
// ============================================================================

func ParseMongoValue(value string) interface{} {
	var floatNum float64
	if _, err := fmt.Sscanf(value, "%f", &floatNum); err == nil {
		if floatNum == float64(int(floatNum)) {
			return int(floatNum)
		}
		return floatNum
	}
	return value
}

// ============================================================================
// DCL OPERATIONS
// ============================================================================

func BuildCreateUserCommand(userName, password string) (bson.D, error) {
	if userName == "" || password == "" {
		return nil, fmt.Errorf("username and password required for CREATE_USER")
	}
	return bson.D{
		{Key: "createUser", Value: userName},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{}},
	}, nil
}

func BuildAlterUserCommand(userName, password string) (bson.D, error) {
	if userName == "" {
		return nil, fmt.Errorf("username required for ALTER_USER")
	}
	return bson.D{
		{Key: "updateUser", Value: userName},
		{Key: "pwd", Value: password},
	}, nil
}

func BuildDropUserCommand(userName string) (bson.D, error) {
	if userName == "" {
		return nil, fmt.Errorf("username required for DROP_USER")
	}
	return bson.D{{Key: "dropUser", Value: userName}}, nil
}

func BuildCreateRoleCommand(roleName string) (bson.D, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name required for CREATE_ROLE")
	}
	return bson.D{
		{Key: "createRole", Value: roleName},
		{Key: "privileges", Value: bson.A{}},
		{Key: "roles", Value: bson.A{}},
	}, nil
}

func BuildDropRoleCommand(roleName string) (bson.D, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name required for DROP_ROLE")
	}
	return bson.D{{Key: "dropRole", Value: roleName}}, nil
}

func BuildGrantRoleCommand(userName, roleName string) (bson.D, error) {
	if userName == "" || roleName == "" {
		return nil, fmt.Errorf("username and role name required for GRANT_ROLE")
	}
	return bson.D{
		{Key: "grantRolesToUser", Value: userName},
		{Key: "roles", Value: bson.A{roleName}},
	}, nil
}

func BuildRevokeRoleCommand(userName, roleName string) (bson.D, error) {
	if userName == "" || roleName == "" {
		return nil, fmt.Errorf("username and role name required for REVOKE_ROLE")
	}
	return bson.D{
		{Key: "revokeRolesFromUser", Value: userName},
		{Key: "roles", Value: bson.A{roleName}},
	}, nil
}

func BuildGrantCommand(userName string, permissions []string, target string, dbName string) (bson.D, error) {
	if userName == "" || len(permissions) == 0 {
		return nil, fmt.Errorf("username and permissions required for GRANT")
	}
	mongoPermissions := MapSQLToMongoPermissions(permissions)
	privileges := bson.A{}
	for _, perm := range mongoPermissions {
		privileges = append(privileges, bson.D{
			{Key: "resource", Value: bson.D{{Key: "db", Value: dbName}, {Key: "collection", Value: target}}},
			{Key: "actions", Value: bson.A{perm}},
		})
	}
	return bson.D{
		{Key: "grantPrivilegesToRole", Value: userName},
		{Key: "privileges", Value: privileges},
	}, nil
}

func BuildRevokeCommand(userName string, permissions []string, target string, dbName string) (bson.D, error) {
	if userName == "" || len(permissions) == 0 {
		return nil, fmt.Errorf("username and permissions required for REVOKE")
	}
	mongoPermissions := MapSQLToMongoPermissions(permissions)
	privileges := bson.A{}
	for _, perm := range mongoPermissions {
		privileges = append(privileges, bson.D{
			{Key: "resource", Value: bson.D{{Key: "db", Value: dbName}, {Key: "collection", Value: target}}},
			{Key: "actions", Value: bson.A{perm}},
		})
	}
	return bson.D{
		{Key: "revokePrivilegesFromRole", Value: userName},
		{Key: "privileges", Value: privileges},
	}, nil
}

func MapSQLToMongoPermissions(sqlPermissions []string) []string {
	permissionMap := map[string]string{
		"SELECT": "find", "INSERT": "insert", "UPDATE": "update", "DELETE": "remove", "ALL": "dbAdmin",
	}
	var mongoPerms []string
	for _, sqlPerm := range sqlPermissions {
		if mongoPerm, exists := permissionMap[strings.ToUpper(sqlPerm)]; exists {
			mongoPerms = append(mongoPerms, mongoPerm)
		} else {
			mongoPerms = append(mongoPerms, sqlPerm)
		}
	}
	return mongoPerms
}

// ============================================================================
// DDL OPERATIONS
// ============================================================================

func BuildRenameCollectionCommand(dbName, oldName, newName string) (bson.D, error) {
	if newName == "" {
		return nil, fmt.Errorf("no new name specified for RENAME COLLECTION")
	}
	return bson.D{
		{Key: "renameCollection", Value: dbName + "." + oldName},
		{Key: "to", Value: dbName + "." + newName},
	}, nil
}

func BuildCreateViewCommand(viewName, collectionName string) (bson.D, error) {
	if viewName == "" {
		return nil, fmt.Errorf("view name required for CREATE VIEW")
	}
	if collectionName == "" {
		return nil, fmt.Errorf("collection name required for CREATE VIEW")
	}
	return bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: collectionName},
		{Key: "pipeline", Value: bson.A{}},
	}, nil
}

func ExtractCollectionNameFromQuery(viewQuery string) (string, error) {
	if viewQuery == "" {
		return "", fmt.Errorf("viewQuery is empty")
	}
	viewQueryUpper := strings.ToUpper(viewQuery)
	if !strings.Contains(viewQueryUpper, "FROM") {
		return "", fmt.Errorf("ViewQuery missing FROM clause: %s", viewQuery)
	}
	parts := strings.Split(viewQueryUpper, "FROM")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid ViewQuery format: %s", viewQuery)
	}
	words := strings.Fields(strings.TrimSpace(parts[1]))
	if len(words) == 0 {
		return "", fmt.Errorf("no collection specified after FROM: %s", viewQuery)
	}
	return strings.ToLower(words[0]), nil
}

// ============================================================================
// DQL OPERATIONS
// ============================================================================

func BuildMongoDBJoinPipeline(query *pb.DocumentQuery) []bson.M {
	pipeline := []bson.M{}

	for _, join := range query.Joins {
		lookupStage := bson.M{
			"$lookup": bson.M{
				"from":         join.Table,
				"localField":   ExtractFieldName(join.LeftExpr.Value),
				"foreignField": ExtractFieldName(join.RightExpr.Value),
				"as":           join.Table + "_joined",
			},
		}
		pipeline = append(pipeline, lookupStage)

		if strings.ToLower(join.JoinType) == "inner" || strings.ToLower(join.JoinType) == "inner_join" {
			pipeline = append(pipeline, bson.M{"$unwind": bson.M{"path": "$" + join.Table + "_joined"}})
		} else {
			pipeline = append(pipeline, bson.M{"$unwind": bson.M{"path": "$" + join.Table + "_joined", "preserveNullAndEmptyArrays": true}})
		}
	}

	if len(query.Conditions) > 0 {
		pipeline = append(pipeline, BuildMongoDBMatchStage(query.Conditions))
	}
	if len(query.OrderBy) > 0 {
		pipeline = append(pipeline, BuildMongoDBSortStage(query.OrderBy))
	}
	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}
	if query.Skip > 0 {
		pipeline = append(pipeline, bson.M{"$skip": query.Skip})
	}

	if len(query.Columns) > 0 {
		projection := bson.M{}
		for _, col := range query.Columns {
			parts := strings.Split(col.Value, ".")
			if len(parts) == 2 {
				projection[parts[1]] = 1
			}
		}
		if len(projection) > 0 {
			pipeline = append(pipeline, bson.M{"$project": projection})
		}
	}

	return pipeline
}

func BuildMongoDBAggregatePipeline(query *pb.DocumentQuery) []bson.M {
	pipeline := []bson.M{}

	if len(query.Conditions) > 0 {
		pipeline = append(pipeline, BuildMongoDBMatchStage(query.Conditions))
	}
	if len(query.GroupBy) == 0 && len(query.OrderBy) > 0 {
		pipeline = append(pipeline, BuildMongoDBSortStage(query.OrderBy))
	}
	if query.Skip > 0 {
		pipeline = append(pipeline, bson.M{"$skip": query.Skip})
	}
	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}

	if query.Aggregate != nil {
		pipeline = append(pipeline, BuildMongoDBGroupStage(query))
		
		aggField := query.Aggregate.FieldExpr.Value
		if query.Distinct && aggField != "" {
			aggFunc := strings.ToLower(query.Aggregate.Function)
			if aggFunc == "count" {
				pipeline = append(pipeline, bson.M{"$project": bson.M{"_id": "$_id", "result": bson.M{"$size": "$result"}}})
			} else if aggFunc == "sum" {
				pipeline = append(pipeline, bson.M{"$project": bson.M{"_id": "$_id", "result": bson.M{"$sum": "$result"}}})
			} else if aggFunc == "avg" {
				pipeline = append(pipeline, bson.M{"$project": bson.M{"_id": "$_id", "result": bson.M{"$avg": "$result"}}})
			}
		}

		if len(query.Having) > 0 {
			pipeline = append(pipeline, BuildMongoDBHavingStage(query.Having))
		}
		if len(query.GroupBy) > 0 && len(query.OrderBy) > 0 {
			pipeline = append(pipeline, BuildMongoDBSortStage(query.OrderBy))
		}
	} else {
		pipeline = append(pipeline, bson.M{"$group": bson.M{"_id": nil, "result": bson.M{"$sum": 1}}})
	}

	return pipeline
}

func BuildMongoDBGroupStage(query *pb.DocumentQuery) bson.M {
	groupID := interface{}(nil)

	if len(query.GroupBy) > 0 {
		if len(query.GroupBy) == 1 {
			groupID = "$" + query.GroupBy[0].Value
		} else {
			groupFields := bson.M{}
			for _, field := range query.GroupBy {
				groupFields[field.Value] = "$" + field.Value
			}
			groupID = groupFields
		}
	}

	var aggExpr bson.M
	aggFunc := strings.ToLower(query.Aggregate.Function)
	aggField := getAggField(query.Aggregate)  // ✅ Uses nil-safe helper

	if query.Distinct && aggField != "" {
		switch aggFunc {
		case "count", "sum", "avg":
			aggExpr = bson.M{"$addToSet": "$" + aggField}
		default:
			aggExpr = bson.M{"$" + aggFunc: "$" + aggField}
		}
	} else {
		switch aggFunc {
		case "count":
			aggExpr = bson.M{"$sum": 1}
		case "sum":
			aggExpr = bson.M{"$sum": "$" + aggField}
		case "avg":
			aggExpr = bson.M{"$avg": "$" + aggField}
		case "min":
			aggExpr = bson.M{"$min": "$" + aggField}
		case "max":
			aggExpr = bson.M{"$max": "$" + aggField}
		default:
			aggExpr = bson.M{"$sum": 1}
		}
	}

	return bson.M{"$group": bson.M{"_id": groupID, "result": aggExpr}}
}

func BuildMongoDBHavingStage(having []*pb.QueryCondition) bson.M {
	matchConditions := bson.M{}
	for _, cond := range having {
		field := "result"
		valueStr := cond.ValueExpr.Value
		var value interface{} = valueStr
		if intVal, err := strconv.Atoi(valueStr); err == nil {
			value = intVal
		}

		switch cond.Operator {
		case "$gt", ">":
			matchConditions[field] = bson.M{"$gt": value}
		case "$gte", ">=":
			matchConditions[field] = bson.M{"$gte": value}
		case "$lt", "<":
			matchConditions[field] = bson.M{"$lt": value}
		case "$lte", "<=":
			matchConditions[field] = bson.M{"$lte": value}
		case "$ne", "!=":
			matchConditions[field] = bson.M{"$ne": value}
		default:
			matchConditions[field] = value
		}
	}
	return bson.M{"$match": matchConditions}
}

func BuildWindowFunctionPipeline(query *pb.DocumentQuery) ([]bson.M, error) {
	pipeline := []bson.M{}
	if len(query.Conditions) > 0 {
		pipeline = append(pipeline, BuildMongoDBMatchStage(query.Conditions))
	}
	for _, wf := range query.WindowFunctions {
		windowStage, err := BuildWindowStage(wf)
		if err != nil {
			return nil, err
		}
		pipeline = append(pipeline, windowStage)
	}
	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}
	return pipeline, nil
}

func BuildWindowStage(wf *pb.WindowClause) (bson.M, error) {
	windowSpec := bson.M{}

	if len(wf.PartitionBy) > 0 {
		if len(wf.PartitionBy) == 1 {
			windowSpec["partitionBy"] = "$" + wf.PartitionBy[0].Value
		} else {
			partitionFields := bson.A{}
			for _, field := range wf.PartitionBy {
				partitionFields = append(partitionFields, "$"+field.Value)
			}
			windowSpec["partitionBy"] = partitionFields
		}
	}

	if len(wf.OrderBy) > 0 {
		sortFields := bson.M{}
		for _, ob := range wf.OrderBy {
			direction, _ := strconv.Atoi(ob.Direction)
			if direction == 0 {
				direction = 1
			}
			sortFields[ob.FieldExpr.Value] = direction
		}
		windowSpec["sortBy"] = sortFields
	}

	var windowExpr bson.M
	function := strings.ToLower(wf.Function)

	switch function {
	case "$documentnumber", "row_number":
		windowExpr = bson.M{wf.Alias: bson.M{"$documentNumber": bson.M{}}}
	case "$rank", "rank":
		windowExpr = bson.M{wf.Alias: bson.M{"$rank": bson.M{}}}
	case "$denserank", "dense_rank":
		windowExpr = bson.M{wf.Alias: bson.M{"$denseRank": bson.M{}}}
	case "$shift", "shift":
		offset := int(wf.Offset)
		if offset == 0 {
			offset = -1
		}
		windowExpr = bson.M{wf.Alias: bson.M{"$shift": bson.M{"output": "$" + wf.Alias, "by": offset}}}
	case "ntile":
		windowExpr = bson.M{wf.Alias: bson.M{"$rank": bson.M{}}}
	default:
		return nil, fmt.Errorf("unsupported window function: %s", function)
	}

	windowSpec["output"] = windowExpr
	return bson.M{"$setWindowFields": windowSpec}, nil
}

func BuildSetOperationPipeline(query *pb.DocumentQuery) ([]bson.M, error) {
	pipeline := []bson.M{}
	if len(query.Conditions) > 0 {
		pipeline = append(pipeline, BuildMongoDBMatchStage(query.Conditions))
	}

	operation := strings.ToLower(query.Operation)
	switch operation {
	case "unionwith":
		pipeline = append(pipeline, bson.M{"$unionWith": bson.M{"coll": query.Collection}})
	case "intersect", "setdifference":
		// Already handled by combining conditions in translator
	default:
		return nil, fmt.Errorf("unsupported set operation: %s", operation)
	}

	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}
	return pipeline, nil
}

func BuildMongoDBMatchStage(conditions []*pb.QueryCondition) bson.M {
	return bson.M{"$match": BuildMongoFilter(conditions)}
}

func BuildMongoDBSortStage(orderBy []*pb.OrderByClause) bson.M {
	sortFields := bson.M{}
	for _, ob := range orderBy {
		field := ExtractFieldName(ob.FieldExpr.Value)
		direction := 1
		if ob.Direction == "-1" || strings.ToUpper(ob.Direction) == "DESC" {
			direction = -1
		}
		sortFields[field] = direction
	}
	return bson.M{"$sort": sortFields}
}

func ExtractFieldName(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		return parts[1]
	}
	return field
}

// ============================================================================
// TCL OPERATIONS
// ============================================================================

func BuildTransactionOptions() *options.TransactionOptions {
	return options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
}

func IsUnsupportedTCLOperation(operation string) bool {
	unsupportedOps := map[string]bool{
		"savepoint": true, "rollback_to": true, "rollback_to_savepoint": true, "release_savepoint": true,
	}
	return unsupportedOps[strings.ToLower(operation)]
}

func GetUnsupportedOperationError(operation string) error {
	switch strings.ToLower(operation) {
	case "savepoint":
		return fmt.Errorf("SAVEPOINT not supported in MongoDB")
	case "rollback_to", "rollback_to_savepoint":
		return fmt.Errorf("ROLLBACK TO SAVEPOINT not supported in MongoDB")
	case "release_savepoint":
		return fmt.Errorf("RELEASE SAVEPOINT not supported in MongoDB")
	default:
		return fmt.Errorf("unsupported TCL operation: %s", operation)
	}
}