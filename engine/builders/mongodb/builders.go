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
// FILTER BUILDING
// ============================================================================

// BuildMongoFilter builds a BSON filter from conditions with expression support
func BuildMongoFilter(conditions []*pb.QueryCondition) bson.M {
	if len(conditions) == 0 {
		return bson.M{}
	}

	filter := bson.M{}
	var exprConditions []interface{}
	
	for _, cond := range conditions {
		// Check if field contains expression operators
		if IsFieldExpression(cond.Field) {
			// Build $expr for expression-based condition
			exprCond := BuildExprCondition(cond)
			exprConditions = append(exprConditions, exprCond)
		} else {
			// Regular field - use standard filter
			value := ParseMongoValue(cond.Value)
			mongoOp := cond.Operator
			
			if mongoOp == "$eq" {
				filter[cond.Field] = value
			} else {
				filter[cond.Field] = bson.M{mongoOp: value}
			}
		}
	}
	
	// If we have expression conditions, add them with $expr
	if len(exprConditions) > 0 {
		if len(exprConditions) == 1 {
			filter["$expr"] = exprConditions[0]
		} else {
			filter["$expr"] = bson.M{"$and": exprConditions}
		}
	}

	return filter
}

// ============================================================================
// DOCUMENT BUILDING
// ============================================================================

// BuildMongoDocument builds a BSON document from fields
func BuildMongoDocument(fields []*pb.QueryField) bson.M {
	document := bson.M{}
	for _, field := range fields {
		document[field.Name] = ParseMongoValue(field.Value)
	}
	return document
}

// ============================================================================
// UPDATE BUILDING - SIMPLE (No expressions or simple field+literal)
// ============================================================================

// BuildMongoSimpleUpdate builds simple $set/$inc/$mul update
func BuildMongoSimpleUpdate(fields []*pb.QueryField) bson.M {
	update := bson.M{}
	setFields := bson.M{}
	incFields := bson.M{}
	mulFields := bson.M{}
	
	for _, field := range fields {
		expr := field.GetExpression()
		
		if expr != nil && expr.ExpressionType == "BINARY" {
			// Only simple: field OP literal (not field OP field)
			if expr.LeftIsField && !expr.RightIsField {
				rightValue := ParseMongoValue(expr.RightOperand)
				
				switch expr.Operator {
				case "+":
					incFields[field.Name] = rightValue
				case "-":
					if numValue, ok := rightValue.(int); ok {
						incFields[field.Name] = -numValue
					}
				case "*":
					mulFields[field.Name] = rightValue
				case "/":
					if numValue, ok := rightValue.(int); ok && numValue != 0 {
						mulFields[field.Name] = 1.0 / float64(numValue)
					}
				default:
					setFields[field.Name] = field.Value
				}
			} else {
				setFields[field.Name] = field.Value
			}
		} else {
			// No expression, regular $set
			setFields[field.Name] = ParseMongoValue(field.Value)
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
// UPDATE BUILDING - PIPELINE (Complex expressions)
// ============================================================================

// BuildMongoPipelineUpdate builds aggregation pipeline for complex expressions
func BuildMongoPipelineUpdate(fields []*pb.QueryField) mongo.Pipeline {
	setStage := bson.M{}
	
	for _, field := range fields {
		expr := field.GetExpression()
		
		if expr == nil {
			// Simple literal value
			setStage[field.Name] = ParseMongoValue(field.Value)
			continue
		}
		
		// Build expression based on type
		switch expr.ExpressionType {
		case "BINARY":
			result := BuildMongoBinaryExpression(expr)
			log.Printf("🔍 Field '%s' built as: %+v", field.Name, result)
			setStage[field.Name] = result
			
		case "FUNCTION":
			setStage[field.Name] = BuildMongoFunctionExpression(expr)
			
		case "CASEWHEN":
			setStage[field.Name] = BuildMongoCaseWhenExpression(expr)
			
		default:
			// Unknown expression type, use literal
			setStage[field.Name] = field.Value
		}
	}
	
	log.Printf("🔍 Final pipeline $set stage: %+v", setStage)
	
	// Return pipeline: [{ $set: {...} }]
	return mongo.Pipeline{
		{{Key: "$set", Value: setStage}},
	}
}

// BuildMongoBinaryExpression builds MongoDB expression for binary operations
func BuildMongoBinaryExpression(expr *pb.FieldExpression) interface{} {
	leftValue := BuildMongoOperand(expr.LeftOperand, expr.LeftIsField)
	rightValue := BuildMongoOperand(expr.RightOperand, expr.RightIsField)
	
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

// IsComplexExpression checks if an operand string is a complex expression
func IsComplexExpression(operand string) bool {
	operand = strings.TrimSpace(operand)
	
	// Check for parentheses (nested expressions)
	if strings.Contains(operand, "(") || strings.Contains(operand, ")") {
		return true
	}
	
	// Check for binary operators (with spaces)
	operators := []string{" + ", " - ", " * ", " / ", " % "}
	for _, op := range operators {
		if strings.Contains(operand, op) {
			return true
		}
	}
	
	return false
}

// BuildMongoOperand builds a MongoDB operand (field reference or literal)
func BuildMongoOperand(operand string, isField bool) interface{} {
	// ALWAYS check for complex expressions FIRST, regardless of isField flag
	if IsComplexExpression(operand) {
		return ParseFieldExpression(operand)
	}
	
	if isField {
		// Simple field reference: prefix with $
		return "$" + operand
	}
	
	// Literal value
	return ParseMongoValue(operand)
}

// BuildMongoFunctionExpression builds MongoDB expression for functions
func BuildMongoFunctionExpression(expr *pb.FieldExpression) interface{} {
	funcName := strings.ToUpper(expr.FunctionName)
	
	// Build arguments
	var args []interface{}
	for _, arg := range expr.FunctionArgs {
		// Check if arg is a field (starts with letter, no quotes)
		if len(arg) > 0 && !strings.HasPrefix(arg, "'") && !strings.HasPrefix(arg, "\"") {
			// Check if it's a number
			val := ParseMongoValue(arg)
			if _, ok := val.(int); ok {
				args = append(args, val)
			} else if _, ok := val.(float64); ok {
				args = append(args, val)
			} else {
				// It's a field
				args = append(args, "$"+arg)
			}
		} else {
			// Remove quotes if present
			cleanArg := strings.Trim(arg, "'\"")
			args = append(args, cleanArg)
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
	
	// Unknown function, return first arg or empty string
	if len(args) > 0 {
		return args[0]
	}
	return ""
}

// BuildMongoCaseWhenExpression builds MongoDB $switch expression for CASE WHEN
func BuildMongoCaseWhenExpression(expr *pb.FieldExpression) interface{} {
	branches := bson.A{}
	
	for _, caseCondition := range expr.CaseConditions {
		condExpr := ParseMongoConditionExpression(caseCondition.Condition)
		
		branch := bson.M{
			"case": condExpr,
			"then": caseCondition.ThenValue,
		}
		branches = append(branches, branch)
	}
	
	if len(branches) == 0 {
		return ""
	}
	
	switchExpr := bson.M{
		"branches": branches,
	}
	
	if expr.CaseElse != "" {
		switchExpr["default"] = expr.CaseElse
	}
	
	return bson.M{"$switch": switchExpr}
}

// ParseMongoConditionExpression parses a condition string into MongoDB expression
func ParseMongoConditionExpression(condition string) interface{} {
	condition = strings.TrimSpace(condition)
	
	operators := []string{">=", "<=", "!=", "=", ">", "<"}
	
	for _, op := range operators {
		if idx := strings.Index(condition, op); idx != -1 {
			left := strings.TrimSpace(condition[:idx])
			right := strings.TrimSpace(condition[idx+len(op):])
			
			leftValue := "$" + left
			rightValue := ParseMongoValue(right)
			
			switch op {
			case ">=":
				return bson.M{"$gte": bson.A{leftValue, rightValue}}
			case "<=":
				return bson.M{"$lte": bson.A{leftValue, rightValue}}
			case ">":
				return bson.M{"$gt": bson.A{leftValue, rightValue}}
			case "<":
				return bson.M{"$lt": bson.A{leftValue, rightValue}}
			case "=":
				return bson.M{"$eq": bson.A{leftValue, rightValue}}
			case "!=":
				return bson.M{"$ne": bson.A{leftValue, rightValue}}
			}
		}
	}
	
	return true
}

// BuildMongoProjectionExpression builds MongoDB expression for $project stage
func BuildMongoProjectionExpression(expr *pb.FieldExpression) interface{} {
	switch expr.ExpressionType {
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

// IsFieldExpression checks if a field string contains expression operators
func IsFieldExpression(field string) bool {
	// Check for binary operators with spaces
	expressionOps := []string{" * ", " / ", " + ", " - ", " % "}
	for _, op := range expressionOps {
		if strings.Contains(field, op) {
			return true
		}
	}
	
	// Check for functions
	if strings.Contains(field, "(") && strings.Contains(field, ")") {
		return true
	}
	
	return false
}

// BuildExprCondition builds MongoDB $expr for expression-based WHERE conditions
func BuildExprCondition(cond *pb.QueryCondition) interface{} {
	// Parse the field expression
	leftExpr := ParseFieldExpression(cond.Field)
	
	// Parse the value
	rightValue := ParseMongoValue(cond.Value)
	
	// Build comparison operator
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
	case "$eq":
		comparisonOp = "$eq"
	case "$ne":
		comparisonOp = "$ne"
	default:
		comparisonOp = "$eq"
	}
	
	// Return: { $comparisonOp: [leftExpr, rightValue] }
	return bson.M{
		comparisonOp: bson.A{leftExpr, rightValue},
	}
}

// NormalizeOperatorSpacing ensures operators have spaces around them
func NormalizeOperatorSpacing(s string) string {
	// Add spaces around operators (but preserve those in parentheses)
	for _, op := range []string{"*", "/", "+", "-", "%"} {
		// Don't add space if already has space
		withSpace := " " + op + " "
		noSpace := op
		
		// Replace operator without spaces with operator with spaces
		s = strings.ReplaceAll(s, noSpace, withSpace)
	}
	
	// Clean up multiple spaces
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	
	return strings.TrimSpace(s)
}

// ParseFieldExpression parses a field expression string into MongoDB format
func ParseFieldExpression(fieldStr string) interface{} {
	fieldStr = strings.TrimSpace(fieldStr)
	
	// Normalize spacing around operators
	fieldStr = NormalizeOperatorSpacing(fieldStr)
	
	// Check for functions first
	if strings.Contains(fieldStr, "(") && strings.Contains(fieldStr, ")") {
		openIdx := strings.Index(fieldStr, "(")
		if openIdx > 0 && IsLetter(rune(fieldStr[openIdx-1])) {
			return ParseFunctionExpressionInWhere(fieldStr)
		}
	}
	
	// Check for parenthesized expressions
	if strings.HasPrefix(fieldStr, "(") {
		return ParseParenthesizedFieldExpression(fieldStr)
	}
	
	// Check multiplication/division first (higher precedence)
	for _, op := range []string{" * ", " / ", " % "} {
		if idx := strings.Index(fieldStr, op); idx != -1 {
			left := strings.TrimSpace(fieldStr[:idx])
			right := strings.TrimSpace(fieldStr[idx+len(op):])
			
			leftExpr := ParseFieldExpression(left)
			rightExpr := ParseFieldExpression(right)
			
			var mongoOp string
			switch op {
			case " * ":
				mongoOp = "$multiply"
			case " / ":
				mongoOp = "$divide"
			case " % ":
				mongoOp = "$mod"
			}
			
			return bson.M{mongoOp: bson.A{leftExpr, rightExpr}}
		}
	}
	
	// Then check addition/subtraction
	for _, op := range []string{" + ", " - "} {
		if idx := strings.Index(fieldStr, op); idx != -1 {
			left := strings.TrimSpace(fieldStr[:idx])
			right := strings.TrimSpace(fieldStr[idx+len(op):])
			
			leftExpr := ParseFieldExpression(left)
			rightExpr := ParseFieldExpression(right)
			
			var mongoOp string
			switch op {
			case " + ":
				mongoOp = "$add"
			case " - ":
				mongoOp = "$subtract"
			}
			
			return bson.M{mongoOp: bson.A{leftExpr, rightExpr}}
		}
	}
	
	// Check if it's a number
	val := ParseMongoValue(fieldStr)
	if _, ok := val.(int); ok {
		return val
	}
	if _, ok := val.(float64); ok {
		return val
	}
	
	// It's a field name - prefix with $
	return "$" + fieldStr
}

// ParseParenthesizedFieldExpression handles (price - cost) * quantity
func ParseParenthesizedFieldExpression(fieldStr string) interface{} {
	depth := 0
	closingIdx := -1
	for i, ch := range fieldStr {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				closingIdx = i
				break
			}
		}
	}
	
	if closingIdx == -1 {
		return "$" + fieldStr
	}
	
	innerExpr := strings.TrimSpace(fieldStr[1:closingIdx])
	remaining := strings.TrimSpace(fieldStr[closingIdx+1:])
	
	if remaining == "" {
		return ParseFieldExpression(innerExpr)
	}
	
	tokens := strings.Fields(remaining)
	if len(tokens) < 2 {
		return ParseFieldExpression(innerExpr)
	}
	
	operator := tokens[0]
	rightOperand := strings.Join(tokens[1:], " ")
	
	leftExpr := ParseFieldExpression(innerExpr)
	rightExpr := ParseFieldExpression(rightOperand)
	
	var mongoOp string
	switch operator {
	case "*":
		mongoOp = "$multiply"
	case "/":
		mongoOp = "$divide"
	case "+":
		mongoOp = "$add"
	case "-":
		mongoOp = "$subtract"
	case "%":
		mongoOp = "$mod"
	default:
		return leftExpr
	}
	
	return bson.M{mongoOp: bson.A{leftExpr, rightExpr}}
}

// ParseFunctionExpressionInWhere parses function calls in WHERE clause
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

// IsLetter checks if a rune is a letter
func IsLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// ============================================================================
// VALUE PARSING
// ============================================================================

// ParseMongoValue attempts to parse value as number, otherwise returns string
func ParseMongoValue(value string) interface{} {
	// Try float first to preserve decimals
	var floatNum float64
	if _, err := fmt.Sscanf(value, "%f", &floatNum); err == nil {
		// If it has no decimal part, return as int
		if floatNum == float64(int(floatNum)) {
			return int(floatNum)
		}
		return floatNum
	}
	
	return value
}

// ============================================================================
// DCL OPERATIONS - BSON COMMAND BUILDERS
// ============================================================================

// BuildCreateUserCommand builds BSON command for createUser
func BuildCreateUserCommand(userName, password string) (bson.D, error) {
	if userName == "" || password == "" {
		return nil, fmt.Errorf("username and password required for CREATE_USER")
	}

	command := bson.D{
		{Key: "createUser", Value: userName},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{}},
	}

	return command, nil
}

// BuildAlterUserCommand builds BSON command for updateUser
func BuildAlterUserCommand(userName, password string) (bson.D, error) {
	if userName == "" {
		return nil, fmt.Errorf("username required for ALTER_USER")
	}

	command := bson.D{
		{Key: "updateUser", Value: userName},
		{Key: "pwd", Value: password},
	}

	return command, nil
}

// BuildDropUserCommand builds BSON command for dropUser
func BuildDropUserCommand(userName string) (bson.D, error) {
	if userName == "" {
		return nil, fmt.Errorf("username required for DROP_USER")
	}

	command := bson.D{
		{Key: "dropUser", Value: userName},
	}

	return command, nil
}

// BuildCreateRoleCommand builds BSON command for createRole
func BuildCreateRoleCommand(roleName string) (bson.D, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name required for CREATE_ROLE")
	}

	command := bson.D{
		{Key: "createRole", Value: roleName},
		{Key: "privileges", Value: bson.A{}},
		{Key: "roles", Value: bson.A{}},
	}

	return command, nil
}

// BuildDropRoleCommand builds BSON command for dropRole
func BuildDropRoleCommand(roleName string) (bson.D, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name required for DROP_ROLE")
	}

	command := bson.D{
		{Key: "dropRole", Value: roleName},
	}

	return command, nil
}

// BuildGrantRoleCommand builds BSON command for grantRolesToUser
func BuildGrantRoleCommand(userName, roleName string) (bson.D, error) {
	if userName == "" || roleName == "" {
		return nil, fmt.Errorf("username and role name required for GRANT_ROLE")
	}

	command := bson.D{
		{Key: "grantRolesToUser", Value: userName},
		{Key: "roles", Value: bson.A{roleName}},
	}

	return command, nil
}

// BuildRevokeRoleCommand builds BSON command for revokeRolesFromUser
func BuildRevokeRoleCommand(userName, roleName string) (bson.D, error) {
	if userName == "" || roleName == "" {
		return nil, fmt.Errorf("username and role name required for REVOKE_ROLE")
	}

	command := bson.D{
		{Key: "revokeRolesFromUser", Value: userName},
		{Key: "roles", Value: bson.A{roleName}},
	}

	return command, nil
}

// BuildGrantCommand builds BSON command for grantPrivilegesToRole
func BuildGrantCommand(userName string, permissions []string, target string, dbName string) (bson.D, error) {
	if userName == "" || len(permissions) == 0 {
		return nil, fmt.Errorf("username and permissions required for GRANT")
	}

	// Map SQL permissions to MongoDB actions
	mongoPermissions := MapSQLToMongoPermissions(permissions)

	// Build privileges array
	privileges := bson.A{}
	for _, perm := range mongoPermissions {
		privilege := bson.D{
			{Key: "resource", Value: bson.D{
				{Key: "db", Value: dbName},
				{Key: "collection", Value: target},
			}},
			{Key: "actions", Value: bson.A{perm}},
		}
		privileges = append(privileges, privilege)
	}

	command := bson.D{
		{Key: "grantPrivilegesToRole", Value: userName},
		{Key: "privileges", Value: privileges},
	}

	return command, nil
}

// BuildRevokeCommand builds BSON command for revokePrivilegesFromRole
func BuildRevokeCommand(userName string, permissions []string, target string, dbName string) (bson.D, error) {
	if userName == "" || len(permissions) == 0 {
		return nil, fmt.Errorf("username and permissions required for REVOKE")
	}

	// Map SQL permissions to MongoDB actions
	mongoPermissions := MapSQLToMongoPermissions(permissions)

	// Build privileges array
	privileges := bson.A{}
	for _, perm := range mongoPermissions {
		privilege := bson.D{
			{Key: "resource", Value: bson.D{
				{Key: "db", Value: dbName},
				{Key: "collection", Value: target},
			}},
			{Key: "actions", Value: bson.A{perm}},
		}
		privileges = append(privileges, privilege)
	}

	command := bson.D{
		{Key: "revokePrivilegesFromRole", Value: userName},
		{Key: "privileges", Value: privileges},
	}

	return command, nil
}

// MapSQLToMongoPermissions converts SQL permissions to MongoDB actions
func MapSQLToMongoPermissions(sqlPermissions []string) []string {
	permissionMap := map[string]string{
		"SELECT": "find",
		"INSERT": "insert",
		"UPDATE": "update",
		"DELETE": "remove",
		"ALL":    "dbAdmin", // MongoDB role for full access
	}
	
	var mongoPerms []string
	for _, sqlPerm := range sqlPermissions {
		if mongoPerm, exists := permissionMap[strings.ToUpper(sqlPerm)]; exists {
			mongoPerms = append(mongoPerms, mongoPerm)
		} else {
			// If no mapping found, use original (already in MongoDB format)
			mongoPerms = append(mongoPerms, sqlPerm)
		}
	}
	
	return mongoPerms
}

// ============================================================================
// DDL OPERATIONS - BSON COMMAND BUILDERS
// ============================================================================

// BuildRenameCollectionCommand builds BSON command for renameCollection
func BuildRenameCollectionCommand(dbName, oldName, newName string) (bson.D, error) {
	if newName == "" {
		return nil, fmt.Errorf("no new name specified for RENAME COLLECTION")
	}

	command := bson.D{
		{Key: "renameCollection", Value: dbName + "." + oldName},
		{Key: "to", Value: dbName + "." + newName},
	}

	return command, nil
}

// BuildCreateViewCommand builds BSON command for create view
func BuildCreateViewCommand(viewName, collectionName string) (bson.D, error) {
	if viewName == "" {
		return nil, fmt.Errorf("view name required for CREATE VIEW")
	}

	if collectionName == "" {
		return nil, fmt.Errorf("collection name required for CREATE VIEW")
	}

	// MongoDB views are based on aggregation pipelines
	command := bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: collectionName},
		{Key: "pipeline", Value: bson.A{}}, // Empty pipeline for now
	}

	return command, nil
}

// ExtractCollectionNameFromQuery extracts collection name from ViewQuery
func ExtractCollectionNameFromQuery(viewQuery string) (string, error) {
	if viewQuery == "" {
		return "", fmt.Errorf("viewQuery is empty")
	}

	// Parse "SELECT * FROM users WHERE..." to extract "users"
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
	
	collectionName := strings.ToLower(words[0])
	
	return collectionName, nil
}

// ============================================================================
// DQL OPERATIONS - AGGREGATION PIPELINE BUILDERS
// ============================================================================

// BuildMongoDBJoinPipeline constructs MongoDB aggregation pipeline for JOINs
func BuildMongoDBJoinPipeline(query *pb.DocumentQuery) []bson.M {
	pipeline := []bson.M{}

	for _, join := range query.Joins {
		lookupStage := bson.M{
			"$lookup": bson.M{
				"from":         join.Table,
				"localField":   ExtractFieldName(join.LeftField),
				"foreignField": ExtractFieldName(join.RightField),
				"as":           join.Table + "_joined",
			},
		}
		pipeline = append(pipeline, lookupStage)

		if strings.ToLower(join.JoinType) == "inner" || strings.ToLower(join.JoinType) == "inner_join" {
			unwindStage := bson.M{
				"$unwind": bson.M{
					"path": "$" + join.Table + "_joined",
				},
			}
			pipeline = append(pipeline, unwindStage)
		} else {
			unwindStage := bson.M{
				"$unwind": bson.M{
					"path":                       "$" + join.Table + "_joined",
					"preserveNullAndEmptyArrays": true,
				},
			}
			pipeline = append(pipeline, unwindStage)
		}
	}

	if len(query.Conditions) > 0 {
		matchStage := BuildMongoDBMatchStage(query.Conditions)
		pipeline = append(pipeline, matchStage)
	}

	if len(query.OrderBy) > 0 {
		sortStage := BuildMongoDBSortStage(query.OrderBy)
		pipeline = append(pipeline, sortStage)
	}

	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}

	if query.Skip > 0 {
		pipeline = append(pipeline, bson.M{"$skip": query.Skip})
	}

	// Add $project stage for column selection
	if len(query.Columns) > 0 {
		projection := bson.M{}
		for _, col := range query.Columns {
			// Convert "User.name" → "name: 1" or "Project.title" → "projects_joined.title: 1"
			parts := strings.Split(col, ".")
			if len(parts) == 2 {
				// For joined collections, use the joined array name
				// For main collection, use field directly
				projection[parts[1]] = 1
			}
		}

		if len(projection) > 0 {
			pipeline = append(pipeline, bson.M{"$project": projection})
		}
	}

	return pipeline
}

// BuildMongoDBAggregatePipeline constructs MongoDB aggregation pipeline for aggregates
func BuildMongoDBAggregatePipeline(query *pb.DocumentQuery) []bson.M {
	pipeline := []bson.M{}

	// STEP 1: $match - Apply WHERE conditions FIRST
	if len(query.Conditions) > 0 {
		matchStage := BuildMongoDBMatchStage(query.Conditions)
		pipeline = append(pipeline, matchStage)
	}

	// STEP 2: For non-grouped queries with ORDER BY, apply it before LIMIT
	if len(query.GroupBy) == 0 && len(query.OrderBy) > 0 {
		sortStage := BuildMongoDBSortStage(query.OrderBy)
		pipeline = append(pipeline, sortStage)
	}

	// STEP 3: Apply SKIP before aggregation (to limit source data)
	if query.Skip > 0 {
		pipeline = append(pipeline, bson.M{"$skip": query.Skip})
	}

	// STEP 4: Apply LIMIT before aggregation (to limit source data)
	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}

	// STEP 5: Perform aggregation
	if query.Aggregate != nil {
		groupStage := BuildMongoDBGroupStage(query)
		pipeline = append(pipeline, groupStage)

		// STEP 5b: For DISTINCT aggregations, add post-processing
		if query.Distinct && query.Aggregate.Field != "" {
			aggFunc := strings.ToLower(query.Aggregate.Function)

			if aggFunc == "count" {
				// COUNT DISTINCT: Count the size of the unique values array
				pipeline = append(pipeline, bson.M{
					"$project": bson.M{
						"_id":    "$_id",
						"result": bson.M{"$size": "$result"},
					},
				})
			} else if aggFunc == "sum" {
				// SUM DISTINCT: Sum the unique values array
				pipeline = append(pipeline, bson.M{
					"$project": bson.M{
						"_id":    "$_id",
						"result": bson.M{"$sum": "$result"},
					},
				})
			} else if aggFunc == "avg" {
				// AVG DISTINCT: Average the unique values array
				pipeline = append(pipeline, bson.M{
					"$project": bson.M{
						"_id":    "$_id",
						"result": bson.M{"$avg": "$result"},
					},
				})
			}
		}

		// STEP 6: Apply HAVING after grouping
		if len(query.Having) > 0 {
			havingStage := BuildMongoDBHavingStage(query.Having)
			pipeline = append(pipeline, havingStage)
		}

		// STEP 7: Apply ORDER BY after grouping (for GROUP BY queries)
		if len(query.GroupBy) > 0 && len(query.OrderBy) > 0 {
			sortStage := BuildMongoDBSortStage(query.OrderBy)
			pipeline = append(pipeline, sortStage)
		}
	} else {
		// Simple COUNT with no GROUP BY
		groupStage := bson.M{
			"$group": bson.M{
				"_id":    nil,
				"result": bson.M{"$sum": 1},
			},
		}
		pipeline = append(pipeline, groupStage)
	}

	return pipeline
}

// BuildMongoDBGroupStage constructs $group stage for aggregations
func BuildMongoDBGroupStage(query *pb.DocumentQuery) bson.M {
	groupID := interface{}(nil)

	if len(query.GroupBy) > 0 {
		if len(query.GroupBy) == 1 {
			groupID = "$" + query.GroupBy[0]
		} else {
			groupFields := bson.M{}
			for _, field := range query.GroupBy {
				groupFields[field] = "$" + field
			}
			groupID = groupFields
		}
	}

	var aggExpr bson.M
	aggFunc := strings.ToLower(query.Aggregate.Function)

	// Handle DISTINCT aggregations
	if query.Distinct && query.Aggregate.Field != "" {
		// For DISTINCT, we need to collect unique values first
		switch aggFunc {
		case "count":
			// COUNT DISTINCT: Use $addToSet to collect unique values
			aggExpr = bson.M{"$addToSet": "$" + query.Aggregate.Field}
		case "sum", "avg":
			// SUM/AVG DISTINCT: Use $addToSet to collect unique values
			aggExpr = bson.M{"$addToSet": "$" + query.Aggregate.Field}
		default:
			// MIN/MAX don't need DISTINCT (already return single value)
			aggExpr = bson.M{"$" + aggFunc: "$" + query.Aggregate.Field}
		}
	} else {
		// Regular aggregations (no DISTINCT)
		switch aggFunc {
		case "count":
			aggExpr = bson.M{"$sum": 1}
		case "sum":
			aggExpr = bson.M{"$sum": "$" + query.Aggregate.Field}
		case "avg":
			aggExpr = bson.M{"$avg": "$" + query.Aggregate.Field}
		case "min":
			aggExpr = bson.M{"$min": "$" + query.Aggregate.Field}
		case "max":
			aggExpr = bson.M{"$max": "$" + query.Aggregate.Field}
		default:
			aggExpr = bson.M{"$sum": 1}
		}
	}

	groupStage := bson.M{
		"$group": bson.M{
			"_id":    groupID,
			"result": aggExpr,
		},
	}

	return groupStage
}

// BuildMongoDBHavingStage constructs $match stage for HAVING clause
func BuildMongoDBHavingStage(having []*pb.QueryCondition) bson.M {
	matchConditions := bson.M{}

	for _, cond := range having {
		// HAVING uses "result" field from aggregation
		field := "result"
		operator := cond.Operator

		var value interface{} = cond.Value
		if intVal, err := strconv.Atoi(cond.Value); err == nil {
			value = intVal
		}

		switch operator {
		case "$gt", ">":
			matchConditions[field] = bson.M{"$gt": value}
		case "$gte", ">=":
			matchConditions[field] = bson.M{"$gte": value}
		case "$lt", "<":
			matchConditions[field] = bson.M{"$lt": value}
		case "$lte", "<=":
			matchConditions[field] = bson.M{"$lte": value}
		case "$eq", "=":
			matchConditions[field] = value
		case "$ne", "!=":
			matchConditions[field] = bson.M{"$ne": value}
		default:
			matchConditions[field] = value
		}
	}

	return bson.M{"$match": matchConditions}
}

// BuildWindowFunctionPipeline constructs $setWindowFields pipeline
func BuildWindowFunctionPipeline(query *pb.DocumentQuery) ([]bson.M, error) {
	pipeline := []bson.M{}

	if len(query.Conditions) > 0 {
		matchStage := BuildMongoDBMatchStage(query.Conditions)
		pipeline = append(pipeline, matchStage)
	}

	if len(query.WindowFunctions) > 0 {
		for _, wf := range query.WindowFunctions {
			windowStage, err := BuildWindowStage(wf)
			if err != nil {
				return nil, err
			}
			pipeline = append(pipeline, windowStage)
		}
	}

	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}

	return pipeline, nil
}

// BuildWindowStage constructs $setWindowFields stage for a window function
func BuildWindowStage(wf *pb.WindowClause) (bson.M, error) {
	windowSpec := bson.M{}

	// Add partitionBy if specified
	if len(wf.PartitionBy) > 0 {
		if len(wf.PartitionBy) == 1 {
			windowSpec["partitionBy"] = "$" + wf.PartitionBy[0]
		} else {
			partitionFields := bson.A{}
			for _, field := range wf.PartitionBy {
				partitionFields = append(partitionFields, "$"+field)
			}
			windowSpec["partitionBy"] = partitionFields
		}
	}

	// Add sortBy if specified
	if len(wf.OrderBy) > 0 {
		sortFields := bson.M{}
		for _, ob := range wf.OrderBy {
			direction, _ := strconv.Atoi(ob.Direction)
			if direction == 0 {
				direction = 1
			}
			sortFields[ob.Field] = direction
		}
		windowSpec["sortBy"] = sortFields
	}

	// Build window function expression
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
		windowExpr = bson.M{wf.Alias: bson.M{"$shift": bson.M{
			"output": "$" + wf.Alias,
			"by":     offset,
		}}}
	case "ntile":
		buckets := int(wf.Buckets)
		if buckets == 0 {
			buckets = 4
		}
		windowExpr = bson.M{wf.Alias: bson.M{"$rank": bson.M{}}}
	default:
		return nil, fmt.Errorf("unsupported window function: %s", function)
	}

	windowSpec["output"] = windowExpr

	return bson.M{"$setWindowFields": windowSpec}, nil
}

// BuildSetOperationPipeline constructs pipeline for set operations
func BuildSetOperationPipeline(query *pb.DocumentQuery) ([]bson.M, error) {
	pipeline := []bson.M{}

	// For INTERSECT and EXCEPT, conditions are already combined by translator
	// Just add the match stage
	if len(query.Conditions) > 0 {
		matchStage := BuildMongoDBMatchStage(query.Conditions)
		pipeline = append(pipeline, matchStage)
	}

	operation := strings.ToLower(query.Operation)

	switch operation {
	case "unionwith":
		// UNION uses $unionWith
		unionStage := bson.M{
			"$unionWith": bson.M{
				"coll": query.Collection,
			},
		}
		pipeline = append(pipeline, unionStage)

	case "intersect":
		// Already handled by combining conditions in translator
		// Documents must match ALL conditions

	case "setdifference":
		// Already handled by negating second query conditions in translator
		// Documents match first conditions but NOT second conditions

	default:
		return nil, fmt.Errorf("unsupported set operation: %s", operation)
	}

	if query.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": query.Limit})
	}

	return pipeline, nil
}

// BuildMongoDBMatchStage constructs $match stage for WHERE conditions
func BuildMongoDBMatchStage(conditions []*pb.QueryCondition) bson.M {
	matchConditions := bson.M{}

	for _, cond := range conditions {
		field := ExtractFieldName(cond.Field)
		operator := cond.Operator

		// Parse value as int if possible
		var value interface{} = cond.Value
		if intVal, err := strconv.Atoi(cond.Value); err == nil {
			value = intVal
		}

		switch operator {
		case "$eq", "=":
			matchConditions[field] = value
		case "$ne", "!=":
			matchConditions[field] = bson.M{"$ne": value}
		case "$gt", ">":
			matchConditions[field] = bson.M{"$gt": value}
		case "$lt", "<":
			matchConditions[field] = bson.M{"$lt": value}
		case "$gte", ">=":
			matchConditions[field] = bson.M{"$gte": value}
		case "$lte", "<=":
			matchConditions[field] = bson.M{"$lte": value}
		default:
			matchConditions[field] = value
		}
	}

	return bson.M{"$match": matchConditions}
}

// BuildMongoDBSortStage constructs $sort stage for ORDER BY
func BuildMongoDBSortStage(orderBy []*pb.OrderByClause) bson.M {
	sortFields := bson.M{}

	for _, ob := range orderBy {
		field := ExtractFieldName(ob.Field)
		direction := 1
		if ob.Direction == "-1" || strings.ToUpper(ob.Direction) == "DESC" {
			direction = -1
		}
		sortFields[field] = direction
	}

	return bson.M{"$sort": sortFields}
}

// ExtractFieldName extracts field name from table.field format
func ExtractFieldName(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		return parts[1]
	}
	return field
}

// ============================================================================
// TCL OPERATIONS - TRANSACTION BUILDERS
// ============================================================================

// BuildTransactionOptions builds MongoDB transaction options with read/write concerns
func BuildTransactionOptions() *options.TransactionOptions {
	txnOpts := options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	
	return txnOpts
}

// IsUnsupportedTCLOperation checks if the TCL operation is unsupported in MongoDB
func IsUnsupportedTCLOperation(operation string) bool {
	unsupportedOps := map[string]bool{
		"savepoint":               true,
		"rollback_to":             true,
		"rollback_to_savepoint":   true,
		"release_savepoint":       true,
	}
	
	return unsupportedOps[strings.ToLower(operation)]
}

// GetUnsupportedOperationError returns an error message for unsupported MongoDB TCL operations
func GetUnsupportedOperationError(operation string) error {
	operation = strings.ToLower(operation)
	
	switch operation {
	case "savepoint":
		return fmt.Errorf(
			"SAVEPOINT not supported in MongoDB.\n\n" +
			"MongoDB uses all-or-nothing transactions without partial rollback capability.\n\n" +
			"Alternatives:\n" +
			"  1. Break workflow into smaller, independent transactions\n" +
			"  2. Use application-level state management\n" +
			"  3. Implement document versioning for rollback capability\n" +
			"  4. Consider PostgreSQL or MySQL if savepoints are required",
		)
	
	case "rollback_to", "rollback_to_savepoint":
		return fmt.Errorf(
			"ROLLBACK TO SAVEPOINT not supported in MongoDB.\n\n" +
			"MongoDB transactions can only be fully committed or fully aborted.\n\n" +
			"Use ROLLBACK (or ABORT) to undo the entire transaction.",
		)
	
	case "release_savepoint":
		return fmt.Errorf(
			"RELEASE SAVEPOINT not supported in MongoDB.\n\n" +
			"MongoDB does not support savepoints, so there are no savepoints to release.\n\n" +
			"This operation is a no-op in MongoDB context.",
		)
	
	default:
		return fmt.Errorf("unsupported TCL operation: %s", operation)
	}
}