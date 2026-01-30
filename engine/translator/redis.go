package translator

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// ============================================================================
// HELPER FUNCTIONS (100% TrueAST)
// ============================================================================

// getExprValue safely extracts value from Expression
func getExprValue(expr *models.Expression) string {
	if expr == nil {
		return ""
	}
	return expr.Value
}

// buildExpressionValue converts TrueAST Expression back to string
// This preserves SSOT - translator returns strings, handler interprets them
func buildExpressionValue(expr *models.Expression) string {
	if expr == nil {
		return ""
	}
	
	switch expr.Type {
	case "LITERAL":
		return expr.Value
	case "FIELD":
		return expr.Value
	case "BINARY":
		left := buildExpressionValue(expr.Left)
		right := buildExpressionValue(expr.Right)
		return fmt.Sprintf("%s %s %s", left, expr.Operator, right)
	case "FUNCTION":
		return expr.Value
	default:
		return expr.Value
	}
}

// ============================================================================
// MAIN TRANSLATOR
// ============================================================================

// TranslateRedis converts OQL Query to Redis KeyValueQuery
func TranslateRedis(query *models.Query, tenantID string) (*pb.KeyValueQuery, error) {
	// Check for unsupported operations first
	switch query.Operation {
	case "CREATE ROLE", "DROP ROLE", "ASSIGN ROLE", "REVOKE ROLE":
		return nil, fmt.Errorf("Redis does not support role operations. Use CREATE USER with permissions instead")
	}

	// Special handling for ALL aggregations (COUNT, SUM, AVG, MIN, MAX)
	if query.Aggregate != nil {
		var command string
		var args []string
		
		fieldValue := getExprValue(query.Aggregate.FieldExpr)
		
		switch query.Aggregate.Function {
		case "COUNT":
			command = "COUNT"
			args = []string{}
			
		case "SUM":
			command = "SUM"
			args = []string{fieldValue}
			
		case "AVG":
			command = "AVG"
			args = []string{fieldValue}
			
		case "MIN":
			command = "MIN"
			args = []string{fieldValue}
			
		case "MAX":
			command = "MAX"
			args = []string{fieldValue}
			
		default:
			return nil, fmt.Errorf("aggregation function %s not supported in Redis", query.Aggregate.Function)
		}
		
		// Add LIMIT to args if present
		if query.Limit > 0 {
			args = append(args, "LIMIT", fmt.Sprintf("%d", query.Limit))
		}
		
		result := &pb.KeyValueQuery{
			Command:    command,
			Key:        buildRedisKeyPattern(tenantID, query.Entity),
			Args:       args,
			Entity:     strings.ToLower(query.Entity),
			Conditions: convertConditionsToProto(query.Conditions),
			Limit:      int32(query.Limit),
			Offset:     int32(query.Offset),
			OrderBy:    convertOrderByToProto(query.OrderBy),
		}
		result.CommandString = buildRedisString(result)
		return result, nil
	}

	// Special handling for DROP TABLE  
	if query.Operation == "DROP TABLE" {
		result := &pb.KeyValueQuery{
			Command: "DROP_TABLE",
			Key:     buildRedisKeyPattern(tenantID, query.Entity),
			Args:    []string{},
			Entity:  strings.ToLower(query.Entity),
		}
		result.CommandString = buildRedisString(result)
		return result, nil
	}

	// Special handling for BULK INSERT
	if query.Operation == "BULK INSERT" {
		result, err := buildBulkInsert(query, tenantID)
		if err != nil {
			return nil, err
		}
		result.CommandString = buildRedisString(result)
		return result, nil
	}

	command := mapping.OperationMap["Redis"][query.Operation]
	if command == "" {
		return nil, fmt.Errorf("operation %s not supported in Redis", query.Operation)
	}
	
	// Special handling for ACL commands
	var args []string
	var key string
	
	if command == "ACL" {
		args = buildACLArgs(query, tenantID)
		key = ""
	} else {
		key = buildRedisKey(tenantID, query.Entity, query.Conditions, query.Fields, query.Operation)
		args = buildRedisArgs(query, command, tenantID)
	}
	
	result := &pb.KeyValueQuery{
		Command:    command,
		Key:        key,
		Args:       args,
		Entity:     strings.ToLower(query.Entity),
		Conditions: convertConditionsToProto(query.Conditions),
		Limit:      int32(query.Limit),
		Offset:     int32(query.Offset),
		OrderBy:    convertOrderByToProto(query.OrderBy),
	}
	result.CommandString = buildRedisString(result)
	return result, nil
}

func buildBulkInsert(query *models.Query, tenantID string) (*pb.KeyValueQuery, error) {
	entityLower := strings.ToLower(query.Entity)
	var pairs []*pb.KeyValuePair
	
	for i, row := range query.BulkData {
		keyId := fmt.Sprintf("%d", i+1)
		if idField := getFieldValue(row, "id"); idField != "" {
			keyId = idField
		}
		
		fullKey := fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, keyId)
		jsonValue := fieldsToJSON(row)
		
		pairs = append(pairs, &pb.KeyValuePair{
			Key:   fullKey,
			Value: jsonValue,
		})
	}
	
	return &pb.KeyValueQuery{
		Command:   "BULK INSERT",
		BulkPairs: pairs,
	}, nil
}

// ============================================================================
// KEY BUILDING (100% TrueAST)
// ============================================================================

func buildRedisKey(tenantID, entity string, conditions []models.Condition, fields []models.Field, operation string) string {
	if operation == "DELETE" && len(conditions) == 0 && len(fields) == 0 {
		return ""
	}
	
	if entity == "" {
		return ""
	}
	
	entityLower := strings.ToLower(entity)
	
	// Case 1: Direct ID lookup (WHERE id = X)
	if isDirectIdLookup(conditions) {
		condValue := getExprValue(conditions[0].ValueExpr)
		return fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, condValue)
	}
	
	// Case 2: Other conditions → return pattern for scanning
	if len(conditions) > 0 {
		return fmt.Sprintf("tenant:%s:%s:*", tenantID, entityLower)
	}
	
	// Check if there's an id field (for CREATE)
	for _, field := range fields {
		nameValue := getExprValue(field.NameExpr)
		if strings.ToLower(nameValue) == "id" {
			fieldValue := getExprValue(field.ValueExpr)
			return fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, fieldValue)
		}
	}
	
	// For CREATE without ID, generate a key
	if operation == "CREATE" || operation == "UPDATE" || operation == "UPSERT" || operation == "REPLACE" {
		return fmt.Sprintf("tenant:%s:%s:generated_%d", tenantID, entityLower, generateID())
	}
	
	return fmt.Sprintf("tenant:%s:%s:*", tenantID, entityLower)
}

var idCounter int
func generateID() int {
	idCounter++
	return idCounter
}

// ============================================================================
// ACL ARGUMENT BUILDING (100% TrueAST)
// ============================================================================

func buildACLArgs(query *models.Query, tenantID string) []string {
	var args []string
	
	switch query.Operation {
	case "CREATE USER":
		args = append(args, "SETUSER")
		args = append(args, buildACLSetUserArgs(query, tenantID)...)
		
	case "DROP USER":
		args = append(args, "DELUSER")
		if query.Permission != nil && query.Permission.UserName != "" {
			args = append(args, query.Permission.UserName)
		} else if query.Entity != "" {
			args = append(args, query.Entity)
		} else {
			for _, cond := range query.Conditions {
				fieldValue := getExprValue(cond.FieldExpr)
				if fieldValue == "username" || fieldValue == "user" {
					condValue := getExprValue(cond.ValueExpr)
					args = append(args, condValue)
					break
				}
			}
		}
		
	case "ALTER USER":
		args = append(args, "SETUSER")
		args = append(args, buildACLSetUserArgs(query, tenantID)...)
		
	case "GRANT":
		args = append(args, "SETUSER")
		if query.Permission != nil {
			target := query.Permission.Target
			if target == "" && query.Permission.UserName != "" {
				target = query.Permission.UserName
			}
			if target != "" {
				args = append(args, target)
				args = append(args, "resetkeys")
				args = append(args, "on")
				args = append(args, fmt.Sprintf("~tenant:%s:*", tenantID))
				
				for _, perm := range query.Permission.Permissions {
					switch strings.ToUpper(perm) {
					case "GET", "SELECT", "READ":
						args = append(args, "+get", "+hgetall", "+lrange", "+smembers", "+zrange")
					case "SET", "INSERT", "WRITE":
						args = append(args, "+set", "+hset", "+hmset", "+lpush", "+sadd", "+zadd")
					case "DELETE", "DEL":
						args = append(args, "+del", "+hdel", "+lpop", "+srem", "+zrem")
					case "ALL":
						args = append(args, "+@all")
					default:
						args = append(args, "+"+strings.ToLower(perm))
					}
				}
			}
		}
		
	case "REVOKE":
		args = append(args, "SETUSER")
		if query.Permission != nil {
			target := query.Permission.Target
			if target == "" && query.Permission.UserName != "" {
				target = query.Permission.UserName
			}
			if target != "" {
				args = append(args, target)
				
				for _, perm := range query.Permission.Permissions {
					switch strings.ToUpper(perm) {
					case "GET", "SELECT", "READ":
						args = append(args, "-get", "-hgetall", "-lrange", "-smembers", "-zrange")
					case "SET", "INSERT", "WRITE":
						args = append(args, "-set", "-hset", "-hmset", "-lpush", "-sadd", "-zadd")
					case "DELETE", "DEL":
						args = append(args, "-del", "-hdel", "-lpop", "-srem", "-zrem")
					default:
						args = append(args, "-"+strings.ToLower(perm))
					}
				}
			}
		}
		
	default:
		args = append(args, "LIST")
	}
	
	return args
}

func buildACLSetUserArgs(query *models.Query, tenantID string) []string {
	var args []string
	
	if query.Permission == nil {
		return args
	}
	
	args = append(args, query.Permission.UserName)
	args = append(args, "resetkeys")
	args = append(args, "on")
	
	if query.Permission.Password != "" {
		args = append(args, ">"+query.Permission.Password)
	}
	
	args = append(args, fmt.Sprintf("~tenant:%s:*", tenantID))
	args = append(args, "+get", "+set", "+del", "+exists", "+ttl", "+expire")
	args = append(args, "+hget", "+hset", "+hgetall", "+hdel", "+hmset")
	args = append(args, "+lpush", "+lpop", "+lrange", "+llen")
	args = append(args, "+sadd", "+srem", "+smembers", "+scard")
	args = append(args, "+zadd", "+zrem", "+zrange", "+zscore")
	
	return args
}

// ============================================================================
// REGULAR ARGUMENT BUILDING (100% TrueAST)
// ============================================================================

func buildRedisArgs(query *models.Query, command string, tenantID string) []string {
	var args []string
	
	switch command {
	case "HMSET", "HSET":
		args = buildHashSetArgs(query)
		
	case "HGETALL":
		// No args needed
		
	case "SET":
		args = buildSetArgs(query)
		
	case "GET":
		// No args needed
		
	case "DEL":
		if query.Entity != "" && len(query.Conditions) == 0 {
			return []string{}
		}
		
	case "MSET":
		args = buildBulkSetArgs(query, tenantID)
		
	case "MULTI", "EXEC", "DISCARD":
		// Transaction commands (no args)
		
	default:
		args = buildGenericArgs(query)
	}
	
	return args
}

func buildHashSetArgs(query *models.Query) []string {
	var args []string
	
	for _, field := range query.Fields {
		nameValue := getExprValue(field.NameExpr)
		if nameValue != "id" {
			// TrueAST: Convert BINARY expressions back to string for handler
			var valueValue string
			if field.ValueExpr != nil && (field.ValueExpr.Type == "BINARY" || field.ValueExpr.Type == "FUNCTION") {
				valueValue = buildExpressionValue(field.ValueExpr)
			} else {
				valueValue = getExprValue(field.ValueExpr)
			}
			args = append(args, nameValue, valueValue)
		}
	}
	
	if len(args) == 0 {
		args = append(args, "_placeholder", "empty")
	}
	
	return args
}

func buildSetArgs(query *models.Query) []string {
	var args []string
	
	if len(query.Fields) == 1 {
		nameValue := getExprValue(query.Fields[0].NameExpr)
		var valueValue string
		if query.Fields[0].ValueExpr != nil && query.Fields[0].ValueExpr.Type == "BINARY" {
			valueValue = buildExpressionValue(query.Fields[0].ValueExpr)
		} else {
			valueValue = getExprValue(query.Fields[0].ValueExpr)
		}
		if nameValue == "value" || nameValue != "id" {
			args = append(args, valueValue)
		}
	} else {
		jsonValue := fieldsToJSON(query.Fields)
		args = append(args, jsonValue)
	}
	
	return args
}

func buildBulkSetArgs(query *models.Query, tenantID string) []string {
	var args []string
	entityLower := strings.ToLower(query.Entity)
	
	for i, row := range query.BulkData {
		keyId := fmt.Sprintf("%d", i+1)
		if idField := getFieldValue(row, "id"); idField != "" {
			keyId = idField
		}
		
		fullKey := fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, keyId)
		args = append(args, fullKey)
		
		jsonValue := fieldsToJSON(row)
		args = append(args, jsonValue)
	}
	
	return args
}

func buildGenericArgs(query *models.Query) []string {
	var args []string
	
	for _, field := range query.Fields {
		var valueValue string
		if field.ValueExpr != nil && field.ValueExpr.Type == "BINARY" {
			valueValue = buildExpressionValue(field.ValueExpr)
		} else {
			valueValue = getExprValue(field.ValueExpr)
		}
		args = append(args, valueValue)
	}
	
	return args
}

// ============================================================================
// HELPER FUNCTIONS (100% TrueAST)
// ============================================================================

func fieldsToJSON(fields []models.Field) string {
	if len(fields) == 0 {
		return "{}"
	}
	
	var pairs []string
	for _, field := range fields {
		nameValue := getExprValue(field.NameExpr)
		var valueValue string
		if field.ValueExpr != nil && field.ValueExpr.Type == "BINARY" {
			valueValue = buildExpressionValue(field.ValueExpr)
		} else {
			valueValue = getExprValue(field.ValueExpr)
		}
		pairs = append(pairs, fmt.Sprintf(`"%s":"%s"`, nameValue, valueValue))
	}
	
	return "{" + strings.Join(pairs, ",") + "}"
}

func getFieldValue(fields []models.Field, name string) string {
	for _, field := range fields {
		nameValue := getExprValue(field.NameExpr)
		if strings.ToLower(nameValue) == strings.ToLower(name) {
			return getExprValue(field.ValueExpr)
		}
	}
	return ""
}

func buildRedisKeyPattern(tenantID, entity string) string {
	entityLower := strings.ToLower(entity)
	return fmt.Sprintf("tenant:%s:%s:*", tenantID, entityLower)
}

func buildRedisString(query *pb.KeyValueQuery) string {
	if query == nil {
		return ""
	}
	
	command := strings.ToUpper(query.Command)
	
	switch command {
	case "BULK INSERT":
		var commands []string
		for _, pair := range query.BulkPairs {
			commands = append(commands, fmt.Sprintf("HMSET %s <fields>", pair.Key))
		}
		return strings.Join(commands, "\n")
		
	case "DROP_TABLE":
		return fmt.Sprintf("DEL %s", query.Key)
		
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		if len(query.Args) > 0 {
			return fmt.Sprintf("%s %s %s", command, query.Key, strings.Join(query.Args, " "))
		}
		return fmt.Sprintf("%s %s", command, query.Key)
	}
	
	parts := []string{command}
	
	if query.Key != "" {
		parts = append(parts, query.Key)
	}
	
	if len(query.Args) > 0 {
		parts = append(parts, query.Args...)
	}
	
	return strings.Join(parts, " ")
}

// convertConditionsToProto converts models.Condition to proto QueryCondition
func convertConditionsToProto(conditions []models.Condition) []*pb.QueryCondition {
	var result []*pb.QueryCondition
	for _, cond := range conditions {
		pc := &pb.QueryCondition{
			Operator: cond.Operator,
			Logic:    cond.Logic,
		}
		if cond.FieldExpr != nil {
			pc.FieldExpr = convertExprToProto(cond.FieldExpr)
		}
		if cond.ValueExpr != nil {
			pc.ValueExpr = convertExprToProto(cond.ValueExpr)
		}
		if cond.Value2Expr != nil {
			pc.Value2Expr = convertExprToProto(cond.Value2Expr)
		}
		for _, v := range cond.ValuesExpr {
			pc.ValuesExpr = append(pc.ValuesExpr, convertExprToProto(v))
		}
		if len(cond.Nested) > 0 {
			pc.Nested = convertConditionsToProto(cond.Nested)
		}
		result = append(result, pc)
	}
	return result
}

// convertExprToProto converts models.Expression to proto Expression
func convertExprToProto(expr *models.Expression) *pb.Expression {
	if expr == nil {
		return nil
	}
	return &pb.Expression{
		Type:  expr.Type,
		Value: expr.Value,
	}
}

// convertOrderByToProto converts models.OrderBy to proto OrderByClause
func convertOrderByToProto(orderBy []models.OrderBy) []*pb.OrderByClause {
	var result []*pb.OrderByClause
	for _, ob := range orderBy {
		poc := &pb.OrderByClause{
			Direction: string(ob.Direction),
		}
		if ob.FieldExpr != nil {
			poc.FieldExpr = &pb.Expression{
				Type:  ob.FieldExpr.Type,
				Value: ob.FieldExpr.Value,
			}
		}
		result = append(result, poc)
	}
	return result
}

// isDirectIdLookup checks if conditions are just "id = X"
func isDirectIdLookup(conditions []models.Condition) bool {
	if len(conditions) != 1 {
		return false
	}
	cond := conditions[0]
	fieldValue := getExprValue(cond.FieldExpr)
	return strings.ToLower(fieldValue) == "id" && cond.Operator == "="
}
