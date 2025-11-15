package translator

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

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
		
		switch query.Aggregate.Function {
		case "COUNT":
			command = "COUNT"
			args = []string{}
			
		case "SUM":
			command = "SUM"
			args = []string{query.Aggregate.Field} // Field to sum
			
		case "AVG":
			command = "AVG"
			args = []string{query.Aggregate.Field} // Field to average
			
		case "MIN":
			command = "MIN"
			args = []string{query.Aggregate.Field} // Field to find minimum
			
		case "MAX":
			command = "MAX"
			args = []string{query.Aggregate.Field} // Field to find maximum
			
		default:
			return nil, fmt.Errorf("aggregation function %s not supported in Redis", query.Aggregate.Function)
		}
		
		// ✅ ADD THESE 4 LINES
		// Add LIMIT to args if present
		if query.Limit > 0 {
			args = append(args, "LIMIT", fmt.Sprintf("%d", query.Limit))
		}
		
		result := &pb.KeyValueQuery{
			Command: command,
			Key:     buildRedisKeyPattern(tenantID, query.Entity),
			Args:    args,
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
		// For ACL commands, we need to add the subcommand as the first argument
		args = buildACLArgs(query, tenantID)
		// ACL commands don't need a key
		key = ""
	} else {
		// Build Redis key with tenant prefix for non-ACL commands
		key = buildRedisKey(tenantID, query.Entity, query.Conditions, query.Fields, query.Operation)
		// Build arguments based on operation
		args = buildRedisArgs(query, command, tenantID)
	}
	
	result := &pb.KeyValueQuery{
		Command: command,
		Key:     key,
		Args:    args,
	}
	result.CommandString = buildRedisString(result)
	return result, nil
}

func buildBulkInsert(query *models.Query, tenantID string) (*pb.KeyValueQuery, error) {
	entityLower := strings.ToLower(query.Entity)
	var pairs []*pb.KeyValuePair
	
	for i, row := range query.BulkData {
		// Generate key for each row
		keyId := fmt.Sprintf("%d", i+1)
		if idField := getFieldValue(row, "id"); idField != "" {
			keyId = idField
		}
		
		// Build full key with tenant prefix
		fullKey := fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, keyId)
		
		// Convert fields to JSON value
		jsonValue := fieldsToJSON(row)
		
		pairs = append(pairs, &pb.KeyValuePair{
			Key:   fullKey,
			Value: jsonValue,
		})
	}
	
	return &pb.KeyValueQuery{
		Command:   "BULK INSERT",  // Special command
		BulkPairs: pairs,
	}, nil
}

// ============================================================================
// KEY BUILDING
// ============================================================================

// buildRedisKey constructs Redis key with tenant isolation
func buildRedisKey(tenantID, entity string, conditions []models.Condition, fields []models.Field, operation string) string {
	// Special handling for DELETE with no conditions
	if operation == "DELETE" && len(conditions) == 0 && len(fields) == 0 {
		// If deleting without specific ID, we need to handle this differently
		// For now, return empty to signal an error
		return ""
	}
	
	if entity == "" {
		return ""
	}
	
	entityLower := strings.ToLower(entity)
	
	// Check if there's an id condition (most common case)
	for _, cond := range conditions {
		if strings.ToLower(cond.Field) == "id" {
			// Include tenant prefix for isolation
			return fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, cond.Value)
		}
	}
	
	// Check if there's another field condition
	if len(conditions) > 0 {
		cond := conditions[0]
		return fmt.Sprintf("tenant:%s:%s:%s:%s", tenantID, entityLower, cond.Field, cond.Value)
	}
	
	// Check if there's an id field (for CREATE with explicit id)
	for _, field := range fields {
		if strings.ToLower(field.Name) == "id" {
			return fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, field.Value)
		}
	}
	
	// For CREATE without ID, generate a key
	if operation == "CREATE" || operation == "UPDATE" || operation == "UPSERT" || operation == "REPLACE" {
		// Generate a simple incrementing ID (in production, use UUID or similar)
		return fmt.Sprintf("tenant:%s:%s:generated_%d", tenantID, entityLower, generateID())
	}
	
	// For operations without specific ID
	return fmt.Sprintf("tenant:%s:%s", tenantID, entityLower)
}

// Simple ID generator (in production, use something better)
var idCounter int
func generateID() int {
	idCounter++
	return idCounter
}

// ============================================================================
// ACL ARGUMENT BUILDING
// ============================================================================

// buildACLArgs builds arguments for ACL commands
func buildACLArgs(query *models.Query, tenantID string) []string {
	var args []string
	
	// Determine ACL subcommand based on OQL operation
	switch query.Operation {
	case "CREATE USER":
		args = append(args, "SETUSER")
		args = append(args, buildACLSetUserArgs(query, tenantID)...)
		
	case "DROP USER":
		args = append(args, "DELUSER")
		// Make sure we're getting the username correctly
		if query.Permission != nil && query.Permission.UserName != "" {
			args = append(args, query.Permission.UserName)
		} else if query.Entity != "" {
			// Fallback: use entity as username if no Permission struct
			args = append(args, query.Entity)
		} else {
			// Try to get from conditions or fields
			for _, cond := range query.Conditions {
				if cond.Field == "username" || cond.Field == "user" {
					args = append(args, cond.Value)
					break
				}
			}
		}
		
	case "ALTER USER":
		args = append(args, "SETUSER")
		args = append(args, buildACLSetUserArgs(query, tenantID)...)
		
	case "GRANT":
		args = append(args, "SETUSER")
		// For GRANT, we modify existing user permissions
		if query.Permission != nil {
			// The target is who we're granting TO
			target := query.Permission.Target
			if target == "" && query.Permission.UserName != "" {
				target = query.Permission.UserName
			}
			if target != "" {
				args = append(args, target)
				
				// Need to reset and re-add all permissions
				args = append(args, "resetkeys")
				args = append(args, "on")
				
				// Add key pattern for tenant
				args = append(args, fmt.Sprintf("~tenant:%s:*", tenantID))
				
				// Add new permissions
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
						// Pass through as-is
						args = append(args, "+"+strings.ToLower(perm))
					}
				}
			}
		}
		
	case "REVOKE":
		args = append(args, "SETUSER")
		// For REVOKE, we remove permissions
		if query.Permission != nil {
			// The target is who we're revoking FROM
			target := query.Permission.Target
			if target == "" && query.Permission.UserName != "" {
				target = query.Permission.UserName
			}
			if target != "" {
				args = append(args, target)
				
				// Remove permissions
				for _, perm := range query.Permission.Permissions {
					switch strings.ToUpper(perm) {
					case "GET", "SELECT", "READ":
						args = append(args, "-get", "-hgetall", "-lrange", "-smembers", "-zrange")
					case "SET", "INSERT", "WRITE":
						args = append(args, "-set", "-hset", "-hmset", "-lpush", "-sadd", "-zadd")
					case "DELETE", "DEL":
						args = append(args, "-del", "-hdel", "-lpop", "-srem", "-zrem")
					default:
						// Pass through as-is
						args = append(args, "-"+strings.ToLower(perm))
					}
				}
			}
		}
		
	default:
		// Unknown ACL operation - default to LIST
		args = append(args, "LIST")
	}
	
	return args
}

// buildACLSetUserArgs builds arguments for ACL SETUSER (CREATE USER/ALTER USER)
func buildACLSetUserArgs(query *models.Query, tenantID string) []string {
	var args []string
	
	if query.Permission == nil {
		return args
	}
	
	// Username
	args = append(args, query.Permission.UserName)
	
	// Reset keys first to avoid pattern conflicts
	args = append(args, "resetkeys")
	
	// Enable user
	args = append(args, "on")
	
	// Add password if provided
	if query.Permission.Password != "" {
		args = append(args, ">"+query.Permission.Password)
	}
	
	// Add key pattern access for tenant
	args = append(args, fmt.Sprintf("~tenant:%s:*", tenantID))
	
	// Add default permissions for new user - UPDATED to include hash operations
	args = append(args, "+get", "+set", "+del", "+exists", "+ttl", "+expire")
	args = append(args, "+hget", "+hset", "+hgetall", "+hdel", "+hmset")
	args = append(args, "+lpush", "+lpop", "+lrange", "+llen")
	args = append(args, "+sadd", "+srem", "+smembers", "+scard")
	args = append(args, "+zadd", "+zrem", "+zrange", "+zscore")
	
	return args
}

// ============================================================================
// REGULAR ARGUMENT BUILDING
// ============================================================================

// buildRedisArgs builds command arguments based on operation type
func buildRedisArgs(query *models.Query, command string, tenantID string) []string {
	var args []string
	
	switch command {
	case "HMSET", "HSET":
		// Hash operations: need field-value pairs
		args = buildHashSetArgs(query)
		
	case "HGETALL":
		// HGETALL doesn't need args, just the key
		// Return empty args
		
	case "SET":
		// SET key value (for native Redis commands, keep old behavior)
		args = buildSetArgs(query)
		
	case "GET":
		// GET key (no additional args)
		
	case "DEL":
		// DEL key [key ...]
		// If we have a key, no additional args needed
		// If key is empty, we need to build keys from conditions
		if query.Entity != "" && len(query.Conditions) == 0 {
			// Trying to delete without specific key - this is an error
			// Return empty args to trigger error
			return []string{}
		}
		
	case "MSET":
		// MSET key1 value1 key2 value2 ...
		args = buildBulkSetArgs(query, tenantID)
		
	case "MULTI", "EXEC", "DISCARD":
		// Transaction commands (no args)
		
	default:
		// Generic args from fields
		args = buildGenericArgs(query)
	}
	
	return args
}

// buildHashSetArgs builds field-value pairs for HMSET/HSET
// Converts Query fields to: ["field1", "value1", "field2", "value2"]
func buildHashSetArgs(query *models.Query) []string {
	var args []string
	
	// Convert fields to alternating field-value pairs
	// HMSET/HSET expects: field1 value1 field2 value2 ...
	for _, field := range query.Fields {
		if field.Name != "id" {  // Skip id field (it's in the key)
			// Check if field has an expression (e.g., value = value + 1)
			if field.Expression != nil {
				// Convert expression to string
				exprString := expressionToString(field.Expression)
				args = append(args, field.Name, exprString)
			} else {
				// Use the plain value
				args = append(args, field.Name, field.Value)
			}
		}
	}
	
	// Safeguard: if no fields after filtering, add a placeholder
	// This prevents "wrong number of arguments" errors
	if len(args) == 0 {
		args = append(args, "_placeholder", "empty")
	}
	
	return args
}

// expressionToString converts a FieldExpression to string format
func expressionToString(expr *models.FieldExpression) string {
	if expr == nil {
		return ""
	}
	
	switch expr.Type {
	case "BINARY":
		// Binary expression: "value + 1" or "balance - 100"
		return fmt.Sprintf("%s %s %s", expr.LeftOperand, expr.Operator, expr.RightOperand)
	
	case "FUNCTION":
		// Function call: "UPPER(name)"
		return fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(expr.FunctionArgs, ", "))
	
	case "CASEWHEN":
		// CASE WHEN statement (more complex, return as-is for now)
		return "CASE_EXPRESSION"
	
	default:
		return ""
	}
}

// buildSetArgs builds arguments for SET command (for native Redis usage)
func buildSetArgs(query *models.Query) []string {
	var args []string
	
	// Convert fields to JSON or simple value
	if len(query.Fields) == 1 && query.Fields[0].Name == "value" {
		// Simple value: SET key value
		args = append(args, query.Fields[0].Value)
	} else if len(query.Fields) == 1 && query.Fields[0].Name != "id" {
		// Single non-id field, use its value
		args = append(args, query.Fields[0].Value)
	} else {
		// Multiple fields: serialize to JSON
		jsonValue := fieldsToJSON(query.Fields)
		args = append(args, jsonValue)
	}
	
	return args
}

// buildBulkSetArgs builds arguments for MSET (bulk insert)
func buildBulkSetArgs(query *models.Query, tenantID string) []string {
	var args []string
	
	// For MSET, we need key-value pairs
	// But Redis MSET expects: MSET key1 value1 key2 value2
	// We need to pass additional keys, not use the main key
	entityLower := strings.ToLower(query.Entity)
	
	for i, row := range query.BulkData {
		// Generate key for each row
		keyId := fmt.Sprintf("%d", i+1)
		if idField := getFieldValue(row, "id"); idField != "" {
			keyId = idField
		}
		
		// Add key with full path
		fullKey := fmt.Sprintf("tenant:%s:%s:%s", tenantID, entityLower, keyId)
		args = append(args, fullKey)
		
		// Add value as JSON
		jsonValue := fieldsToJSON(row)
		args = append(args, jsonValue)
	}
	
	return args
}

// buildGenericArgs builds generic arguments from fields
func buildGenericArgs(query *models.Query) []string {
	var args []string
	
	for _, field := range query.Fields {
		args = append(args, field.Value)
	}
	
	return args
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// fieldsToJSON converts fields to simple JSON format
func fieldsToJSON(fields []models.Field) string {
	if len(fields) == 0 {
		return "{}"
	}
	
	var pairs []string
	for _, field := range fields {
		// Simple JSON: "field":"value"
		pairs = append(pairs, fmt.Sprintf(`"%s":"%s"`, field.Name, field.Value))
	}
	
	return "{" + strings.Join(pairs, ",") + "}"
}

// getFieldValue gets a field value by name
func getFieldValue(fields []models.Field, name string) string {
	for _, field := range fields {
		if strings.ToLower(field.Name) == strings.ToLower(name) {
			return field.Value
		}
	}
	return ""
}

// buildRedisKeyPattern builds a pattern for KEYS/SCAN operations
func buildRedisKeyPattern(tenantID, entity string) string {
    entityLower := strings.ToLower(entity)
    return fmt.Sprintf("tenant:%s:%s:*", tenantID, entityLower)
}

// buildRedisString generates Redis command string for OmniQL users
func buildRedisString(query *pb.KeyValueQuery) string {
	if query == nil {
		return ""
	}
	
	command := strings.ToUpper(query.Command)
	
	// Handle special cases
	switch command {
	case "BULK INSERT":
		// For bulk insert, show multiple HMSET commands
		var commands []string
		for _, pair := range query.BulkPairs {
			commands = append(commands, fmt.Sprintf("HMSET %s <fields>", pair.Key))
		}
		return strings.Join(commands, "\n")
		
	case "DROP_TABLE":
		// For drop table, show DEL with pattern
		return fmt.Sprintf("DEL %s", query.Key)
		
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		// Aggregation operations
		if len(query.Args) > 0 {
			return fmt.Sprintf("%s %s %s", command, query.Key, strings.Join(query.Args, " "))
		}
		return fmt.Sprintf("%s %s", command, query.Key)
	}
	
	// Build standard Redis command
	parts := []string{command}
	
	// Add key if present
	if query.Key != "" {
		parts = append(parts, query.Key)
	}
	
	// Add args if present
	if len(query.Args) > 0 {
		parts = append(parts, query.Args...)
	}
	
	return strings.Join(parts, " ")
}