package reverse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// ENTRY POINT
// ============================================================================

// RedisToQuery converts a Redis command string to an OQL Query
func RedisToQuery(commandStr string) (*models.Query, error) {
	if commandStr == "" {
		return nil, fmt.Errorf("%w: empty command", ErrParseError)
	}

	// Parse command and arguments
	parts := parseRedisCommand(commandStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: invalid command format", ErrParseError)
	}

	command := strings.ToUpper(parts[0])

	// ==================== TCL ====================
	switch command {
	case "MULTI":
		return &models.Query{
			Operation:   "BEGIN",
			Transaction: &models.Transaction{Operation: "BEGIN"},
		}, nil

	case "EXEC":
		return &models.Query{
			Operation:   "COMMIT",
			Transaction: &models.Transaction{Operation: "COMMIT"},
		}, nil

	case "DISCARD":
		return &models.Query{
			Operation:   "ROLLBACK",
			Transaction: &models.Transaction{Operation: "ROLLBACK"},
		}, nil
	}

	// ==================== CRUD ====================
	switch command {
	case "HGETALL":
		return convertRedisHGetAll(parts)

	case "HMSET":
		return convertRedisHMSet(parts)

	case "HSET":
		return convertRedisHSet(parts)

	case "GET":
		return convertRedisGet(parts)

	case "SET":
		return convertRedisSet(parts)

	case "DEL":
		return convertRedisDel(parts)

	case "MSET":
		return convertRedisMSet(parts)
	}

	// ==================== AGGREGATES ====================
	switch command {
	case "COUNT":
		return convertRedisAggregate(parts, "COUNT")

	case "SUM":
		return convertRedisAggregate(parts, "SUM")

	case "AVG":
		return convertRedisAggregate(parts, "AVG")

	case "MIN":
		return convertRedisAggregate(parts, "MIN")

	case "MAX":
		return convertRedisAggregate(parts, "MAX")
	}

	// ==================== DCL ====================
	if command == "ACL" {
		return convertRedisACL(parts)
	}

	// ==================== DDL ====================
	if command == "DROP_TABLE" {
		return convertRedisDropTable(parts)
	}

	// ==================== BULK INSERT ====================
	if command == "BULK" && len(parts) > 1 && strings.ToUpper(parts[1]) == "INSERT" {
		return convertRedisBulkInsert(parts)
	}

	// ==================== EXTENDED OPERATIONS ====================
	// Route to advanced handler for additional commands
	return ConvertExtendedRedisCommand(parts)
}

// ============================================================================
// COMMAND PARSING
// ============================================================================

// parseRedisCommand splits a Redis command string into parts
// Handles quoted strings and preserves field-value pairs
func parseRedisCommand(commandStr string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)

	for _, r := range commandStr {
		switch {
		case (r == '"' || r == '\'') && !inQuotes:
			inQuotes = true
			quoteChar = r
		case r == quoteChar && inQuotes:
			inQuotes = false
			quoteChar = 0
		case r == ' ' && !inQuotes:
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// ============================================================================
// KEY PARSING
// ============================================================================

// RedisKeyInfo contains parsed information from a Redis key
type RedisKeyInfo struct {
	TenantID   string
	Entity     string
	ID         string
	Field      string // For field:value pattern
	FieldValue string
	IsPattern  bool // true if key ends with *
}

// parseRedisKey parses a Redis key into its components
// Formats:
//   - tenant:tenantId:entity:id
//   - tenant:tenantId:entity:*
//   - tenant:tenantId:entity:field:value
//   - tenant:tenantId:entity
func parseRedisKey(key string) *RedisKeyInfo {
	info := &RedisKeyInfo{}

	// Check for pattern
	if strings.HasSuffix(key, ":*") || strings.HasSuffix(key, "*") {
		info.IsPattern = true
		key = strings.TrimSuffix(key, ":*")
		key = strings.TrimSuffix(key, "*")
	}

	parts := strings.Split(key, ":")

	// Expected format: tenant:tenantId:entity[:id|:field:value]
	if len(parts) < 3 {
		return info
	}

	// parts[0] = "tenant"
	info.TenantID = parts[1]

	if len(parts) >= 3 {
		info.Entity = parts[2]
	}

	if len(parts) == 4 {
		// Could be ID or field name
		info.ID = parts[3]
	}

	if len(parts) == 5 {
		// field:value pattern
		info.Field = parts[3]
		info.FieldValue = parts[4]
	}

	return info
}

// entityFromKey extracts entity name and converts to PascalCase
func entityFromKey(key string) string {
	info := parseRedisKey(key)
	if info.Entity == "" {
		return ""
	}
	return TableToEntity(info.Entity)
}

// ============================================================================
// CRUD: HGETALL → GET
// ============================================================================

func convertRedisHGetAll(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: HGETALL requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Add condition if ID present
	if keyInfo.ID != "" && keyInfo.ID != "*" && !keyInfo.IsPattern {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		}
	}

	// Add condition if field:value pattern
	if keyInfo.Field != "" && keyInfo.FieldValue != "" {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr(keyInfo.Field),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.FieldValue),
			},
		}
	}

	return query, nil
}

// ============================================================================
// CRUD: GET → GET (simple key-value)
// ============================================================================

func convertRedisGet(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: GET requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	if keyInfo.ID != "" {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		}
	}

	return query, nil
}

// ============================================================================
// CRUD: HMSET → CREATE
// ============================================================================

func convertRedisHMSet(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: HMSET requires key and field-value pairs", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "CREATE",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Add ID field if present in key
	if keyInfo.ID != "" && !strings.HasPrefix(keyInfo.ID, "generated_") {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("id"),
			ValueExpr: LiteralExpr(keyInfo.ID),
		})
	}

	// Parse field-value pairs (starting from index 2)
	for i := 2; i < len(parts)-1; i += 2 {
		fieldName := parts[i]
		fieldValue := parts[i+1]

		// Skip placeholder fields
		if fieldName == "_placeholder" {
			continue
		}

		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr(fieldName),
			ValueExpr: parseFieldValue(fieldValue),
		})
	}

	return query, nil
}

// ============================================================================
// CRUD: SET → CREATE (simple key-value)
// ============================================================================

func convertRedisSet(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: SET requires key and value", ErrParseError)
	}

	key := parts[1]
	value := parts[2]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "CREATE",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Add ID field if present
	if keyInfo.ID != "" {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("id"),
			ValueExpr: LiteralExpr(keyInfo.ID),
		})
	}

	// Check if value is JSON
	if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
		fields := parseJSONFields(value)
		query.Fields = append(query.Fields, fields...)
	} else {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("value"),
			ValueExpr: LiteralExpr(value),
		})
	}

	return query, nil
}

// ============================================================================
// CRUD: HSET → UPDATE
// ============================================================================

func convertRedisHSet(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: HSET requires key and field-value pair", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Add condition for ID
	if keyInfo.ID != "" {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		}
	}

	// Parse field-value pairs
	for i := 2; i < len(parts)-1; i += 2 {
		fieldName := parts[i]
		fieldValue := parts[i+1]

		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr(fieldName),
			ValueExpr: parseFieldValue(fieldValue),
		})
	}

	return query, nil
}

// ============================================================================
// CRUD: DEL → DELETE
// ============================================================================

func convertRedisDel(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: DEL requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	// Pattern deletion → DROP TABLE
	if keyInfo.IsPattern {
		return &models.Query{
			Operation: "DROP TABLE",
			Entity:    TableToEntity(keyInfo.Entity),
		}, nil
	}

	query := &models.Query{
		Operation: "DELETE",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	if keyInfo.ID != "" {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		}
	}

	return query, nil
}

// ============================================================================
// CRUD: MSET → BULK INSERT
// ============================================================================

func convertRedisMSet(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: MSET requires key-value pairs", ErrParseError)
	}

	query := &models.Query{
		Operation: "BULK INSERT",
	}

	// Parse key-value pairs
	for i := 1; i < len(parts)-1; i += 2 {
		key := parts[i]
		value := parts[i+1]

		keyInfo := parseRedisKey(key)

		// Set entity from first key
		if query.Entity == "" {
			query.Entity = TableToEntity(keyInfo.Entity)
		}

		var row []models.Field

		// Add ID field
		if keyInfo.ID != "" {
			row = append(row, models.Field{
				NameExpr:  FieldExpr("id"),
				ValueExpr: LiteralExpr(keyInfo.ID),
			})
		}

		// Parse value (JSON or simple)
		if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
			row = append(row, parseJSONFields(value)...)
		} else {
			row = append(row, models.Field{
				NameExpr:  FieldExpr("value"),
				ValueExpr: LiteralExpr(value),
			})
		}

		query.BulkData = append(query.BulkData, row)
	}

	return query, nil
}

// ============================================================================
// DDL: DROP_TABLE / DEL pattern → DROP TABLE
// ============================================================================

func convertRedisDropTable(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: DROP_TABLE requires key pattern", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	return &models.Query{
		Operation: "DROP TABLE",
		Entity:    TableToEntity(keyInfo.Entity),
	}, nil
}

// ============================================================================
// BULK INSERT (multi-line format)
// ============================================================================

func convertRedisBulkInsert(parts []string) (*models.Query, error) {
	// BULK INSERT is handled specially - typically multi-line
	// For now, return a basic structure
	return &models.Query{
		Operation: "BULK INSERT",
	}, nil
}

// ============================================================================
// AGGREGATES: COUNT, SUM, AVG, MIN, MAX
// ============================================================================

func convertRedisAggregate(parts []string, function string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: %s requires key pattern", ErrParseError, function)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: function,
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Field for SUM, AVG, MIN, MAX
	fieldName := "*"
	if function != "COUNT" && len(parts) > 2 {
		fieldName = parts[2]
	}

	query.Aggregate = &models.Aggregation{
		Function:  models.AggregateFunc(function),
		FieldExpr: FieldExpr(fieldName),
	}

	// Check for LIMIT in args
	for i := 2; i < len(parts)-1; i++ {
		if strings.ToUpper(parts[i]) == "LIMIT" {
			if limit, err := strconv.Atoi(parts[i+1]); err == nil {
				query.Limit = limit
			}
		}
	}

	return query, nil
}

// ============================================================================
// DCL: ACL → CREATE USER, DROP USER, GRANT, REVOKE, ALTER USER
// ============================================================================

func convertRedisACL(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: ACL requires subcommand", ErrParseError)
	}

	subCommand := strings.ToUpper(parts[1])

	switch subCommand {
	case "SETUSER":
		return convertRedisACLSetUser(parts)

	case "DELUSER":
		return convertRedisACLDelUser(parts)

	case "LIST":
		return &models.Query{
			Operation: "GET",
			Entity:    "User",
		}, nil

	default:
		return nil, fmt.Errorf("%w: unknown ACL subcommand: %s", ErrNotSupported, subCommand)
	}
}

func convertRedisACLSetUser(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: ACL SETUSER requires username", ErrParseError)
	}

	username := parts[2]

	// Analyze args to determine operation type
	hasPassword := false
	hasResetKeys := false
	permissions := []string{}

	for i := 3; i < len(parts); i++ {
		arg := parts[i]

		if strings.HasPrefix(arg, ">") {
			hasPassword = true
		}

		if arg == "resetkeys" {
			hasResetKeys = true
		}

		// Permission additions
		if strings.HasPrefix(arg, "+") {
			perm := strings.TrimPrefix(arg, "+")
			permissions = append(permissions, mapRedisPermissionToOQL(perm))
		}

		// Permission removals (for REVOKE)
		if strings.HasPrefix(arg, "-") {
			perm := strings.TrimPrefix(arg, "-")
			permissions = append(permissions, mapRedisPermissionToOQL(perm))
		}
	}

	// Determine operation based on args
	if hasResetKeys && hasPassword {
		// CREATE USER or ALTER USER with password
		query := &models.Query{
			Operation: "CREATE USER",
			Permission: &models.Permission{
				Operation: "CREATE USER",
				UserName:  username,
			},
		}

		// Extract password
		for i := 3; i < len(parts); i++ {
			if strings.HasPrefix(parts[i], ">") {
				query.Permission.Password = strings.TrimPrefix(parts[i], ">")
				break
			}
		}

		return query, nil
	}

	// Check for permission additions (GRANT)
	hasAdditions := false
	hasRemovals := false
	for i := 3; i < len(parts); i++ {
		if strings.HasPrefix(parts[i], "+") {
			hasAdditions = true
		}
		if strings.HasPrefix(parts[i], "-") {
			hasRemovals = true
		}
	}

	if hasAdditions && !hasRemovals {
		return &models.Query{
			Operation: "GRANT",
			Permission: &models.Permission{
				Operation:   "GRANT",
				Target:      username,
				Permissions: permissions,
			},
		}, nil
	}

	if hasRemovals {
		return &models.Query{
			Operation: "REVOKE",
			Permission: &models.Permission{
				Operation:   "REVOKE",
				Target:      username,
				Permissions: permissions,
			},
		}, nil
	}

	// Default: ALTER USER
	return &models.Query{
		Operation: "ALTER USER",
		Permission: &models.Permission{
			Operation: "ALTER USER",
			UserName:  username,
		},
	}, nil
}

func convertRedisACLDelUser(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: ACL DELUSER requires username", ErrParseError)
	}

	username := parts[2]

	return &models.Query{
		Operation: "DROP USER",
		Permission: &models.Permission{
			Operation: "DROP USER",
			UserName:  username,
		},
	}, nil
}

// ============================================================================
// HELPERS
// ============================================================================

// parseFieldValue converts a string value to an Expression
// Handles expressions like "field + 1" for increment operations
func parseFieldValue(value string) *models.Expression {
	// Skip binary expression detection for date/timestamp patterns
	if isDateOrTimestamp(value) {
		return LiteralExpr(value)
	}

	// Check for binary expressions (e.g., "count + 1")
	// Only match if there's a field name on left side (not starting with digit or -)
	if match := binaryExprRegex.FindStringSubmatch(value); len(match) == 4 {
		leftPart := strings.TrimSpace(match[1])
		// Don't match if left side looks like a date component (all digits)
		if !isNumericString(leftPart) {
			return BinaryExpr(
				FieldExpr(leftPart),
				strings.TrimSpace(match[2]),
				LiteralExpr(strings.TrimSpace(match[3])),
			)
		}
	}

	// Check for function expressions (e.g., "MIN(field, value)")
	if match := functionExprRegex.FindStringSubmatch(value); len(match) == 3 {
		funcName := strings.ToUpper(match[1])
		argsStr := match[2]
		args := strings.Split(argsStr, ",")

		var argExprs []*models.Expression
		for _, arg := range args {
			argExprs = append(argExprs, LiteralExpr(strings.TrimSpace(arg)))
		}

		return FunctionExpr(funcName, argExprs...)
	}

	return LiteralExpr(value)
}

// isDateOrTimestamp checks if value looks like a date or timestamp
func isDateOrTimestamp(value string) bool {
	// ISO date: 2024-01-15 or 1990-01-15
	if len(value) == 10 && value[4] == '-' && value[7] == '-' {
		// Check if parts are numeric
		if isNumericString(value[0:4]) && isNumericString(value[5:7]) && isNumericString(value[8:10]) {
			return true
		}
	}
	// ISO timestamp: 2024-01-15T10:30:00Z or with timezone
	if len(value) >= 19 && value[4] == '-' && value[7] == '-' && (value[10] == 'T' || value[10] == ' ') {
		return true
	}
	// Contains T and Z (timestamp indicator)
	if strings.Contains(value, "T") && (strings.Contains(value, "Z") || strings.Contains(value, "+")) {
		return true
	}
	return false
}

// isNumericString checks if string contains only digits
func isNumericString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// Regex patterns for expression parsing
var (
	binaryExprRegex   = regexp.MustCompile(`^(.+?)\s*([+\-*/])\s*(.+)$`)
	functionExprRegex = regexp.MustCompile(`^(\w+)\((.+)\)$`)
)

// parseJSONFields parses a simple JSON object into Fields
// Format: {"field1":"value1","field2":"value2"}
func parseJSONFields(jsonStr string) []models.Field {
	var fields []models.Field

	// Remove braces
	jsonStr = strings.TrimPrefix(jsonStr, "{")
	jsonStr = strings.TrimSuffix(jsonStr, "}")

	// Simple parsing (not full JSON parser)
	pairs := strings.Split(jsonStr, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.Trim(strings.TrimSpace(kv[0]), `"'`)
		value := strings.Trim(strings.TrimSpace(kv[1]), `"'`)

		fields = append(fields, models.Field{
			NameExpr:  FieldExpr(key),
			ValueExpr: LiteralExpr(value),
		})
	}

	return fields
}

// mapRedisPermissionToOQL maps Redis permissions to OQL permission names
func mapRedisPermissionToOQL(redisPerm string) string {
	switch strings.ToLower(redisPerm) {
	case "get", "hgetall", "lrange", "smembers", "zrange":
		return "READ"
	case "set", "hset", "hmset", "lpush", "sadd", "zadd":
		return "WRITE"
	case "del", "hdel", "lpop", "srem", "zrem":
		return "DELETE"
	case "@all":
		return "ALL"
	default:
		return strings.ToUpper(redisPerm)
	}
}

// mapOQLPermissionToRedis maps OQL permissions to Redis commands (for tests)
func mapOQLPermissionToRedis(oqlPerm string) []string {
	switch strings.ToUpper(oqlPerm) {
	case "READ", "GET", "SELECT":
		return []string{"+get", "+hgetall", "+lrange", "+smembers", "+zrange"}
	case "WRITE", "SET", "INSERT":
		return []string{"+set", "+hset", "+hmset", "+lpush", "+sadd", "+zadd"}
	case "DELETE", "DEL":
		return []string{"+del", "+hdel", "+lpop", "+srem", "+zrem"}
	case "ALL":
		return []string{"+@all"}
	default:
		return []string{"+" + strings.ToLower(oqlPerm)}
	}
}