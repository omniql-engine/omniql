package reverse

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// REDIS ADVANCED REVERSE TRANSLATION
// Covers: UPSERT, REPLACE, List/Set/SortedSet operations, Complex expressions,
//         Extended key patterns, Tenant isolation
// ============================================================================

// ============================================================================
// UPSERT DETECTION (HMSET with upsert semantics)
// ============================================================================

// ConvertRedisToUpsert converts HMSET to UPSERT when detected
// Redis doesn't distinguish CREATE vs UPSERT - HMSET always upserts
// We detect UPSERT context from key pattern or explicit marker
func ConvertRedisToUpsert(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: HMSET requires key and field-value pairs", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPSERT",
		Entity:    TableToEntity(keyInfo.Entity),
		Upsert:    &models.Upsert{},
	}

	// Add ID as conflict field
	if keyInfo.ID != "" {
		query.Upsert.ConflictFields = []*models.Expression{FieldExpr("id")}
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("id"),
			ValueExpr: LiteralExpr(keyInfo.ID),
		})
	}

	// Parse field-value pairs
	for i := 2; i < len(parts)-1; i += 2 {
		fieldName := parts[i]
		fieldValue := parts[i+1]

		if fieldName == "_placeholder" {
			continue
		}

		field := models.Field{
			NameExpr:  FieldExpr(fieldName),
			ValueExpr: parseFieldValue(fieldValue),
		}

		query.Fields = append(query.Fields, field)
		query.Upsert.UpdateFields = append(query.Upsert.UpdateFields, field)
	}

	return query, nil
}

// ============================================================================
// REPLACE DETECTION (HMSET with replace semantics)
// ============================================================================

// ConvertRedisToReplace converts HMSET to REPLACE
// REPLACE = DELETE + INSERT (full document replacement)
func ConvertRedisToReplace(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: HMSET requires key and field-value pairs", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "REPLACE",
		Entity:    TableToEntity(keyInfo.Entity),
	}

	// Add condition for existing record
	if keyInfo.ID != "" {
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		}

		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("id"),
			ValueExpr: LiteralExpr(keyInfo.ID),
		})
	}

	// Parse field-value pairs
	for i := 2; i < len(parts)-1; i += 2 {
		fieldName := parts[i]
		fieldValue := parts[i+1]

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
// LIST OPERATIONS (LPUSH, LPOP, LRANGE, LLEN)
// ============================================================================

// ConvertRedisLPush converts LPUSH to array append operation
func ConvertRedisLPush(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: LPUSH requires key and value", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	// Values to push
	for i := 2; i < len(parts); i++ {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("_list"),
			ValueExpr: FunctionExpr("ARRAY_PREPEND", FieldExpr("_list"), LiteralExpr(parts[i])),
		})
	}

	return query, nil
}

// ConvertRedisLRange converts LRANGE to GET with array slice
func ConvertRedisLRange(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: LRANGE requires key, start, stop", ErrParseError)
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

	// Parse start and stop for LIMIT/OFFSET
	start, _ := strconv.Atoi(parts[2])
	stop, _ := strconv.Atoi(parts[3])

	if start > 0 {
		query.Offset = start
	}
	if stop >= 0 && stop >= start {
		query.Limit = stop - start + 1
	}

	return query, nil
}

// ============================================================================
// SET OPERATIONS (SADD, SREM, SMEMBERS, SCARD)
// ============================================================================

// ConvertRedisSAdd converts SADD to set add operation
func ConvertRedisSAdd(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: SADD requires key and member(s)", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	// Members to add
	for i := 2; i < len(parts); i++ {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("_set"),
			ValueExpr: FunctionExpr("ARRAY_ADD_UNIQUE", FieldExpr("_set"), LiteralExpr(parts[i])),
		})
	}

	return query, nil
}

// ConvertRedisSMembers converts SMEMBERS to GET set members
func ConvertRedisSMembers(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: SMEMBERS requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Distinct:  true, // Sets have unique members
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
// SORTED SET OPERATIONS (ZADD, ZREM, ZRANGE, ZSCORE)
// ============================================================================

// ConvertRedisZAdd converts ZADD to sorted set add
func ConvertRedisZAdd(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: ZADD requires key, score, member", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	// Parse score-member pairs
	for i := 2; i < len(parts)-1; i += 2 {
		score := parts[i]
		member := parts[i+1]

		query.Fields = append(query.Fields, models.Field{
			NameExpr: FieldExpr(member),
			ValueExpr: LiteralExpr(score),
		})
	}

	return query, nil
}

// ConvertRedisZRange converts ZRANGE to GET with ORDER BY
func ConvertRedisZRange(parts []string) (*models.Query, error) {
	if len(parts) < 4 {
		return nil, fmt.Errorf("%w: ZRANGE requires key, start, stop", ErrParseError)
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

	// Default sort by score ASC
	query.OrderBy = []models.OrderBy{
		{
			FieldExpr: FieldExpr("_score"),
			Direction: models.Asc,
		},
	}

	// Check for WITHSCORES, REV, BYSCORE, BYLEX options
	for i := 4; i < len(parts); i++ {
		switch strings.ToUpper(parts[i]) {
		case "REV":
			query.OrderBy[0].Direction = models.Desc
		case "LIMIT":
			if i+2 < len(parts) {
				offset, _ := strconv.Atoi(parts[i+1])
				count, _ := strconv.Atoi(parts[i+2])
				query.Offset = offset
				query.Limit = count
				i += 2
			}
		}
	}

	// Parse start and stop
	start, _ := strconv.Atoi(parts[2])
	stop, _ := strconv.Atoi(parts[3])

	if query.Offset == 0 && start > 0 {
		query.Offset = start
	}
	if query.Limit == 0 && stop >= 0 && stop >= start {
		query.Limit = stop - start + 1
	}

	return query, nil
}

// ============================================================================
// HASH FIELD OPERATIONS (HGET, HDEL, HEXISTS, HKEYS, HVALS)
// ============================================================================

// ConvertRedisHGet converts HGET to GET specific field
func ConvertRedisHGet(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: HGET requires key and field", ErrParseError)
	}

	key := parts[1]
	field := parts[2]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Columns:   []*models.Expression{FieldExpr(field)},
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

// ConvertRedisHDel converts HDEL to UPDATE with NULL
func ConvertRedisHDel(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: HDEL requires key and field(s)", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	// Set fields to NULL
	for i := 2; i < len(parts); i++ {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr(parts[i]),
			ValueExpr: LiteralExpr("NULL"),
		})
	}

	return query, nil
}

// ============================================================================
// KEY OPERATIONS (EXISTS, TTL, EXPIRE, KEYS, SCAN)
// ============================================================================

// ConvertRedisExists converts EXISTS to COUNT with condition
func ConvertRedisExists(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: EXISTS requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "COUNT",
		Entity:    TableToEntity(keyInfo.Entity),
		Aggregate: &models.Aggregation{
			Function:  models.Count,
			FieldExpr: FieldExpr("*"),
		},
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

// ConvertRedisKeys converts KEYS pattern to GET with LIKE
func ConvertRedisKeys(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: KEYS requires pattern", ErrParseError)
	}

	pattern := parts[1]
	keyInfo := parseRedisKey(pattern)

	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Columns:   []*models.Expression{FieldExpr("id")}, // Just return keys/IDs
	}

	// If pattern has wildcard in ID position, add LIKE condition
	if strings.Contains(keyInfo.ID, "*") && keyInfo.ID != "*" {
		likePattern := strings.ReplaceAll(keyInfo.ID, "*", "%")
		query.Conditions = []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "LIKE",
				ValueExpr: LiteralExpr(likePattern),
			},
		}
	}

	return query, nil
}

// ============================================================================
// COMPLEX EXPRESSION PARSING
// ============================================================================

// ParseComplexRedisValue parses complex values including expressions
func ParseComplexRedisValue(value string) *models.Expression {
	// Check for increment pattern: "INCR field" or "field + 1"
	if incrMatch := incrRegex.FindStringSubmatch(value); len(incrMatch) > 0 {
		return BinaryExpr(
			FieldExpr(incrMatch[1]),
			"+",
			LiteralExpr(incrMatch[2]),
		)
	}

	// Check for decrement pattern: "DECR field" or "field - 1"
	if decrMatch := decrRegex.FindStringSubmatch(value); len(decrMatch) > 0 {
		return BinaryExpr(
			FieldExpr(decrMatch[1]),
			"-",
			LiteralExpr(decrMatch[2]),
		)
	}

	// Check for multiplication: "field * 2"
	if mulMatch := mulRegex.FindStringSubmatch(value); len(mulMatch) > 0 {
		return BinaryExpr(
			FieldExpr(mulMatch[1]),
			"*",
			LiteralExpr(mulMatch[2]),
		)
	}

	// Check for division: "field / 2"
	if divMatch := divRegex.FindStringSubmatch(value); len(divMatch) > 0 {
		return BinaryExpr(
			FieldExpr(divMatch[1]),
			"/",
			LiteralExpr(divMatch[2]),
		)
	}

	// Check for MIN function: "MIN(field, value)"
	if minMatch := minFuncRegex.FindStringSubmatch(value); len(minMatch) > 0 {
		return FunctionExpr("MIN", FieldExpr(minMatch[1]), LiteralExpr(minMatch[2]))
	}

	// Check for MAX function: "MAX(field, value)"
	if maxMatch := maxFuncRegex.FindStringSubmatch(value); len(maxMatch) > 0 {
		return FunctionExpr("MAX", FieldExpr(maxMatch[1]), LiteralExpr(maxMatch[2]))
	}

	// Check for array operations
	if strings.HasPrefix(value, "ARRAY_APPEND(") {
		return parseArrayFunction(value, "ARRAY_APPEND")
	}
	if strings.HasPrefix(value, "ARRAY_REMOVE(") {
		return parseArrayFunction(value, "ARRAY_REMOVE")
	}
	if strings.HasPrefix(value, "ARRAY_ADD_UNIQUE(") {
		return parseArrayFunction(value, "ARRAY_ADD_UNIQUE")
	}

	// Default: literal value
	return LiteralExpr(value)
}

// Regex patterns for expression parsing
var (
	incrRegex     = regexp.MustCompile(`^(\w+)\s*\+\s*(\d+)$`)
	decrRegex     = regexp.MustCompile(`^(\w+)\s*-\s*(\d+)$`)
	mulRegex      = regexp.MustCompile(`^(\w+)\s*\*\s*(\d+(?:\.\d+)?)$`)
	divRegex      = regexp.MustCompile(`^(\w+)\s*/\s*(\d+(?:\.\d+)?)$`)
	minFuncRegex  = regexp.MustCompile(`^MIN\((\w+),\s*(.+)\)$`)
	maxFuncRegex  = regexp.MustCompile(`^MAX\((\w+),\s*(.+)\)$`)
)

func parseArrayFunction(value string, funcName string) *models.Expression {
	// Extract arguments from function call
	start := strings.Index(value, "(")
	end := strings.LastIndex(value, ")")
	if start == -1 || end == -1 {
		return LiteralExpr(value)
	}

	argsStr := value[start+1 : end]
	args := strings.SplitN(argsStr, ",", 2)
	if len(args) != 2 {
		return LiteralExpr(value)
	}

	return FunctionExpr(funcName,
		FieldExpr(strings.TrimSpace(args[0])),
		LiteralExpr(strings.TrimSpace(args[1])),
	)
}

// ============================================================================
// EXTENDED KEY PATTERN PARSING
// ============================================================================

// RedisKeyPattern represents advanced key pattern info
type RedisKeyPattern struct {
	TenantID    string
	Entity      string
	ID          string
	Segments    []string // All key segments
	IsPattern   bool
	PatternType string // "exact", "prefix", "suffix", "contains"
}

// ParseRedisKeyPattern parses key with advanced pattern detection
func ParseRedisKeyPattern(key string) *RedisKeyPattern {
	pattern := &RedisKeyPattern{
		Segments: strings.Split(key, ":"),
	}

	// Detect pattern type
	if strings.Contains(key, "*") {
		pattern.IsPattern = true
		if strings.HasSuffix(key, "*") && strings.Count(key, "*") == 1 {
			pattern.PatternType = "prefix"
		} else if strings.HasPrefix(key, "*") && strings.Count(key, "*") == 1 {
			pattern.PatternType = "suffix"
		} else {
			pattern.PatternType = "contains"
		}
	} else {
		pattern.PatternType = "exact"
	}

	// Extract components
	if len(pattern.Segments) >= 2 && pattern.Segments[0] == "tenant" {
		pattern.TenantID = pattern.Segments[1]
	}
	if len(pattern.Segments) >= 3 {
		pattern.Entity = pattern.Segments[2]
	}
	if len(pattern.Segments) >= 4 {
		pattern.ID = pattern.Segments[3]
	}

	return pattern
}

// ToConditions converts key pattern to OQL conditions
func (p *RedisKeyPattern) ToConditions() []models.Condition {
	var conditions []models.Condition

	if p.ID != "" && p.ID != "*" {
		switch p.PatternType {
		case "exact":
			conditions = append(conditions, models.Condition{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(p.ID),
			})
		case "prefix":
			likePattern := strings.TrimSuffix(p.ID, "*") + "%"
			conditions = append(conditions, models.Condition{
				FieldExpr: FieldExpr("id"),
				Operator:  "LIKE",
				ValueExpr: LiteralExpr(likePattern),
			})
		case "suffix":
			likePattern := "%" + strings.TrimPrefix(p.ID, "*")
			conditions = append(conditions, models.Condition{
				FieldExpr: FieldExpr("id"),
				Operator:  "LIKE",
				ValueExpr: LiteralExpr(likePattern),
			})
		case "contains":
			likePattern := strings.ReplaceAll(p.ID, "*", "%")
			conditions = append(conditions, models.Condition{
				FieldExpr: FieldExpr("id"),
				Operator:  "LIKE",
				ValueExpr: LiteralExpr(likePattern),
			})
		}
	}

	return conditions
}

// ============================================================================
// JSON HELPERS (ADVANCED)
// ============================================================================

// ParseJSONToFields converts JSON string to Fields with type inference
func ParseJSONToFields(jsonStr string) []models.Field {
	var fields []models.Field

	// Remove outer braces
	jsonStr = strings.TrimSpace(jsonStr)
	jsonStr = strings.TrimPrefix(jsonStr, "{")
	jsonStr = strings.TrimSuffix(jsonStr, "}")

	if jsonStr == "" {
		return fields
	}

	// Parse key-value pairs (simple parser - doesn't handle nested objects)
	pairs := splitJSONPairs(jsonStr)

	for _, pair := range pairs {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.Trim(strings.TrimSpace(kv[0]), `"'`)
		value := strings.TrimSpace(kv[1])

		// Type inference
		valueExpr := inferJSONValueType(value)

		fields = append(fields, models.Field{
			NameExpr:  FieldExpr(key),
			ValueExpr: valueExpr,
		})
	}

	// Sort fields for deterministic order
	sort.Slice(fields, func(i, j int) bool {
		return getExpressionValue(fields[i].NameExpr) < getExpressionValue(fields[j].NameExpr)
	})

	return fields
}

// splitJSONPairs splits JSON object into key-value pairs
// Handles nested objects and arrays properly
func splitJSONPairs(jsonStr string) []string {
	var pairs []string
	var current strings.Builder
	depth := 0
	inString := false

	for _, r := range jsonStr {
		switch r {
		case '"':
			if depth == 0 {
				inString = !inString
			}
			current.WriteRune(r)
		case '{', '[':
			depth++
			current.WriteRune(r)
		case '}', ']':
			depth--
			current.WriteRune(r)
		case ',':
			if depth == 0 && !inString {
				if current.Len() > 0 {
					pairs = append(pairs, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		pairs = append(pairs, current.String())
	}

	return pairs
}

// inferJSONValueType infers OQL type from JSON value
func inferJSONValueType(value string) *models.Expression {
	value = strings.TrimSpace(value)

	// String
	if (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
		(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) {
		return LiteralExpr(strings.Trim(value, `"'`))
	}

	// Boolean
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" {
		return LiteralExpr(lower)
	}

	// Null
	if lower == "null" {
		return LiteralExpr("NULL")
	}

	// Number (int or float)
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return LiteralExpr(value)
	}

	// Array (simplified - just return as string)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		return LiteralExpr(value)
	}

	// Nested object (simplified - just return as string)
	if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
		return LiteralExpr(value)
	}

	// Default: treat as string
	return LiteralExpr(value)
}

func getExpressionValue(expr *models.Expression) string {
	if expr == nil {
		return ""
	}
	return expr.Value
}

// ============================================================================
// TENANT ISOLATION HELPERS
// ============================================================================

// ExtractTenantFromKey extracts tenant ID from Redis key
func ExtractTenantFromKey(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) >= 2 && parts[0] == "tenant" {
		return parts[1]
	}
	return ""
}

// ExtractTenantFromACL extracts tenant pattern from ACL args
// Looks for ~tenant:XXX:* pattern
func ExtractTenantFromACL(args []string) string {
	for _, arg := range args {
		if strings.HasPrefix(arg, "~tenant:") {
			parts := strings.Split(strings.TrimPrefix(arg, "~"), ":")
			if len(parts) >= 2 {
				return parts[1]
			}
		}
	}
	return ""
}

// ============================================================================
// EXTENDED OPERATIONS ROUTER
// ============================================================================

// ConvertExtendedRedisCommand handles additional Redis commands
// Called from main router when basic commands don't match
func ConvertExtendedRedisCommand(parts []string) (*models.Query, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: empty command", ErrParseError)
	}

	command := strings.ToUpper(parts[0])

	switch command {
	// List operations
	case "LPUSH":
		return ConvertRedisLPush(parts)
	case "LRANGE":
		return ConvertRedisLRange(parts)
	case "LPOP":
		return convertRedisLPop(parts)
	case "LLEN":
		return convertRedisLLen(parts)

	// Set operations
	case "SADD":
		return ConvertRedisSAdd(parts)
	case "SMEMBERS":
		return ConvertRedisSMembers(parts)
	case "SREM":
		return convertRedisSRem(parts)
	case "SCARD":
		return convertRedisSCard(parts)

	// Sorted set operations
	case "ZADD":
		return ConvertRedisZAdd(parts)
	case "ZRANGE":
		return ConvertRedisZRange(parts)
	case "ZREM":
		return convertRedisZRem(parts)
	case "ZSCORE":
		return convertRedisZScore(parts)

	// Hash operations
	case "HGET":
		return ConvertRedisHGet(parts)
	case "HDEL":
		return ConvertRedisHDel(parts)
	case "HKEYS":
		return convertRedisHKeys(parts)
	case "HVALS":
		return convertRedisHVals(parts)

	// Key operations
	case "EXISTS":
		return ConvertRedisExists(parts)
	case "KEYS":
		return ConvertRedisKeys(parts)
	case "TTL":
		return convertRedisTTL(parts)
	case "EXPIRE":
		return convertRedisExpire(parts)

	default:
		return nil, fmt.Errorf("%w: unsupported Redis command: %s", ErrNotSupported, command)
	}
}

// ============================================================================
// ADDITIONAL COMMAND CONVERTERS
// ============================================================================

func convertRedisLPop(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: LPOP requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	return &models.Query{
		Operation: "UPDATE",
		Entity:    TableToEntity(keyInfo.Entity),
		Fields: []models.Field{
			{
				NameExpr:  FieldExpr("_list"),
				ValueExpr: FunctionExpr("ARRAY_POP", FieldExpr("_list"), LiteralExpr("FIRST")),
			},
		},
	}, nil
}

func convertRedisLLen(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: LLEN requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	return &models.Query{
		Operation: "COUNT",
		Entity:    TableToEntity(keyInfo.Entity),
		Aggregate: &models.Aggregation{
			Function:  models.Count,
			FieldExpr: FieldExpr("_list"),
		},
	}, nil
}

func convertRedisSRem(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: SREM requires key and member(s)", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	for i := 2; i < len(parts); i++ {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr("_set"),
			ValueExpr: FunctionExpr("ARRAY_REMOVE", FieldExpr("_set"), LiteralExpr(parts[i])),
		})
	}

	return query, nil
}

func convertRedisSCard(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: SCARD requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	return &models.Query{
		Operation: "COUNT",
		Entity:    TableToEntity(keyInfo.Entity),
		Aggregate: &models.Aggregation{
			Function:  models.Count,
			FieldExpr: FieldExpr("_set"),
		},
		Distinct: true,
	}, nil
}

func convertRedisZRem(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: ZREM requires key and member(s)", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	query := &models.Query{
		Operation: "UPDATE",
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

	// Set members to NULL to remove
	for i := 2; i < len(parts); i++ {
		query.Fields = append(query.Fields, models.Field{
			NameExpr:  FieldExpr(parts[i]),
			ValueExpr: LiteralExpr("NULL"),
		})
	}

	return query, nil
}

func convertRedisZScore(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: ZSCORE requires key and member", ErrParseError)
	}

	key := parts[1]
	member := parts[2]
	keyInfo := parseRedisKey(key)

	return &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Columns:   []*models.Expression{FieldExpr(member)},
		Conditions: []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		},
	}, nil
}

func convertRedisHKeys(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: HKEYS requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	// Return field names (column names)
	return &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Conditions: []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		},
	}, nil
}

func convertRedisHVals(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: HVALS requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	// Return field values
	return &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Conditions: []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		},
	}, nil
}

func convertRedisTTL(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: TTL requires key", ErrParseError)
	}

	key := parts[1]
	keyInfo := parseRedisKey(key)

	// TTL returns expiration time - map to GET with _ttl column
	return &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(keyInfo.Entity),
		Columns:   []*models.Expression{FieldExpr("_ttl")},
		Conditions: []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		},
	}, nil
}

func convertRedisExpire(parts []string) (*models.Query, error) {
	if len(parts) < 3 {
		return nil, fmt.Errorf("%w: EXPIRE requires key and seconds", ErrParseError)
	}

	key := parts[1]
	seconds := parts[2]
	keyInfo := parseRedisKey(key)

	// EXPIRE sets TTL - map to UPDATE with _ttl field
	return &models.Query{
		Operation: "UPDATE",
		Entity:    TableToEntity(keyInfo.Entity),
		Fields: []models.Field{
			{
				NameExpr:  FieldExpr("_ttl"),
				ValueExpr: LiteralExpr(seconds),
			},
		},
		Conditions: []models.Condition{
			{
				FieldExpr: FieldExpr("id"),
				Operator:  "=",
				ValueExpr: LiteralExpr(keyInfo.ID),
			},
		},
	}, nil
}