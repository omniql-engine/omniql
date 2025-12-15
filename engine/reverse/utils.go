package reverse

import (
	"strings"
	"unicode"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// REVERSE MAPS - Built from SSOT at init()
// ============================================================================

var (
	SQLToOQLOperation map[string]map[string]string // dbType → sql_op → oql_op
	SQLToOQLOperator  map[string]map[string]string // dbType → sql_op → oql_op
	SQLToOQLType      map[string]map[string]string // dbType → sql_type → oql_type
)

func init() {
	buildReverseMaps()
}

func buildReverseMaps() {
	// Operations: select → GET, insert → CREATE, etc.
	SQLToOQLOperation = make(map[string]map[string]string)
	for dbType, ops := range mapping.OperationMap {
		SQLToOQLOperation[dbType] = make(map[string]string)
		for oqlOp, sqlOp := range ops {
			SQLToOQLOperation[dbType][strings.ToLower(sqlOp)] = oqlOp
		}
	}

	// Operators: = → =, $eq → =, etc.
	SQLToOQLOperator = make(map[string]map[string]string)
	for dbType, ops := range mapping.OperatorMap {
		SQLToOQLOperator[dbType] = make(map[string]string)
		for oqlOp, sqlOp := range ops {
			SQLToOQLOperator[dbType][sqlOp] = oqlOp
		}
	}

	// Types: SERIAL → AUTO, VARCHAR → STRING, etc.
	SQLToOQLType = make(map[string]map[string]string)
	for dbType, types := range mapping.TypeMap {
		SQLToOQLType[dbType] = make(map[string]string)
		for oqlType, sqlType := range types {
			SQLToOQLType[dbType][strings.ToUpper(sqlType)] = oqlType
		}
	}
}

// ============================================================================
// SSOT LOOKUPS
// ============================================================================

func GetOQLOperation(sqlOp, dbType string) string {
	if ops, ok := SQLToOQLOperation[dbType]; ok {
		if oql, found := ops[strings.ToLower(sqlOp)]; found {
			return oql
		}
	}
	return ""
}

func GetOQLOperator(sqlOp, dbType string) string {
	if ops, ok := SQLToOQLOperator[dbType]; ok {
		if oql, found := ops[sqlOp]; found {
			return oql
		}
	}
	return sqlOp // Return as-is if not mapped
}

func GetOQLType(sqlType, dbType string) string {
	if types, ok := SQLToOQLType[dbType]; ok {
		if oql, found := types[strings.ToUpper(sqlType)]; found {
			return oql
		}
	}
	return sqlType // Return as-is if not mapped
}

// ============================================================================
// TABLE ↔ ENTITY CONVERSION
// ============================================================================

// TableToEntity: users → User, order_items → OrderItem
func TableToEntity(table string) string {
	return toPascalCase(toSingular(strings.ToLower(table)))
}

// EntityToTable: User → users, OrderItem → order_items
func EntityToTable(entity string) string {
	return toPlural(toSnakeCase(entity))
}

// ============================================================================
// TRUEAST EXPRESSION BUILDERS
// ============================================================================

func FieldExpr(name string) *models.Expression {
	return &models.Expression{Type: "FIELD", Value: name}
}

func LiteralExpr(value string) *models.Expression {
	return &models.Expression{Type: "LITERAL", Value: value}
}

func BinaryExpr(left *models.Expression, op string, right *models.Expression) *models.Expression {
	return &models.Expression{
		Type:     "BINARY",
		Left:     left,
		Operator: op,
		Right:    right,
	}
}

func FunctionExpr(name string, args ...*models.Expression) *models.Expression {
	return &models.Expression{
		Type:         "FUNCTION",
		FunctionName: name,
		FunctionArgs: args,
	}
}

// ============================================================================
// STRING TRANSFORMATIONS
// ============================================================================

func toSingular(word string) string {
	irregulars := map[string]string{
		"people": "person", "children": "child", "men": "man", "women": "woman",
		"teeth": "tooth", "feet": "foot", "geese": "goose", "mice": "mouse",
	}
	if s, ok := irregulars[word]; ok {
		return s
	}
	if strings.HasSuffix(word, "ies") && len(word) > 3 {
		return word[:len(word)-3] + "y"
	}
	if strings.HasSuffix(word, "ves") && len(word) > 3 {
		return word[:len(word)-3] + "f"
	}
	if strings.HasSuffix(word, "oes") && len(word) > 3 {
		return word[:len(word)-2]
	}
	if strings.HasSuffix(word, "ses") || strings.HasSuffix(word, "xes") ||
		strings.HasSuffix(word, "ches") || strings.HasSuffix(word, "shes") {
		return word[:len(word)-2]
	}
	if strings.HasSuffix(word, "s") && len(word) > 1 {
		return word[:len(word)-1]
	}
	return word
}

func toPlural(word string) string {
	irregulars := map[string]string{
		"person": "people", "child": "children", "man": "men", "woman": "women",
		"tooth": "teeth", "foot": "feet", "goose": "geese", "mouse": "mice",
	}
	if p, ok := irregulars[word]; ok {
		return p
	}
	if strings.HasSuffix(word, "y") && len(word) > 1 && !isVowel(rune(word[len(word)-2])) {
		return word[:len(word)-1] + "ies"
	}
	if strings.HasSuffix(word, "f") {
		return word[:len(word)-1] + "ves"
	}
	if strings.HasSuffix(word, "fe") {
		return word[:len(word)-2] + "ves"
	}
	if strings.HasSuffix(word, "s") || strings.HasSuffix(word, "x") ||
		strings.HasSuffix(word, "ch") || strings.HasSuffix(word, "sh") {
		return word + "es"
	}
	return word + "s"
}

func toPascalCase(snake string) string {
	var b strings.Builder
	cap := true
	for _, r := range snake {
		if r == '_' {
			cap = true
			continue
		}
		if cap {
			b.WriteRune(unicode.ToUpper(r))
			cap = false
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func toSnakeCase(pascal string) string {
	var b strings.Builder
	for i, r := range pascal {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteRune('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isVowel(r rune) bool {
	return r == 'a' || r == 'e' || r == 'i' || r == 'o' || r == 'u'
}

// ============================================================================
// ADDITIONAL TRUEAST BUILDERS (for DQL/Advanced)
// ============================================================================

func CaseExpr(conditions []*models.CaseCondition, elseExpr *models.Expression) *models.Expression {
	return &models.Expression{
		Type:           "CASEWHEN",
		CaseConditions: conditions,
		CaseElse:       elseExpr,
	}
}

func WindowExpr(partitionBy []*models.Expression, orderBy []models.OrderBy, offset, buckets int) *models.Expression {
	return &models.Expression{
		Type:          "WINDOW",
		PartitionBy:   partitionBy,
		WindowOrderBy: orderBy,
		WindowOffset:  offset,
		WindowBuckets: buckets,
	}
}

func NewCondition(field *models.Expression, op string, value *models.Expression) models.Condition {
	return models.Condition{
		FieldExpr: field,
		Operator:  op,
		ValueExpr: value,
	}
}

func NewConditionWithLogic(field *models.Expression, op string, value *models.Expression, logic string) models.Condition {
	return models.Condition{
		FieldExpr: field,
		Operator:  op,
		ValueExpr: value,
		Logic:     logic,
	}
}