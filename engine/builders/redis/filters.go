package redis

import (
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/mapping"
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// MatchesConditions checks if a hash matches conditions with AND/OR logic
func MatchesConditions(hash map[string]string, conditions []*pb.QueryCondition) bool {
	if len(conditions) == 0 {
		return true
	}

	result := evaluateCondition(hash, conditions[0])

	for i := 1; i < len(conditions); i++ {
		cond := conditions[i]
		condResult := evaluateCondition(hash, cond)

		if cond.Logic == "OR" {
			result = result || condResult
		} else {
			result = result && condResult
		}
	}
	return result
}

// evaluateCondition evaluates a single condition against hash
func evaluateCondition(hash map[string]string, cond *pb.QueryCondition) bool {
	// Handle nested/grouped conditions
	if cond.Operator == "GROUP" && len(cond.Nested) > 0 {
		return MatchesConditions(hash, cond.Nested)
	}

	if cond.FieldExpr == nil {
		return true
	}

	field := cond.FieldExpr.Value
	actual, exists := hash[field]
	operator := strings.ToUpper(cond.Operator)
	category := mapping.GetOperatorCategory(operator)

	// Handle NULLCHECK (no value needed)
	if category == "NULLCHECK" {
		return matchNullCheck(exists, operator)
	}

	// Field doesn't exist
	if !exists {
		return operator == "!=" || operator == "<>" || operator == "NOT_IN"
	}

	// Route by category from SSOT
	switch category {
	case "COMPARISON":
		expected := ""
		if cond.ValueExpr != nil {
			expected = cond.ValueExpr.Value
		}
		return matchComparison(actual, operator, expected)
	case "MULTI_VALUE":
		return matchMultiValue(actual, operator, cond.ValuesExpr)
	case "RANGE":
		return matchRange(actual, operator, cond.ValueExpr, cond.Value2Expr)
	default:
		if cond.ValueExpr != nil {
			return actual == cond.ValueExpr.Value
		}
		return false
	}
}

// matchNullCheck handles IS_NULL / IS_NOT_NULL
func matchNullCheck(exists bool, operator string) bool {
	switch operator {
	case "IS_NULL", "IS NULL":
		return !exists
	case "IS_NOT_NULL", "IS NOT NULL":
		return exists
	}
	return false
}

// matchComparison handles =, !=, >, <, >=, <=, LIKE, ILIKE
func matchComparison(actual, operator, expected string) bool {
	switch operator {
	case "=", "==":
		return actual == expected
	case "!=", "<>":
		return actual != expected
	case ">":
		return compareNumeric(actual, expected) > 0
	case "<":
		return compareNumeric(actual, expected) < 0
	case ">=":
		return compareNumeric(actual, expected) >= 0
	case "<=":
		return compareNumeric(actual, expected) <= 0
	case "LIKE":
		return matchLike(actual, expected, true)
	case "NOT_LIKE", "NOT LIKE":
		return !matchLike(actual, expected, true)
	case "ILIKE":
		return matchLike(actual, expected, false)
	case "NOT_ILIKE", "NOT ILIKE":
		return !matchLike(actual, expected, false)
	}
	return actual == expected
}

// matchMultiValue handles IN / NOT_IN using ValuesExpr array
func matchMultiValue(actual, operator string, values []*pb.Expression) bool {
	found := false
	for _, v := range values {
		if v != nil && actual == v.Value {
			found = true
			break
		}
	}

	switch operator {
	case "IN":
		return found
	case "NOT_IN", "NOT IN":
		return !found
	}
	return false
}

// matchRange handles BETWEEN / NOT_BETWEEN using ValueExpr and Value2Expr
func matchRange(actual, operator string, val1, val2 *pb.Expression) bool {
	if val1 == nil || val2 == nil {
		return false
	}

	min := val1.Value
	max := val2.Value
	inRange := compareNumeric(actual, min) >= 0 && compareNumeric(actual, max) <= 0

	switch operator {
	case "BETWEEN":
		return inRange
	case "NOT_BETWEEN", "NOT BETWEEN":
		return !inRange
	}
	return false
}

// matchLike performs LIKE pattern matching
func matchLike(actual, pattern string, caseSensitive bool) bool {
	if !caseSensitive {
		actual = strings.ToLower(actual)
		pattern = strings.ToLower(pattern)
	}

	if pattern == "%" {
		return true
	}
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(actual, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(actual, pattern[1:])
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(actual, pattern[:len(pattern)-1])
	}
	return actual == pattern
}

// compareNumeric compares two values numerically, falls back to string
func compareNumeric(a, b string) int {
	aNum, err1 := strconv.ParseFloat(a, 64)
	bNum, err2 := strconv.ParseFloat(b, 64)

	if err1 != nil || err2 != nil {
		return strings.Compare(a, b)
	}
	if aNum < bNum {
		return -1
	}
	if aNum > bNum {
		return 1
	}
	return 0
}