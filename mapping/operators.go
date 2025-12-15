package mapping

import "strings"

// OperatorMap - Runtime mapping for translators
// Usage: OperatorMap["MongoDB"]["="] returns "$eq"
var OperatorMap = map[string]map[string]string{
	"PostgreSQL": {
		// Basic comparison operators
		"=":  "=",
		"!=": "!=",
		">":  ">",
		"<":  "<",
		">=": ">=",
		"<=": "<=",
		
		// Advanced operators
		"IN":          "IN",
		"NOT_IN":      "NOT IN",
		"BETWEEN":     "BETWEEN",
		"NOT_BETWEEN": "NOT BETWEEN",
		"LIKE":        "LIKE",
		"NOT_LIKE":    "NOT LIKE",
		"ILIKE":       "ILIKE",        // Case-insensitive LIKE (PostgreSQL specific)
		"NOT_ILIKE":   "NOT ILIKE",
		"IS_NULL":     "IS NULL",
		"IS_NOT_NULL": "IS NOT NULL",
		
		// Logical operators
		"AND": "AND",
		"OR":  "OR",
		"NOT": "NOT",
	},
	"MySQL": {
		// Basic comparison operators
		"=":  "=",
		"!=": "!=",
		">":  ">",
		"<":  "<",
		">=": ">=",
		"<=": "<=",
		
		// Advanced operators
		"IN":          "IN",
		"NOT_IN":      "NOT IN",
		"BETWEEN":     "BETWEEN",
		"NOT_BETWEEN": "NOT BETWEEN",
		"LIKE":        "LIKE",
		"NOT_LIKE":    "NOT LIKE",
		"ILIKE":       "LIKE",  // MySQL doesn't have ILIKE, use LIKE with LOWER()
		"NOT_ILIKE":   "NOT LIKE",
		"IS_NULL":     "IS NULL",
		"IS_NOT_NULL": "IS NOT NULL",
		
		// Logical operators
		"AND": "AND",
		"OR":  "OR",
		"NOT": "NOT",
	},
	"SQLite": {
		// Basic comparison operators
		"=":  "=",
		"!=": "!=",
		">":  ">",
		"<":  "<",
		">=": ">=",
		"<=": "<=",
		
		// Advanced operators
		"IN":          "IN",
		"NOT_IN":      "NOT IN",
		"BETWEEN":     "BETWEEN",
		"NOT_BETWEEN": "NOT BETWEEN",
		"LIKE":        "LIKE",
		"NOT_LIKE":    "NOT LIKE",
		"ILIKE":       "LIKE",  // SQLite LIKE is case-insensitive by default
		"NOT_ILIKE":   "NOT LIKE",
		"IS_NULL":     "IS NULL",
		"IS_NOT_NULL": "IS NOT NULL",
		
		// Logical operators
		"AND": "AND",
		"OR":  "OR",
		"NOT": "NOT",
	},
	"MongoDB": {
		// Basic comparison operators
		"=":  "$eq",
		"!=": "$ne",
		">":  "$gt",
		"<":  "$lt",
		">=": "$gte",
		"<=": "$lte",
		
		// Advanced operators
		"IN":          "$in",
		"NOT_IN":      "$nin",
		"BETWEEN":     "$gte/$lte",  // Requires two conditions
		"NOT_BETWEEN": "$lt/$gt",    // Requires two conditions
		"LIKE":        "$regex",
		"NOT_LIKE":    "$not/$regex",
		"ILIKE":       "$regex",     // Use regex with 'i' flag
		"NOT_ILIKE":   "$not/$regex",
		"IS_NULL":     "null",
		"IS_NOT_NULL": "$ne:null",
		
		// Logical operators
		"AND": "implicit",  // MongoDB uses implicit AND in queries
		"OR":  "$or",
		"NOT": "$not",
	},
}

// OperatorDefinition defines operators for documentation
type OperatorDefinition struct {
	// Basic comparison
	Equals         string // =
	NotEquals      string // !=
	GreaterThan    string // >
	LessThan       string // <
	GreaterOrEqual string // >=
	LessOrEqual    string // <=
	
	// Advanced comparison
	In          string // IN
	NotIn       string // NOT IN
	Between     string // BETWEEN
	NotBetween  string // NOT BETWEEN
	Like        string // LIKE
	NotLike     string // NOT LIKE
	ILike       string // ILIKE (case-insensitive)
	NotILike    string // NOT ILIKE
	IsNull      string // IS NULL
	IsNotNull   string // IS NOT NULL
	
	// Logical operators
	And string // AND
	Or  string // OR
	Not string // NOT
}

// OperatorDocs - Documentation of operator syntax per database
var OperatorDocs = map[string]OperatorDefinition{
	"PostgreSQL": {
		// Basic comparison
		Equals:         "=",
		NotEquals:      "!=",
		GreaterThan:    ">",
		LessThan:       "<",
		GreaterOrEqual: ">=",
		LessOrEqual:    "<=",
		
		// Advanced comparison
		In:         "IN",
		NotIn:      "NOT IN",
		Between:    "BETWEEN",
		NotBetween: "NOT BETWEEN",
		Like:       "LIKE",
		NotLike:    "NOT LIKE",
		ILike:      "ILIKE",
		NotILike:   "NOT ILIKE",
		IsNull:     "IS NULL",
		IsNotNull:  "IS NOT NULL",
		
		// Logical operators
		And: "AND",
		Or:  "OR",
		Not: "NOT",
	},
	"MySQL": {
		// Basic comparison
		Equals:         "=",
		NotEquals:      "!=",
		GreaterThan:    ">",
		LessThan:       "<",
		GreaterOrEqual: ">=",
		LessOrEqual:    "<=",
		
		// Advanced comparison
		In:         "IN",
		NotIn:      "NOT IN",
		Between:    "BETWEEN",
		NotBetween: "NOT BETWEEN",
		Like:       "LIKE",
		NotLike:    "NOT LIKE",
		ILike:      "LIKE (use with LOWER())",
		NotILike:   "NOT LIKE (use with LOWER())",
		IsNull:     "IS NULL",
		IsNotNull:  "IS NOT NULL",
		
		// Logical operators
		And: "AND",
		Or:  "OR",
		Not: "NOT",
	},
	"SQLite": {
		// Basic comparison
		Equals:         "=",
		NotEquals:      "!=",
		GreaterThan:    ">",
		LessThan:       "<",
		GreaterOrEqual: ">=",
		LessOrEqual:    "<=",
		
		// Advanced comparison
		In:         "IN",
		NotIn:      "NOT IN",
		Between:    "BETWEEN",
		NotBetween: "NOT BETWEEN",
		Like:       "LIKE",
		NotLike:    "NOT LIKE",
		ILike:      "LIKE (case-insensitive by default)",
		NotILike:   "NOT LIKE",
		IsNull:     "IS NULL",
		IsNotNull:  "IS NOT NULL",
		
		// Logical operators
		And: "AND",
		Or:  "OR",
		Not: "NOT",
	},
	"MongoDB": {
		// Basic comparison
		Equals:         "$eq",
		NotEquals:      "$ne",
		GreaterThan:    "$gt",
		LessThan:       "$lt",
		GreaterOrEqual: "$gte",
		LessOrEqual:    "$lte",
		
		// Advanced comparison
		In:         "$in",
		NotIn:      "$nin",
		Between:    "$gte + $lte",
		NotBetween: "$lt + $gt",
		Like:       "$regex",
		NotLike:    "$not + $regex",
		ILike:      "$regex with 'i' flag",
		NotILike:   "$not + $regex with 'i' flag",
		IsNull:     "null",
		IsNotNull:  "$ne: null",
		
		// Logical operators
		And: "implicit (array of conditions)",
		Or:  "$or",
		Not: "$not",
	},
}

// OperatorExamples - Usage examples for each operator
var OperatorExamples = map[string]map[string]string{
	"PostgreSQL": {
		"=":           "age = 25",
		"!=":          "status != 'inactive'",
		">":           "price > 100",
		"IN":          "status IN ('active', 'pending')",
		"BETWEEN":     "age BETWEEN 18 AND 65",
		"LIKE":        "name LIKE 'John%'",
		"ILIKE":       "email ILIKE '%@gmail.com'",
		"IS_NULL":     "deleted_at IS NULL",
		"IS_NOT_NULL": "updated_at IS NOT NULL",
	},
	"MySQL": {
		"=":           "age = 25",
		"!=":          "status != 'inactive'",
		">":           "price > 100",
		"IN":          "status IN ('active', 'pending')",
		"BETWEEN":     "age BETWEEN 18 AND 65",
		"LIKE":        "name LIKE 'John%'",
		"ILIKE":       "LOWER(email) LIKE LOWER('%@gmail.com')",
		"IS_NULL":     "deleted_at IS NULL",
		"IS_NOT_NULL": "updated_at IS NOT NULL",
	},
	"SQLite": {
		"=":           "age = 25",
		"!=":          "status != 'inactive'",
		">":           "price > 100",
		"IN":          "status IN ('active', 'pending')",
		"BETWEEN":     "age BETWEEN 18 AND 65",
		"LIKE":        "name LIKE 'John%'",
		"ILIKE":       "name LIKE 'john%'  -- case-insensitive by default",
		"IS_NULL":     "deleted_at IS NULL",
		"IS_NOT_NULL": "updated_at IS NOT NULL",
	},
	"MongoDB": {
		"$eq":  "{age: {$eq: 25}}",
		"$ne":  "{status: {$ne: 'inactive'}}",
		"$gt":  "{price: {$gt: 100}}",
		"$in":  "{status: {$in: ['active', 'pending']}}",
		"BETWEEN": "{age: {$gte: 18, $lte: 65}}",
		"$regex":  "{name: {$regex: /^John/}}",
		"$regex_i": "{email: {$regex: /@gmail.com$/i}}",
		"IS_NULL": "{deleted_at: null}",
		"IS_NOT_NULL": "{updated_at: {$ne: null}}",
	},
}

// ArithmeticOperators - operators for expressions
var ArithmeticOperators = map[string]bool{
    "+": true,
    "-": true,
    "*": true,
    "/": true,
    "%": true,
}

// OperatorCategories - SSOT for operator types
var OperatorCategories = map[string]string{
	// Multi-value operators (IN)
	"IN":          "MULTI_VALUE",
	"NOT_IN":      "MULTI_VALUE",
	
	// Range operators (BETWEEN)
	"BETWEEN":     "RANGE",
	"NOT_BETWEEN": "RANGE",
	
	// Null check operators (no value needed)
	"IS_NULL":     "NULLCHECK",
	"IS_NOT_NULL": "NULLCHECK",
	
	// Standard comparison (single value)
	"=":           "COMPARISON",
	"!=":          "COMPARISON",
	">":           "COMPARISON",
	"<":           "COMPARISON",
	">=":          "COMPARISON",
	"<=":          "COMPARISON",
	"LIKE":        "COMPARISON",
	"NOT_LIKE":    "COMPARISON",
	"ILIKE":       "COMPARISON",
	"NOT_ILIKE":   "COMPARISON",
}

// WindowFunctions - SSOT for window function names
var WindowFunctions = map[string]bool{
	"ROW NUMBER": true,
	"RANK":       true,
	"DENSE RANK": true,
	"LAG":        true,
	"LEAD":       true,
	"NTILE":      true,
}

// WindowFunctionPrefixes - for two-word detection
var WindowFunctionPrefixes = map[string]string{
	"ROW":   "NUMBER",
	"DENSE": "RANK",
}

// IsWindowFunction checks if name is window function
func IsWindowFunction(name string) bool {
	return WindowFunctions[strings.ToUpper(name)]
}

// GetWindowFunctionSuffix returns second word if prefix matches
func GetWindowFunctionSuffix(prefix string) (string, bool) {
	suffix, ok := WindowFunctionPrefixes[strings.ToUpper(prefix)]
	return suffix, ok
}

// WindowFunctionHasField checks if function takes a field argument (LAG, LEAD)
func WindowFunctionHasField(name string) bool {
	upper := strings.ToUpper(name)
	return upper == "LAG" || upper == "LEAD"
}

// WindowFunctionHasBuckets checks if function takes bucket count (NTILE)
func WindowFunctionHasBuckets(name string) bool {
	return strings.ToUpper(name) == "NTILE"
}

// GetOperatorCategory returns the category for an operator
func GetOperatorCategory(op string) string {
	return OperatorCategories[strings.ToUpper(op)]
}

// IsArithmeticOperator checks if token is arithmetic operator
func IsArithmeticOperator(op string) bool {
    return ArithmeticOperators[op]
}

// IsComparisonOperator checks if token is comparison operator
func IsComparisonOperator(op string) bool {
    upper := strings.ToUpper(op)
    // Check explicit operators not in map (parsed as multi-word)
    if upper == "IS" || upper == "NOT" {
        return true
    }
    _, exists := OperatorMap["PostgreSQL"][upper]
    return exists
}