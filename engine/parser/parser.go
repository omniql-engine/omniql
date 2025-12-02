package parser

import (
	"fmt"
	"strings"
	"regexp"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	"github.com/omniql-engine/omniql/engine/parser/ast"
)

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// Parse converts OQL string into Query struct
// Routes to appropriate group parser based on operation type
func Parse(oql string) (*models.Query, error) {
	oql = strings.TrimSpace(oql)
	
	if oql == "" {
		return nil, fmt.Errorf("empty query")
	}
	
	// ✅ NEW: Tokenize first - validates all tokens against mapping
	_, err := ast.Tokenize(oql)
	if err != nil {
		return nil, err
	}
	
	parts := strings.Fields(oql)
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid query format")
	}
	
	// Detect multi-word operations (CREATE TABLE, ALTER TABLE, etc.)
	operation := detectOperation(parts)
	
	// Special case: GET with aggregate (GET User COUNT, GET Project SUM:budget)
	if operation == "GET" && len(parts) >= 3 {
		// Scan for aggregate keywords anywhere after entity (position 2+)
		for i := 2; i < len(parts); i++ {
			potentialAgg := strings.ToUpper(parts[i])
			aggFunc := strings.Split(potentialAgg, ":")[0]
			if isAggregateKeyword(aggFunc) {
				operation = potentialAgg
				break  // Found aggregate, stop scanning
			}
		}

		// Check for window functions (GET User WITH ROW NUMBER)
		if len(parts) >= 4 && strings.ToUpper(parts[2]) == "WITH" {
			windowFunc := strings.ToUpper(parts[3])
			if isWindowFunction(windowFunc) {
				operation = windowFunc
			}
			// Check for two-word window functions (ROW NUMBER, DENSE RANK)
			if len(parts) >= 5 {
				twoWord := windowFunc + " " + strings.ToUpper(parts[4])
				if isWindowFunction(twoWord) {
					operation = twoWord
				}
			}
		}
		
		// Check for window functions (GET User WITH ROW NUMBER)
		if len(parts) >= 4 && strings.ToUpper(parts[2]) == "WITH" {
			windowFunc := strings.ToUpper(parts[3])
			if isWindowFunction(windowFunc) {
				operation = windowFunc
			}
		}
	}
	
	// Special case: WITH clause (CTE)
	if operation == "WITH" {
		return parseCTE(parts)
	}
	
	// Special case: Set operations (UNION, INTERSECT, EXCEPT)
	if operation == "GET" {
		upperOQL := strings.ToUpper(oql)
		setOps := []string{"UNION ALL", "UNION", "INTERSECT", "EXCEPT"}
		for _, setOp := range setOps {
			if strings.Contains(upperOQL, " "+setOp+" ") {
				operation = setOp
				break
			}
		}
	}
	
	// Route to appropriate group parser
	group := getOperationGroup(operation)

	switch group {
	case "CRUD":
		return parseCRUD(operation, parts)
	case "DDL":
		return parseDDL(operation, parts)
	case "DQL":
		return parseDQL(operation, parts)
	case "TCL":
		return parseTCL(operation, parts)
	case "DCL":
		return parseDCL(operation, parts)
	default:
		return nil, fmt.Errorf("unknown operation: %s", operation)
	}
}

// ============================================================================
// OPERATION DETECTION (Uses mapping.OperationGroups)
// ============================================================================

// detectOperation detects multi-word operations like CREATE TABLE, ALTER TABLE
func detectOperation(parts []string) string {
	if len(parts) < 2 {
		return strings.ToUpper(parts[0])
	}
	
	// Try two-word operation first (CREATE TABLE, DROP USER, etc.)
	twoWord := strings.ToUpper(parts[0]) + " " + strings.ToUpper(parts[1])
	
	// Check if this is a valid two-word operation
	if _, exists := mapping.OperationGroups[twoWord]; exists {
		// Only accept if the second word is uppercase in original (User vs USER)
		if parts[1] == strings.ToUpper(parts[1]) {
			return twoWord
		}
	}
	
	// Fall back to single word
	return strings.ToUpper(parts[0])
}

func getEntityIndex(operation string) int {
	wordCount := strings.Count(operation, " ") + 1
	return wordCount
}

// getOperationGroup determines which group an operation belongs to
// Uses mapping.OperationGroups - completely dynamic
func getOperationGroup(operation string) string {
	// Extract base operation (SUM:budget → SUM)
	baseOp := strings.Split(operation, ":")[0]
	
	group, exists := mapping.OperationGroups[baseOp]
	if !exists {
		return "UNKNOWN"
	}
	return group
}

// isAggregateKeyword checks if keyword is an aggregate function
func isAggregateKeyword(keyword string) bool {
	aggKeywords := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}
	for _, agg := range aggKeywords {
		if keyword == agg {
			return true
		}
	}
	return false
}

// isWindowFunction checks if keyword is a window function
func isWindowFunction(keyword string) bool {
	windowFuncs := []string{"ROW NUMBER", "RANK", "DENSE RANK", "LAG", "LEAD", "NTILE"}
	for _, wf := range windowFuncs {
		if keyword == wf {
			return true
		}
	}
	return false
}

// ============================================================================
// QUOTE HANDLING
// ============================================================================

// parseQuotedValue extracts a quoted value from parts starting at index startIdx
// Supports both single (') and double (") quotes
// Handles escape sequences: \', \", \\, \n, \t
// Returns: (unescaped value, tokens consumed, error)
func parseQuotedValue(parts []string, startIdx int) (string, int, error) {
	if startIdx >= len(parts) {
		return "", 0, fmt.Errorf("index out of bounds")
	}
	
	firstToken := parts[startIdx]
	if len(firstToken) == 0 {
		return "", 0, fmt.Errorf("empty token")
	}
	
	// Detect quote type
	quoteChar := firstToken[0]
	if quoteChar != '\'' && quoteChar != '"' {
		return "", 0, fmt.Errorf("value does not start with quote")
	}
	
	// Check if quote is closed in the same token (e.g., 'value' or "value")
	if len(firstToken) > 1 && firstToken[len(firstToken)-1] == quoteChar {
		// Single token with quotes - check if closing quote is escaped
		content := firstToken[1 : len(firstToken)-1]
		
		// Count trailing backslashes
		backslashCount := 0
		for i := len(content) - 1; i >= 0 && content[i] == '\\'; i-- {
			backslashCount++
		}
		
		// If even number of backslashes (or zero), closing quote is NOT escaped
		if backslashCount%2 == 0 {
			return unescapeString(content), 1, nil
		}
	}
	
	// Multi-token quoted value - collect until closing quote
	var contentParts []string
	contentParts = append(contentParts, firstToken[1:]) // Remove opening quote
	
	tokensConsumed := 1
	foundClosing := false
	
	for i := startIdx + 1; i < len(parts); i++ {
		token := parts[i]
		tokensConsumed++
		
		// Check if this token ends with the closing quote
		if len(token) > 0 && token[len(token)-1] == quoteChar {
			// Check if it's escaped
			if len(token) > 1 && token[len(token)-2] == '\\' {
				// Count consecutive backslashes before quote
				backslashCount := 0
				for j := len(token) - 2; j >= 0 && token[j] == '\\'; j-- {
					backslashCount++
				}
				// If odd number of backslashes, quote is escaped
				if backslashCount%2 == 1 {
					contentParts = append(contentParts, token)
					continue
				}
			}
			
			// Found unescaped closing quote
			contentParts = append(contentParts, token[:len(token)-1]) // Remove closing quote
			foundClosing = true
			break
		}
		
		contentParts = append(contentParts, token)
	}
	
	if !foundClosing {
		return "", 0, fmt.Errorf("unclosed quote: expected closing %c", quoteChar)
	}
	
	// Join parts with spaces and unescape
	content := strings.Join(contentParts, " ")
	unescaped := unescapeString(content)
	
	return unescaped, tokensConsumed, nil
}

// unescapeString processes escape sequences in a string
// Supports: \', \", \\, \n, \t, \r
func unescapeString(s string) string {
	var result strings.Builder
	i := 0
	
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			// Handle escape sequence
			switch s[i+1] {
			case '\'':
				result.WriteByte('\'')
				i += 2
			case '"':
				result.WriteByte('"')
				i += 2
			case '\\':
				result.WriteByte('\\')
				i += 2
			case 'n':
				result.WriteByte('\n')
				i += 2
			case 't':
				result.WriteByte('\t')
				i += 2
			case 'r':
				result.WriteByte('\r')
				i += 2
			default:
				// Unknown escape sequence, keep backslash
				result.WriteByte(s[i])
				i++
			}
		} else {
			result.WriteByte(s[i])
			i++
		}
	}
	
	return result.String()
}

// ============================================================================
// CONDITION PARSING (Uses mapping.OperatorMap)
// ============================================================================

// parseConditions parses WHERE conditions with support for:
// - Parentheses: (age > 25 OR vip = true) AND status = active
// - IN operator: status IN (active, pending, verified)
// - BETWEEN: age BETWEEN 18 AND 65
// - IS NULL / IS NOT NULL: deleted_at IS NULL
// - Quoted values: name = 'O\'Brien'
// - Expressions: (price - cost) * quantity > 100
func parseConditions(parts []string) ([]models.Condition, error) {
	// ✅ FIX IN OPERATOR SPACING FIRST (before any tokenization)
	rejoined := strings.Join(parts, " ")
	rejoined = fixInOperatorSpacing(rejoined)  // ✅ ADD THIS LINE
	parts = strings.Fields(rejoined)            // ✅ ADD THIS LINE
	
	// ✅ CONDITIONAL: Only tokenize if we detect expression patterns
	// Check if we have expression operators in the conditions
	rejoined = strings.Join(parts, " ")  // ✅ Re-join after IN fix
	hasExpressionOperators := strings.Contains(rejoined, " * ") || 
	                          strings.Contains(rejoined, " / ") ||
	                          strings.Contains(rejoined, " + ") ||
	                          strings.Contains(rejoined, " - ")
	
	// Only apply special tokenization if we have expression operators
	if hasExpressionOperators {
		parts = tokenizeWithParens(rejoined)
	}

	// Validate parentheses balance before parsing
	openCount := 0
	for _, part := range parts {
		if part == "(" {
			openCount++
		} else if part == ")" {
			openCount--
			if openCount < 0 {
				return nil, fmt.Errorf("unmatched closing parenthesis")
			}
		}
	}
	if openCount > 0 {
		return nil, fmt.Errorf("unmatched opening parenthesis")
	}
	
	return parseConditionsRecursive(parts, 0)
}

// parseConditionsRecursive handles nested conditions with parentheses
func parseConditionsRecursive(parts []string, startIdx int) ([]models.Condition, error) {
	var conditions []models.Condition
	i := startIdx
	
	for i < len(parts) {
		// Skip AND/OR operators at start
		token := strings.ToUpper(parts[i])
		if token == "AND" || token == "OR" {
			i++
			continue
		}
		
		// Handle opening parenthesis - nested group
		// Handle opening parenthesis - nested group OR expression
		if parts[i] == "(" {
			// Find matching closing parenthesis
			closingIdx := findMatchingParen(parts, i)
			if closingIdx == -1 {
				return nil, fmt.Errorf("unmatched opening parenthesis")
			}
			
			// ✅ NEW: Check if this is a condition group or an expression
			// Condition group: (age > 18 OR status = active) AND ...
			// Expression: (price - cost) * quantity > 50
			
			// Extract tokens inside parentheses
			innerTokens := parts[i+1 : closingIdx]
			
			// Check if there's an operator AFTER the closing paren (expression)
			hasOperatorAfter := closingIdx+1 < len(parts) && 
								(parts[closingIdx+1] == "*" || parts[closingIdx+1] == "/" || 
								parts[closingIdx+1] == "+" || parts[closingIdx+1] == "-")
			
			if hasOperatorAfter {
				// It's an expression like (price - cost) * quantity
				// Parse as a single condition starting here
				condition, consumed, err := parseSingleCondition(parts, i)
				if err != nil {
					return nil, err
				}
				
				// Determine logic operator
				if len(conditions) > 0 && i > 0 {
					prevToken := strings.ToUpper(parts[i-1])
					if prevToken == "OR" {
						condition.Logic = "OR"
					} else {
						condition.Logic = "AND"
					}
				}
				
				conditions = append(conditions, condition)
				i += consumed
				
				// Skip trailing AND/OR
				if i < len(parts) && (strings.ToUpper(parts[i]) == "AND" || strings.ToUpper(parts[i]) == "OR") {
					i++
				}
				continue
			}
			
			// It's a condition group - parse recursively
			groupConditions, err := parseConditionsRecursive(innerTokens, 0)
			if err != nil {
				return nil, err
			}
			
			// Determine logic operator before this group
			logic := "AND" // default
			if len(conditions) > 0 && i > 0 {
				prevToken := strings.ToUpper(parts[i-1])
				if prevToken == "OR" {
					logic = "OR"
				}
			}
			
			// Wrap group in a condition
			groupCondition := models.Condition{
				Logic:      logic,
				Conditions: groupConditions,
			}
			conditions = append(conditions, groupCondition)
			
			i = closingIdx + 1
			continue
		}
		
		// Handle closing parenthesis - end of this group
		if parts[i] == ")" {
			break
		}
		
		// Parse single condition
		condition, consumed, err := parseSingleCondition(parts, i)
		if err != nil {
			return nil, err
		}
		
		// Determine logic operator
		if len(conditions) > 0 && i > 0 {
			prevToken := strings.ToUpper(parts[i-1])
			if prevToken == "OR" {
				condition.Logic = "OR"
			} else {
				condition.Logic = "AND"
			}
		}
		
		conditions = append(conditions, condition)
		i += consumed
		
		// Skip trailing AND/OR
		if i < len(parts) && (strings.ToUpper(parts[i]) == "AND" || strings.ToUpper(parts[i]) == "OR") {
			i++
		}
	}
	
	return conditions, nil
}

// parseSingleCondition parses one condition (field operator value)
// parseSingleCondition parses one condition (field operator value)
// Uses OperatorMap for validation - no hardcoded operator checks
func parseSingleCondition(parts []string, startIdx int) (models.Condition, int, error) {
	if startIdx+1 >= len(parts) {
		return models.Condition{}, 0, fmt.Errorf("incomplete condition")
	}
	
	// ✅ Try to parse as expression first (for WHERE price * quantity > 100)
	field, _, _, fieldTokens, err := ParseExpressionInContext(ContextWhere, parts, startIdx)
	if err != nil || fieldTokens == 0 {
		// Fall back to simple field
		field = parts[startIdx]
		fieldTokens = 1
	}
	
	// ✅ Calculate where operator starts
	operatorStartIdx := startIdx + fieldTokens
	
	// Build potential multi-word operators dynamically
	var operator string
	var operatorTokens int

	maxWords := len(parts) - operatorStartIdx
	if maxWords > 5 {
		maxWords = 5
	}

	// Try building multi-word operators from longest to shortest
	for wordCount := maxWords; wordCount >= 1; wordCount-- {
		var opWords []string
		validCandidate := true
		
		for i := 0; i < wordCount; i++ {
			idx := operatorStartIdx + i
			if idx >= len(parts) {
				validCandidate = false
				break
			}
			token := parts[idx]
			
			// Skip if this token is a quoted value (not part of operator)
			if len(token) > 0 && (token[0] == '\'' || token[0] == '"') {
				validCandidate = false
				break
			}
			
			opWords = append(opWords, strings.ToUpper(token))
		}
		
		if !validCandidate {
			continue
		}
		
		candidateOp := strings.Join(opWords, " ")
		
		// Check if this is a valid operator in OperatorMap
		if isValidOperator(candidateOp) {
			operator = candidateOp
			operatorTokens = wordCount
			break
		}
	}
	
	// If no valid operator found, error
	if operator == "" {
		if operatorStartIdx < len(parts) {
			return models.Condition{}, 0, fmt.Errorf("unknown operator: %s", parts[operatorStartIdx])
		}
		return models.Condition{}, 0, fmt.Errorf("missing operator after field")
	}

	// Adjust operatorTokens to include field tokens for helper functions
	operatorTokensAdjusted := (fieldTokens - 1) + operatorTokens
	
	// Normalize operator for switch statement
	normalized := normalizeOperator(operator)
	
	// Route to appropriate parser based on operator type
	switch normalized {
	case "IN", "NOT_IN":
		return parseInOperator(parts, startIdx, field, normalized, operatorTokensAdjusted)
		
	case "BETWEEN", "NOT_BETWEEN":
		return parseBetweenOperator(parts, startIdx, field, normalized, operatorTokensAdjusted)
		
	case "IS_NULL", "IS_NOT_NULL":
		return parseNullCheckOperator(field, normalized, operatorTokensAdjusted)
		
	default:
		return parseStandardOperator(parts, startIdx, field, normalized, operatorTokensAdjusted)
	}
}

// parseInOperator handles IN and NOT_IN operators
func parseInOperator(parts []string, startIdx int, field, operator string, operatorTokens int) (models.Condition, int, error) {
	// Expect opening parenthesis after operator
	parenIdx := startIdx + 1 + operatorTokens
	if parenIdx >= len(parts) || parts[parenIdx] != "(" {
		return models.Condition{}, 0, fmt.Errorf("%s operator requires parentheses", operator)
	}
	
	// Find closing parenthesis
	closingIdx := findMatchingParen(parts, parenIdx)
	if closingIdx == -1 {
		return models.Condition{}, 0, fmt.Errorf("unmatched parenthesis in %s clause", operator)
	}
	
	// Parse values inside parentheses
	values, err := parseInValues(parts, parenIdx+1, closingIdx)
	if err != nil {
		return models.Condition{}, 0, err
	}
	
	consumed := closingIdx - startIdx + 1
	return models.Condition{
		Field:    field,
		Operator: operator,
		Values:   values,
	}, consumed, nil
}

// parseBetweenOperator handles BETWEEN and NOT_BETWEEN operators
func parseBetweenOperator(parts []string, startIdx int, field, operator string, operatorTokens int) (models.Condition, int, error) {
	value1Start := startIdx + 1 + operatorTokens
	
	if value1Start >= len(parts) {
		return models.Condition{}, 0, fmt.Errorf("%s requires two values", operator)
	}
	
	// Get first value
	var value1 string
	var value1Consumed int
	
	if len(parts[value1Start]) > 0 && (parts[value1Start][0] == '\'' || parts[value1Start][0] == '"') {
		var err error
		value1, value1Consumed, err = parseQuotedValue(parts, value1Start)
		if err != nil {
			return models.Condition{}, 0, err
		}
	} else {
		value1 = parts[value1Start]
		value1Consumed = 1
	}
	
	// Expect AND keyword
	andIdx := value1Start + value1Consumed
	if andIdx >= len(parts) || strings.ToUpper(parts[andIdx]) != "AND" {
		return models.Condition{}, 0, fmt.Errorf("%s requires AND keyword", operator)
	}
	
	// Get second value
	value2Start := andIdx + 1
	if value2Start >= len(parts) {
		return models.Condition{}, 0, fmt.Errorf("%s missing second value", operator)
	}
	
	var value2 string
	var value2Consumed int
	
	if len(parts[value2Start]) > 0 && (parts[value2Start][0] == '\'' || parts[value2Start][0] == '"') {
		var err error
		value2, value2Consumed, err = parseQuotedValue(parts, value2Start)
		if err != nil {
			return models.Condition{}, 0, err
		}
	} else {
		value2 = parts[value2Start]
		value2Consumed = 1
	}
	
	consumed := value2Start + value2Consumed - startIdx
	return models.Condition{
		Field:    field,
		Operator: operator,
		Value:    value1,
		Value2:   value2,
	}, consumed, nil
}

// parseNullCheckOperator handles IS_NULL and IS_NOT_NULL operators
func parseNullCheckOperator(field, operator string, operatorTokens int) (models.Condition, int, error) {
	return models.Condition{
		Field:    field,
		Operator: operator,
		Value:    "",
	}, operatorTokens + 1, nil // field + operator tokens
}

// parseStandardOperator handles standard operators (=, >, <, LIKE, etc.)
func parseStandardOperator(parts []string, startIdx int, field, operator string, operatorTokens int) (models.Condition, int, error) {
	valueStart := startIdx + 1 + operatorTokens
	
	if valueStart >= len(parts) {
		return models.Condition{}, 0, fmt.Errorf("missing value for %s operator", operator)
	}
	
	var value string
	var valueConsumed int
	
	// Check if value is quoted
	if len(parts[valueStart]) > 0 && (parts[valueStart][0] == '\'' || parts[valueStart][0] == '"') {
		var err error
		value, valueConsumed, err = parseQuotedValue(parts, valueStart)
		if err != nil {
			return models.Condition{}, 0, err
		}
		// Quoted values are fine - no wildcard check needed
	} else {
		// Unquoted value - collect until keyword
		keywords := []string{"AND", "OR", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING", "UNION", "INTERSECT", "EXCEPT", ")", "COUNT", "SUM", "AVG", "MIN", "MAX"}
		var valueTokens []string
		j := valueStart
		
		for j < len(parts) {
			token := parts[j]
			tokenUpper := strings.ToUpper(token)
			
			// Check if this is a keyword
			isKeyword := false
			for _, kw := range keywords {
				if tokenUpper == kw {
					isKeyword = true
					break
				}
			}
			
			if isKeyword {
				break
			}
			
			valueTokens = append(valueTokens, token)
			j++
		}
		
		if len(valueTokens) == 0 {
			return models.Condition{}, 0, fmt.Errorf("missing value for %s operator", operator)
		}
		
		value = strings.Join(valueTokens, " ")
		valueConsumed = len(valueTokens)
		
		// // ✅ FIXED: Check wildcards ONLY for unquoted LIKE-family operators
		// if operator == "LIKE" || operator == "NOT_LIKE" || operator == "ILIKE" || operator == "NOT_ILIKE" {
		// 	if strings.Contains(value, "%") || strings.Contains(value, "_") {
		// 		return models.Condition{}, 0, fmt.Errorf("%s pattern with wildcards (%%,_) must be quoted. Use: %s '%s'", operator, operator, value)
		// 	}
		// }
	}
	
	consumed := 1 + operatorTokens + valueConsumed
	return models.Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	}, consumed, nil
}

// ============================================================================
// OPERATOR VALIDATION (Uses OperatorMap - No Hardcoding)
// ============================================================================

// normalizeOperator converts operator to mapping format
// "IS NULL" → "IS_NULL", "NOT IN" → "NOT_IN"
func normalizeOperator(op string) string {
	return strings.ReplaceAll(strings.ToUpper(op), " ", "_")
}

// isValidOperator checks if operator exists in OperatorMap
func isValidOperator(operator string) bool {
	normalized := normalizeOperator(operator)
	
	// Check if operator exists in any database
	for _, dbOps := range mapping.OperatorMap {
		if _, exists := dbOps[normalized]; exists {
			return true
		}
	}
	return false
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// findMatchingParen finds the index of the closing parenthesis
func findMatchingParen(parts []string, openIdx int) int {
	if openIdx >= len(parts) || parts[openIdx] != "(" {
		return -1
	}
	
	depth := 1
	for i := openIdx + 1; i < len(parts); i++ {
		if parts[i] == "(" {
			depth++
		} else if parts[i] == ")" {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	
	return -1 // No matching closing paren
}

// parseInValues parses the values inside IN (...) clause
func parseInValues(parts []string, startIdx, endIdx int) ([]string, error) {
	var values []string
	i := startIdx
	
	for i < endIdx {
		// Skip commas
		if parts[i] == "," {
			i++
			continue
		}
		
		// Parse value (quoted or unquoted)
		if len(parts[i]) > 0 && (parts[i][0] == '\'' || parts[i][0] == '"') {
			value, consumed, err := parseQuotedValue(parts, i)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			i += consumed
		} else {
			// Unquoted value - collect until comma or end
			var valueTokens []string
			for i < endIdx && parts[i] != "," {
				valueTokens = append(valueTokens, parts[i])
				i++
			}
			
			if len(valueTokens) > 0 {
				values = append(values, strings.Join(valueTokens, " "))
			}
		}
	}
	
	return values, nil
}

// parseFields parses field assignments: name = John, age = 30
// Used by: CREATE, UPDATE, CREATE TABLE operations
// Handles multi-word values: name = Project Alpha
func parseFields(parts []string) ([]models.Field, error) {
	var fields []models.Field
	
	i := 0
	for i < len(parts) {
		// Skip standalone comma tokens (result of space normalization)
		if i < len(parts) && parts[i] == "," {
			i++
			continue
		}
		
		// Need at least: name = value
		if i+2 >= len(parts) {
			break
		}
		
		if parts[i+1] != "=" {
			// Skip if not a valid field assignment
			i++
			continue
		}
		
		fieldName := parts[i]
		
		// Check if value is quoted
		valueStartToken := parts[i+2]
		var value string
		var tokensConsumed int
		
		if len(valueStartToken) > 0 && (valueStartToken[0] == '\'' || valueStartToken[0] == '"') {
			// Quoted value - use parseQuotedValue
			var err error
			value, tokensConsumed, err = parseQuotedValue(parts, i+2)
			if err != nil {
				return nil, fmt.Errorf("error parsing quoted value for field %s: %w", fieldName, err)
			}
		} else {
			// Unquoted value - collect tokens until we hit a field separator
			var valueTokens []string
			j := i + 2
			
			for j < len(parts) {
				token := parts[j]
				
				// Check if this is start of next field (token followed by "=")
				if j+1 < len(parts) && parts[j+1] == "=" {
					// This token is the next field name, stop here
					break
				}
				
				// Check if this is a standalone comma followed by a field (nextToken = something)
				if token == "," {
					// Peek ahead to see if this comma is followed by a field assignment
					if j+2 < len(parts) && parts[j+2] == "=" {
						// This comma separates fields, stop before it
						break
					}
					// Otherwise, it's a comma INSIDE the value (like JSON), include it
					valueTokens = append(valueTokens, token)
					j++
					continue
				}
				
				// Stop if this token ends with comma AND next token starts a field
				if strings.HasSuffix(token, ",") {
					if j+2 < len(parts) && parts[j+2] == "=" {
						// Comma is field separator, remove it and stop
						valueTokens = append(valueTokens, strings.TrimSuffix(token, ","))
						j++
						break
					}
					// Comma is part of value, include whole token
					valueTokens = append(valueTokens, token)
					j++
					continue
				}
				
				// Stop if we hit a SQL keyword (WHERE, ON, etc.)
				tokenUpper := strings.ToUpper(token)
				keywords := []string{"WHERE", "ON", "SET", "AND", "OR", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING"}
				isKeyword := false
				for _, kw := range keywords {
					if tokenUpper == kw {
						isKeyword = true
						break
					}
				}
				if isKeyword {
					break
				}
				
				valueTokens = append(valueTokens, token)
				j++
			}
			
			if len(valueTokens) == 0 {
				return nil, fmt.Errorf("missing value for field: %s", fieldName)
			}
			
			value = strings.Join(valueTokens, " ")
			tokensConsumed = j - (i + 2)
		}
		
		field := models.Field{
			Name:  fieldName,
			Value: value,
		}
		
		fields = append(fields, field)
		
		// Move past: fieldName + "=" + value
		i = i + 2 + tokensConsumed
		
		// Skip the comma if it's the next token
		if i < len(parts) && parts[i] == "," {
			i++
		}
	}
	
	return fields, nil
}

// findKeyword finds index of keyword in parts (case insensitive)
// Supports multi-word keywords like "ORDER BY", "GROUP BY"
func findKeyword(parts []string, keyword string) int {
	// Check if keyword is multi-word
	keywordParts := strings.Fields(keyword)
	
	if len(keywordParts) == 1 {
		// Single-word keyword
		for i, part := range parts {
			if strings.ToUpper(part) == strings.ToUpper(keyword) {
				return i
			}
		}
		return -1
	}
	
	// Multi-word keyword - check consecutive parts
	for i := 0; i <= len(parts)-len(keywordParts); i++ {
		match := true
		for j, kwPart := range keywordParts {
			if i+j >= len(parts) || strings.ToUpper(parts[i+j]) != strings.ToUpper(kwPart) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	
	return -1
}

// findKeywords finds indices of multiple keywords
func findKeywords(parts []string, keywords []string) map[string]int {
	result := make(map[string]int)
	for _, keyword := range keywords {
		result[keyword] = findKeyword(parts, keyword)
	}
	return result
}

// validateOperation checks if operation is supported for given database
func validateOperation(operation, databaseType string) error {
	if databaseType == "" {
		return nil
	}
	
	dbOps, exists := mapping.OperationMap[databaseType]
	if !exists {
		return fmt.Errorf("unsupported database type: %s", databaseType)
	}
	
	if _, exists := dbOps[operation]; !exists {
		return fmt.Errorf("operation %s not supported for %s", operation, databaseType)
	}
	
	return nil
}

// getTableNamingRule gets the naming rule for an operation
func getTableNamingRule(operation string) string {
	rule, exists := mapping.TableNamingRules[operation]
	if !exists {
		return "plural"
	}
	return rule
}

// applyTableNaming applies naming rule to entity name
func applyTableNaming(entity, rule string) string {
	switch rule {
	case "plural":
		return entity + "s"
	case "exact":
		return entity
	case "none":
		return ""
	default:
		return entity + "s"
	}
}

// extractStringBetween extracts string between two delimiters
func extractStringBetween(s, start, end string) string {
	startIdx := strings.Index(s, start)
	if startIdx == -1 {
		return ""
	}
	startIdx += len(start)
	
	endIdx := strings.Index(s[startIdx:], end)
	if endIdx == -1 {
		return s[startIdx:]
	}
	
	return s[startIdx : startIdx+endIdx]
}

// parseCTE handles WITH clause (Common Table Expressions)
func parseCTE(parts []string) (*models.Query, error) {
	return nil, fmt.Errorf("CTE parsing not yet implemented")
}

// Add this helper function at the bottom of parser.go
func tokenizeWithParens(text string) []string {
    var tokens []string
    var current strings.Builder
    
    for i := 0; i < len(text); i++ {
        ch := text[i]
        
        switch ch {
        case '(', ')', ',':
            // Flush current token
            if current.Len() > 0 {
                tokens = append(tokens, current.String())
                current.Reset()
            }
            // Add separator as token
            tokens = append(tokens, string(ch))
            
        case ' ', '\t', '\n':
            // Flush on whitespace
            if current.Len() > 0 {
                tokens = append(tokens, current.String())
                current.Reset()
            }
            
        default:
            current.WriteByte(ch)
        }
    }
    
    // Flush remaining
    if current.Len() > 0 {
        tokens = append(tokens, current.String())
    }
    
    return tokens
}

// ============================================================================
// COLUMN SELECTION PARSING (NEW)
// ============================================================================

// parseColumns parses column list: User.name, User.email, Project.title
func parseColumns(parts []string) ([]string, error) {
	var columns []string
	
	// Join all parts and split by comma
	joined := strings.Join(parts, " ")
	
	// Remove spaces around commas for clean split
	joined = strings.ReplaceAll(joined, " , ", ",")
	joined = strings.ReplaceAll(joined, ", ", ",")
	
	columnList := strings.Split(joined, ",")
	
	for _, col := range columnList {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		
		// Validate format: Table.column
		if !strings.Contains(col, ".") {
			return nil, fmt.Errorf("column must specify table: %s (use Table.column format)", col)
		}
		
		columns = append(columns, col)
	}
	
	if len(columns) == 0 {
		return nil, fmt.Errorf("COLUMNS clause requires at least one column")
	}
	
	return columns, nil
}

// ============================================================================
// IN OPERATOR FIX - Surgical spacing normalization
// ============================================================================

// fixInOperatorSpacing normalizes spacing for IN and NOT IN operators
// Converts: IN(a,b,c) → IN ( a , b , c )
// Does NOT affect: UPPER(name), STRING(100), (expressions), etc.
func fixInOperatorSpacing(s string) string {
	// Import at top of file if not present: "regexp"
	
	// Fix IN operator: IN(a,b) → IN ( a , b )
	reIn := regexp.MustCompile(`\bIN\s*\(([^)]+)\)`)
	s = reIn.ReplaceAllStringFunc(s, func(match string) string {
		// Extract content between parentheses
		start := strings.Index(match, "(")
		end := strings.LastIndex(match, ")")
		if start == -1 || end == -1 || start >= end {
			return match // Invalid, return unchanged
		}
		
		content := match[start+1 : end]
		
		// Add spaces around commas
		content = strings.ReplaceAll(content, ",", " , ")
		content = strings.TrimSpace(content)
		
		return "IN ( " + content + " )"
	})
	
	// Fix NOT IN operator: NOT IN(a,b) → NOT IN ( a , b )
	reNotIn := regexp.MustCompile(`\bNOT\s+IN\s*\(([^)]+)\)`)
	s = reNotIn.ReplaceAllStringFunc(s, func(match string) string {
		start := strings.Index(match, "(")
		end := strings.LastIndex(match, ")")
		if start == -1 || end == -1 || start >= end {
			return match
		}
		
		content := match[start+1 : end]
		content = strings.ReplaceAll(content, ",", " , ")
		content = strings.TrimSpace(content)
		
		return "NOT IN ( " + content + " )"
	})
	
	return s
}