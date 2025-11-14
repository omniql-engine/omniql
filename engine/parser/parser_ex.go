package parser

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// EXPRESSION PARSER - Handles expressions in all contexts
// Separated from main parser.go for clean architecture
// ============================================================================

// ExpressionContext defines where the expression is being parsed
type ExpressionContext string

const (
	ContextUpdate  ExpressionContext = "UPDATE"
	ContextWhere   ExpressionContext = "WHERE"
	ContextSelect  ExpressionContext = "SELECT"
	ContextOrderBy ExpressionContext = "ORDER_BY"
)

// ParseExpressionInContext parses an expression based on its context
// Returns: (field string, is expression, expression object, tokens consumed, error)
func ParseExpressionInContext(context ExpressionContext, tokens []string, startIdx int) (string, bool, *FieldExpression, int, error) {
	switch context {
	case ContextUpdate:
		return parseUpdateExpression(tokens, startIdx)
	case ContextWhere:
		return parseWhereExpression(tokens, startIdx)
	case ContextSelect:
		return parseSelectExpression(tokens, startIdx)
	case ContextOrderBy:
		return parseOrderByExpression(tokens, startIdx)
	default:
		return "", false, nil, 0, fmt.Errorf("unknown expression context: %s", context)
	}
}

// ============================================================================
// UPDATE CONTEXT - Expression in SET clause
// Stops at: comma, WHERE, ORDER BY, LIMIT, OFFSET
// ============================================================================

func parseUpdateExpression(tokens []string, startIdx int) (string, bool, *FieldExpression, int, error) {
	var exprTokens []string
	i := startIdx
	
	// Collect tokens until we hit a boundary
	for i < len(tokens) {
		token := tokens[i]
		tokenUpper := strings.ToUpper(token)
		
		// Stop at field separators or keywords
		if token == "," {
			break
		}
		
		if tokenUpper == "WHERE" || tokenUpper == "ORDER" || 
		   tokenUpper == "LIMIT" || tokenUpper == "OFFSET" {
			break
		}
		
		exprTokens = append(exprTokens, token)
		i++
	}
	
if len(exprTokens) == 0 {
	return "", false, nil, 0, fmt.Errorf("no expression tokens found")
}

	// ✅ ADD THIS: Strip outer parentheses if present
	exprStr := strings.Join(exprTokens, " ")
	exprStr = strings.TrimSpace(exprStr)

	// Remove outer parentheses: "(price - cost) * quantity" is fine
	if strings.HasPrefix(exprStr, "(") && strings.HasSuffix(exprStr, ")") {
		// Check if these are matching outer parens
		depth := 0
		matchesOuter := true
		for i, ch := range exprStr {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
				if depth == 0 && i < len(exprStr)-1 {
					matchesOuter = false
					break
				}
			}
		}
		if matchesOuter {
			// Strip outer parentheses
			exprStr = exprStr[1 : len(exprStr)-1]
			exprStr = strings.TrimSpace(exprStr)
		}
	}

	// Parse the expression
	isExpr, literal, expr, err := ParseFieldValue(exprStr)
	
	if err != nil {
		return "", false, nil, 0, err
	}
	
	if !isExpr {
		// It's a literal
		return literal, false, nil, len(exprTokens), nil
	}
	
	// It's an expression - convert to string
	field := convertExpressionToString(expr)
	return field, true, expr, len(exprTokens), nil
}

// ============================================================================
// WHERE CONTEXT - Expression in condition field
// Stops at: comparison operators (=, >, <, LIKE, IN, etc.)
// ============================================================================

func parseWhereExpression(tokens []string, startIdx int) (string, bool, *FieldExpression, int, error) {
	var exprTokens []string
	i := startIdx
	parenDepth := 0
	
	// Collect tokens until we hit a comparison operator
	for i < len(tokens) {
		token := tokens[i]
		tokenUpper := strings.ToUpper(token)
		
		// Track parentheses (for functions like UPPER(name))
		for _, ch := range token {
			if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
			}
		}
		
		// Only stop at comparison operators when not inside parentheses
		if parenDepth == 0 {
			// Stop at comparison operators
			if isComparisonOperator(tokenUpper) {
				break
			}
			
			// Stop at logical operators
			if tokenUpper == "AND" || tokenUpper == "OR" {
				break
			}
		}
		
		exprTokens = append(exprTokens, token)
		i++
	}
	
	if len(exprTokens) == 0 {
		return "", false, nil, 0, fmt.Errorf("no expression tokens found")
	}
	
	// Parse the expression
	exprStr := strings.Join(exprTokens, " ")
	isExpr, literal, expr, err := ParseFieldValue(exprStr)
	
	if err != nil {
		// If parsing fails, treat as simple field
		return exprTokens[0], false, nil, 1, nil
	}
	
	if !isExpr {
		// It's a literal/simple field
		return literal, false, nil, len(exprTokens), nil
	}
	
	// It's an expression - convert to string
	field := convertExpressionToString(expr)
	return field, true, expr, len(exprTokens), nil
}

// ============================================================================
// SELECT CONTEXT - Expression in column selection
// Stops at: AS keyword, comma, WHERE, ORDER BY, etc.
// ============================================================================

func parseSelectExpression(tokens []string, startIdx int) (string, bool, *FieldExpression, int, error) {
	var exprTokens []string
	i := startIdx
	parenDepth := 0
	
	// Collect tokens until boundary
	for i < len(tokens) {
		token := tokens[i]
		tokenUpper := strings.ToUpper(token)
		
		// Track parentheses
		for _, ch := range token {
			if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
			}
		}
		
		// Stop at boundaries when not inside parentheses
		if parenDepth == 0 {
			// Stop at AS keyword (for aliases)
			if tokenUpper == "AS" {
				break
			}
			
			// Stop at comma (next column)
			if token == "," {
				break
			}
			
			// Stop at query keywords
			if tokenUpper == "WHERE" || tokenUpper == "ORDER" || 
			   tokenUpper == "LIMIT" || tokenUpper == "OFFSET" ||
			   tokenUpper == "GROUP" || tokenUpper == "HAVING" {
				break
			}
		}
		
		exprTokens = append(exprTokens, token)
		i++
	}
	
	if len(exprTokens) == 0 {
		return "", false, nil, 0, fmt.Errorf("no expression tokens found")
	}
	
	// Parse the expression
	exprStr := strings.Join(exprTokens, " ")
	isExpr, literal, expr, err := ParseFieldValue(exprStr)
	
	if err != nil {
		return "", false, nil, 0, err
	}
	
	if !isExpr {
		return literal, false, nil, len(exprTokens), nil
	}
	
	// It's an expression
	field := convertExpressionToString(expr)
	return field, true, expr, len(exprTokens), nil
}

// ============================================================================
// ORDER BY CONTEXT - Expression in sort field
// Stops at: ASC, DESC, LIMIT, OFFSET, etc.
// ============================================================================

func parseOrderByExpression(tokens []string, startIdx int) (string, bool, *FieldExpression, int, error) {
	var exprTokens []string
	i := startIdx
	parenDepth := 0
	
	// Collect tokens until boundary
	for i < len(tokens) {
		token := tokens[i]
		tokenUpper := strings.ToUpper(token)
		
		// Track parentheses
		for _, ch := range token {
			if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
			}
		}
		
		// Stop at boundaries when not inside parentheses
		if parenDepth == 0 {
			// Stop at sort direction
			if tokenUpper == "ASC" || tokenUpper == "DESC" {
				break
			}
			
			// Stop at query keywords
			if tokenUpper == "LIMIT" || tokenUpper == "OFFSET" ||
			   tokenUpper == "WHERE" || tokenUpper == "GROUP" || tokenUpper == "HAVING" {
				break
			}
		}
		
		exprTokens = append(exprTokens, token)
		i++
	}
	
	if len(exprTokens) == 0 {
		return "", false, nil, 0, fmt.Errorf("no expression tokens found")
	}
	
	// Parse the expression
	exprStr := strings.Join(exprTokens, " ")
	isExpr, literal, expr, err := ParseFieldValue(exprStr)
	
	if err != nil {
		return "", false, nil, 0, err
	}
	
	if !isExpr {
		return literal, false, nil, len(exprTokens), nil
	}
	
	// It's an expression
	field := convertExpressionToString(expr)
	return field, true, expr, len(exprTokens), nil
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// isComparisonOperator checks if a token is a comparison operator
func isComparisonOperator(tokenUpper string) bool {
	comparisonOps := []string{
		"=", ">", "<", ">=", "<=", "!=", "<>",
		"LIKE", "NOT", "IN", "BETWEEN", "IS",
	}
	
	for _, op := range comparisonOps {
		if tokenUpper == op {
			return true
		}
	}
	return false
}

// convertExpressionToString converts a FieldExpression to SQL string
func convertExpressionToString(expr *FieldExpression) string {
	switch expr.Type {
	case ExprTypeBinary:
		return fmt.Sprintf("%s %s %s", expr.LeftOperand, expr.Operator, expr.RightOperand)
		
	case ExprTypeFunction:
		return fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(expr.FunctionArgs, ", "))
		
	case ExprTypeCaseWhen:
		var parts []string
		parts = append(parts, "CASE")
		for _, cond := range expr.CaseConditions {
			parts = append(parts, fmt.Sprintf("WHEN %s THEN %s", cond.Condition, cond.ThenValue))
		}
		if expr.CaseElse != "" {
			parts = append(parts, fmt.Sprintf("ELSE %s", expr.CaseElse))
		}
		parts = append(parts, "END")
		return strings.Join(parts, " ")
	}
	
	return ""
}

// ConvertToModelExpression converts parser.FieldExpression to models.FieldExpression
func ConvertToModelExpression(expr *FieldExpression) *models.FieldExpression {
	if expr == nil {
		return nil
	}
	
	modelExpr := &models.FieldExpression{
		Type:         string(expr.Type),
		LeftOperand:  expr.LeftOperand,
		Operator:     expr.Operator,
		RightOperand: expr.RightOperand,
		LeftIsField:  expr.LeftIsField,
		RightIsField: expr.RightIsField,
		FunctionName: expr.FunctionName,
		FunctionArgs: expr.FunctionArgs,
		CaseElse:     expr.CaseElse,
	}
	
	// Convert CaseConditions
	for _, cc := range expr.CaseConditions {
		modelExpr.CaseConditions = append(modelExpr.CaseConditions, models.CaseCondition{
			Condition: cc.Condition,
			ThenValue: cc.ThenValue,
		})
	}
	
	return modelExpr
}