package parser

import (
	"fmt"
	"regexp"
	"strings"
)

// ExpressionType defines the type of expression
type ExpressionType string

const (
	ExprTypeBinary   ExpressionType = "BINARY"   // value + 1
	ExprTypeFunction ExpressionType = "FUNCTION" // UPPER(name)
	ExprTypeCaseWhen ExpressionType = "CASEWHEN" // CASE WHEN...
)

// FieldExpression represents any expression in a SET clause
type FieldExpression struct {
	Type         ExpressionType
	
	// For binary expressions
	LeftOperand  string
	Operator     string
	RightOperand string
	LeftIsField  bool
	RightIsField bool
	
	// For functions
	FunctionName string
	FunctionArgs []string
	
	// For CASE WHEN
	CaseConditions []CaseCondition
	CaseElse       string
}

// CaseCondition represents a WHEN-THEN pair
type CaseCondition struct {
	Condition string // "age >= 18"
	ThenValue string // "adult"
}

// ParseFieldValue parses a field value which can be:
// 1. Literal: "5"
// 2. Binary expression: "value + 1"
// 3. Parenthesized expression: "(price - cost) * quantity"
// 4. Function: "UPPER(name)"
// 5. CASE WHEN: "CASE WHEN age >= 18 THEN 'adult' ELSE 'child' END"
func ParseFieldValue(input string) (isExpression bool, literal string, expr *FieldExpression, err error) {
	input = strings.TrimSpace(input)
	
	// ✅ NEW: Check for parenthesized expressions FIRST (before function check!)
	if strings.HasPrefix(input, "(") {
		return parseParenthesizedExpression(input)
	}
	
	// 1. Check for CASE WHEN (highest priority)
	if strings.HasPrefix(strings.ToUpper(input), "CASE") {
		caseExpr, err := parseCaseWhen(input)
		if err != nil {
			return false, "", nil, fmt.Errorf("failed to parse CASE WHEN: %w", err)
		}
		return true, "", caseExpr, nil
	}
	
	// 2. Check for functions (e.g., UPPER(name), NOW(), CONCAT(...))
	// Must have opening paren preceded by identifier with no spaces
	if strings.Contains(input, "(") && strings.Contains(input, ")") {
		openIdx := strings.Index(input, "(")
		if openIdx > 0 {
			// Check if character before ( is a letter (function name)
			beforeParen := strings.TrimSpace(input[:openIdx])
			if len(beforeParen) > 0 {
				lastChar := beforeParen[len(beforeParen)-1]
				// Only parse as function if last char before ( is letter/underscore
				if isLetter(rune(lastChar)) || lastChar == '_' {
					funcExpr, err := parseFunction(input)
					if err != nil {
						return false, "", nil, fmt.Errorf("failed to parse function: %w", err)
					}
					return true, "", funcExpr, nil
				}
			}
		}
	}
	
	// 3. Check for binary expressions (e.g., value + 1)
	operators := []struct {
		symbol  string
		pattern string
	}{
		{"+", ` \+ `},
		{"-", ` - `},
		{"*", ` \* `},
		{"/", ` / `},
	}
	
	for _, op := range operators {
		pattern := op.pattern
		re := regexp.MustCompile(pattern)
		
		if re.MatchString(input) {
			parts := re.Split(input, -1)
			
			if len(parts) != 2 {
				return false, "", nil, fmt.Errorf("complex expressions with multiple operators not supported: %s", input)
			}
			
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			
			if left == "" || right == "" {
				return false, "", nil, fmt.Errorf("invalid expression format: %s", input)
			}
			
			expr := &FieldExpression{
				Type:         ExprTypeBinary,
				LeftOperand:  left,
				Operator:     op.symbol,
				RightOperand: right,
				LeftIsField:  isIdentifier(left),
				RightIsField: isIdentifier(right),
			}
			
			return true, "", expr, nil
		}
	}
	
	// 4. It's a literal value
	return false, input, nil, nil
}

// parseParenthesizedExpression handles expressions starting with (
// Examples: (price - cost) * quantity, (a + b) * (c + d)
func parseParenthesizedExpression(input string) (bool, string, *FieldExpression, error) {
	input = strings.TrimSpace(input)
	
	if !strings.HasPrefix(input, "(") {
		return false, "", nil, fmt.Errorf("not a parenthesized expression")
	}
	
	// Find matching closing parenthesis
	depth := 0
	closingIdx := -1
	for i, ch := range input {
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
		return false, "", nil, fmt.Errorf("unmatched opening parenthesis")
	}
	
	// Extract content inside parentheses
	innerExpr := input[1:closingIdx]
	innerExpr = strings.TrimSpace(innerExpr)
	
	// Check if there's an operator after the closing paren
	remaining := strings.TrimSpace(input[closingIdx+1:])
	
	if remaining == "" {
		// Just parentheses around expression: (price - cost)
		// Parse inner expression recursively
		return ParseFieldValue(innerExpr)
	}
	
	// There's an operator after: (price - cost) * quantity
	// Find the operator
	tokens := strings.Fields(remaining)
	if len(tokens) < 2 {
		return false, "", nil, fmt.Errorf("incomplete expression after parentheses")
	}
	
	operator := tokens[0]
	rightOperand := strings.Join(tokens[1:], " ")
	
	// Validate operator
	validOps := map[string]bool{"+": true, "-": true, "*": true, "/": true}
	if !validOps[operator] {
		return false, "", nil, fmt.Errorf("invalid operator: %s", operator)
	}
	
	// Recursively parse left side (inside parentheses)
	leftIsExpr, leftLiteral, leftExpr, err := ParseFieldValue(innerExpr)
	if err != nil {
		return false, "", nil, fmt.Errorf("failed to parse left operand: %w", err)
	}
	
	// Convert left to string
	var leftStr string
	if leftIsExpr && leftExpr != nil {
		leftStr = expressionToString(leftExpr)
	} else {
		leftStr = leftLiteral
	}
	
	// Recursively parse right side (might also have parentheses!)
	rightIsExpr, rightLiteral, rightExpr, err := ParseFieldValue(rightOperand)
	if err != nil {
		return false, "", nil, fmt.Errorf("failed to parse right operand: %w", err)
	}
	
	// Convert right to string
	var rightStr string
	if rightIsExpr && rightExpr != nil {
		rightStr = expressionToString(rightExpr)
	} else {
		rightStr = rightLiteral
	}
	
	// Build expression with parentheses preserved
	expr := &FieldExpression{
		Type:         ExprTypeBinary,
		LeftOperand:  "(" + leftStr + ")",
		Operator:     operator,
		RightOperand: rightStr,
		LeftIsField:  false, // Parenthesized expressions are not simple fields
		RightIsField: isIdentifier(rightStr),
	}
	
	return true, "", expr, nil
}

// expressionToString converts a FieldExpression to string representation
func expressionToString(expr *FieldExpression) string {
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

// parseFunction parses function calls like UPPER(name), CONCAT(a, b), NOW()
func parseFunction(input string) (*FieldExpression, error) {
	input = strings.TrimSpace(input)
	
	// Find opening parenthesis
	openParen := strings.Index(input, "(")
	if openParen == -1 {
		return nil, fmt.Errorf("invalid function format: %s", input)
	}
	
	funcName := strings.TrimSpace(input[:openParen])
	
	// Find closing parenthesis
	closeParen := strings.LastIndex(input, ")")
	if closeParen == -1 {
		return nil, fmt.Errorf("invalid function format: %s", input)
	}
	
	// Extract arguments
	argsStr := strings.TrimSpace(input[openParen+1 : closeParen])
	
	var args []string
	if argsStr != "" {
		// Split by comma (simple split - doesn't handle nested functions yet)
		rawArgs := strings.Split(argsStr, ",")
		for _, arg := range rawArgs {
			args = append(args, strings.TrimSpace(arg))
		}
	}
	
	return &FieldExpression{
		Type:         ExprTypeFunction,
		FunctionName: strings.ToUpper(funcName),
		FunctionArgs: args,
	}, nil
}

// parseCaseWhen parses CASE WHEN statements
// Format: CASE WHEN condition THEN value [WHEN condition THEN value]... [ELSE value] END
func parseCaseWhen(input string) (*FieldExpression, error) {
	input = strings.TrimSpace(input)
	
	// Must start with CASE and end with END
	upperInput := strings.ToUpper(input)
	if !strings.HasPrefix(upperInput, "CASE") || !strings.HasSuffix(upperInput, "END") {
		return nil, fmt.Errorf("CASE statement must start with CASE and end with END")
	}
	
	// Remove CASE and END
	content := strings.TrimSpace(input[4 : len(input)-3])
	
	// ⭐ FIX: Remove leading WHEN if present
	contentUpper := strings.ToUpper(content)
	if strings.HasPrefix(contentUpper, "WHEN ") {
		content = strings.TrimSpace(content[5:]) // Remove "WHEN "
	}
	
	expr := &FieldExpression{
		Type:           ExprTypeCaseWhen,
		CaseConditions: []CaseCondition{},
	}
	
	// Split by WHEN (case-insensitive) for subsequent WHENs
	whenPattern := regexp.MustCompile(`(?i)\s+WHEN\s+`)
	parts := whenPattern.Split(content, -1)
	
	// Process all parts
	for i := 0; i < len(parts); i++ {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			continue
		}
		
		// Check if this part contains ELSE (last part)
		elsePattern := regexp.MustCompile(`(?i)\s+ELSE\s+`)
		if elsePattern.MatchString(part) {
			// Split by ELSE
			elseParts := elsePattern.Split(part, 2)
			
			// First part is the last WHEN-THEN
			if len(elseParts[0]) > 0 {
				cond, err := parseWhenThen(elseParts[0])
				if err != nil {
					return nil, err
				}
				expr.CaseConditions = append(expr.CaseConditions, cond)
			}
			
			// Second part is ELSE value
			if len(elseParts) > 1 {
				expr.CaseElse = strings.TrimSpace(elseParts[1])
			}
			
			break
		}
		
		// Regular WHEN-THEN
		cond, err := parseWhenThen(part)
		if err != nil {
			return nil, err
		}
		expr.CaseConditions = append(expr.CaseConditions, cond)
	}
	
	if len(expr.CaseConditions) == 0 {
		return nil, fmt.Errorf("CASE statement must have at least one WHEN condition")
	}
	
	return expr, nil
}

// parseWhenThen parses a single "condition THEN value" pair
func parseWhenThen(input string) (CaseCondition, error) {
	// Split by THEN
	thenPattern := regexp.MustCompile(`(?i)\s+THEN\s+`)
	parts := thenPattern.Split(input, 2)
	
	if len(parts) != 2 {
		return CaseCondition{}, fmt.Errorf("WHEN must be followed by THEN: %s", input)
	}
	
	condition := strings.TrimSpace(parts[0])
	thenValue := strings.TrimSpace(parts[1])
	
	if condition == "" || thenValue == "" {
		return CaseCondition{}, fmt.Errorf("invalid WHEN-THEN format: %s", input)
	}
	
	return CaseCondition{
		Condition: condition,
		ThenValue: thenValue,
	}, nil
}

// isIdentifier checks if a string is a column name or a literal value
func isIdentifier(s string) bool {
	s = strings.TrimSpace(s)
	
	// Quoted strings are literals
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		return false
	}
	
	// Numbers are literals
	numberPattern := `^-?\d+(\.\d+)?$`
	matched, _ := regexp.MatchString(numberPattern, s)
	if matched {
		return false
	}
	
	// Otherwise it's a column name
	return true
}

// isLetter checks if a rune is a letter
func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}