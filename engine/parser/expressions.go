package parser

import (
	"strings"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/ast"
	"github.com/omniql-engine/omniql/engine/lexer"
)

// =============================================================================
// EXPRESSION PARSING (100% TrueAST)
// Grammar: expression = term (('+' | '-') term)*
//          term       = factor (('*' | '/') factor)*
//          factor     = primary | '(' expression ')'
//          primary    = identifier | number | string | function_call
// =============================================================================

// parseExpression parses arithmetic/logical expressions
func (p *Parser) parseExpression() (*ast.ExpressionNode, error) {
	return p.parseOrExpr()
}

// parseOrExpr parses: expr OR expr
func (p *Parser) parseOrExpr() (*ast.ExpressionNode, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.match("OR") {
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &ast.ExpressionNode{
			Type:     "BINARY",
			Left:     left,
			Operator: "OR",
			Right:    right,
			Position: left.Position,
		}
	}
	return left, nil
}

// parseAndExpr parses: expr AND expr
func (p *Parser) parseAndExpr() (*ast.ExpressionNode, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	for p.match("AND") {
		right, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		left = &ast.ExpressionNode{
			Type:     "BINARY",
			Left:     left,
			Operator: "AND",
			Right:    right,
			Position: left.Position,
		}
	}
	return left, nil
}

// parseAdditive parses: term (('+' | '-') term)*
func (p *Parser) parseAdditive() (*ast.ExpressionNode, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.match("+", "-") {
		op := p.tokens[p.pos-1].Value
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &ast.ExpressionNode{
			Type:     "BINARY",
			Left:     left,
			Operator: op,
			Right:    right,
			Position: left.Position,
		}
	}
	return left, nil
}

// parseMultiplicative parses: factor (('*' | '/') factor)*
func (p *Parser) parseMultiplicative() (*ast.ExpressionNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for p.match("*", "/", "%") {
		op := p.tokens[p.pos-1].Value
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &ast.ExpressionNode{
			Type:     "BINARY",
			Left:     left,
			Operator: op,
			Right:    right,
			Position: left.Position,
		}
	}
	return left, nil
}

// parsePrimary parses: identifier | number | string | function | '(' expr ')'
func (p *Parser) parsePrimary() (*ast.ExpressionNode, error) {
	tok := p.current()

	// Parenthesized expression
	if p.match("(") {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		return expr, nil
	}

	// CASE WHEN
	if strings.ToUpper(tok.Value) == "CASE" {
		return p.parseCaseWhen()
	}

	// Function call: NAME(args)
	if p.peek(1).Value == "(" {
		return p.parseFunctionCall()
	}

	// Identifier (field reference)
	if tok.Type == lexer.TOKEN_IDENTIFIER {
		p.advance()
		return &ast.ExpressionNode{
			Type:     "FIELD",
			Value:    tok.Value,
			Position: tok.Position,
		}, nil
	}

	// Number
	if tok.Type == lexer.TOKEN_NUMBER {
		p.advance()
		return &ast.ExpressionNode{
			Type:     "LITERAL",
			Value:    tok.Value,
			Position: tok.Position,
		}, nil
	}

	// String
	if tok.Type == lexer.TOKEN_STRING {
		p.advance()
		return &ast.ExpressionNode{
			Type:     "LITERAL",
			Value:    tok.Value,
			Position: tok.Position,
		}, nil
	}

	// Boolean (true/false)
	if tok.Type == lexer.TOKEN_BOOLEAN {
		p.advance()
		return &ast.ExpressionNode{
			Type:     "LITERAL",
			Value:    tok.Value,
			Position: tok.Position,
		}, nil
	}

	// Star (*) for COUNT(*), SELECT *, etc.
	if tok.Value == "*" {
		p.advance()
		return &ast.ExpressionNode{
			Type:     "FIELD",
			Value:    "*",
			Position: tok.Position,
		}, nil
	}

	return nil, p.error("expected expression")
}

// parseFunctionCall parses: FUNCTION(arg1, arg2, ...) (100% TrueAST)
func (p *Parser) parseFunctionCall() (*ast.ExpressionNode, error) {
	nameTok := p.advance()
	p.advance() // consume (

	var args []*ast.ExpressionNode
	for !p.isAtEnd() && p.current().Value != ")" {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		p.match(",")
	}
	p.expect(")")

	return &ast.ExpressionNode{
		Type:         "FUNCTION",
		FunctionName: strings.ToUpper(nameTok.Value),
		FunctionArgs: args,
		Position:     nameTok.Position,
	}, nil
}

// parseCaseWhen parses: CASE WHEN condition THEN value [WHEN...] [ELSE value] END (100% TrueAST)
func (p *Parser) parseCaseWhen() (*ast.ExpressionNode, error) {
	expr := &ast.ExpressionNode{
		Type:     "CASEWHEN",
		Position: p.current().Position,
	}
	p.advance() // consume CASE

	// Parse WHEN clauses
	for strings.ToUpper(p.current().Value) == "WHEN" {
		p.advance() // consume WHEN

		// Parse condition as ConditionNode
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}

		if err := p.expect("THEN"); err != nil {
			return nil, err
		}

		// Parse THEN value as ExpressionNode
		thenExpr, err := p.parseCaseThenValue()
		if err != nil {
			return nil, err
		}

		expr.CaseConditions = append(expr.CaseConditions, &ast.CaseConditionNode{
			Condition: &cond,
			ThenExpr:  thenExpr,
			Position:  cond.Position,
		})
	}

	// Optional ELSE
	if strings.ToUpper(p.current().Value) == "ELSE" {
		p.advance()
		elseExpr, err := p.parseCaseThenValue()
		if err != nil {
			return nil, err
		}
		expr.CaseElse = elseExpr
	}

	// END
	if err := p.expect("END"); err != nil {
		return nil, err
	}

	return expr, nil
}


// parseCaseThenValue parses value after THEN or ELSE in CASE expression
func (p *Parser) parseCaseThenValue() (*ast.ExpressionNode, error) {
	return p.parseExpression()
}

// isWindowFunctionStart checks if current tokens start a window function
func (p *Parser) isWindowFunctionStart() bool {
	cur := strings.ToUpper(p.current().Value)
	
	// Check using mapping (SSOT)
	if mapping.IsWindowFunction(cur) {
		return true
	}
	
	// Check for two-word functions
	if p.pos+1 < len(p.tokens) {
		twoWord := cur + " " + strings.ToUpper(p.peek(1).Value)
		if mapping.IsWindowFunction(twoWord) {
			return true
		}
	}
	
	return false
}

// parseWindowFunctionExpr parses window function as expression (100% TrueAST)
func (p *Parser) parseWindowFunctionExpr() (*ast.ExpressionNode, error) {
	expr := &ast.ExpressionNode{
		Type:     "WINDOW",
		Position: p.current().Position,
	}

	// Get function name using SSOT
	funcName := strings.ToUpper(p.advance().Value)
	if suffix, ok := mapping.GetWindowFunctionSuffix(funcName); ok {
		funcName += " " + strings.ToUpper(p.advance().Value)
		_ = suffix // validate matches expected
	}
	expr.FunctionName = funcName

	// Optional field for LAG/LEAD using SSOT
	if mapping.WindowFunctionHasField(funcName) {
		if !p.isAtEnd() && strings.ToUpper(p.current().Value) != "OVER" {
			fieldTok := p.advance()
			expr.FunctionArgs = append(expr.FunctionArgs, &ast.ExpressionNode{
				Type:     "FIELD",
				Value:    fieldTok.Value,
				Position: fieldTok.Position,
			})
		}
	}

	// NTILE requires bucket count using SSOT
	if mapping.WindowFunctionHasBuckets(funcName) {
		if !p.isAtEnd() && strings.ToUpper(p.current().Value) != "OVER" {
			bucketTok := p.advance()
			// Store as int in WindowBuckets
			// For now, also store as FunctionArg for compatibility
			expr.FunctionArgs = append(expr.FunctionArgs, &ast.ExpressionNode{
				Type:     "LITERAL",
				Value:    bucketTok.Value,
				Position: bucketTok.Position,
			})
		}
	}

	// Expect OVER
	if err := p.expect("OVER"); err != nil {
		return nil, err
	}

	// Expect (
	if err := p.expect("("); err != nil {
		return nil, err
	}

	// Parse PARTITION BY and ORDER BY inside parentheses (100% TrueAST)
	for !p.isAtEnd() && p.current().Value != ")" {
		curUpper := strings.ToUpper(p.current().Value)

		// PARTITION BY
		if curUpper == "PARTITION BY" || curUpper == "PARTITION" {
			p.advance()
			if curUpper == "PARTITION" && strings.ToUpper(p.current().Value) == "BY" {
				p.advance()
			}
			for !p.isAtEnd() {
				cur := strings.ToUpper(p.current().Value)
				if cur == "ORDER" || cur == "ORDER BY" || p.current().Value == ")" {
					break
				}
				fieldTok := p.advance()
				expr.PartitionBy = append(expr.PartitionBy, &ast.ExpressionNode{
					Type:     "FIELD",
					Value:    fieldTok.Value,
					Position: fieldTok.Position,
				})
				p.match(",")
			}
			continue
		}

		// ORDER BY
		if curUpper == "ORDER BY" || curUpper == "ORDER" {
			p.advance()
			if curUpper == "ORDER" && strings.ToUpper(p.current().Value) == "BY" {
				p.advance()
			}
			for !p.isAtEnd() && p.current().Value != ")" {
				fieldTok := p.advance()
				dir := "ASC"
				if p.match("DESC") {
					dir = "DESC"
				} else {
					p.match("ASC")
				}
				expr.WindowOrderBy = append(expr.WindowOrderBy, ast.OrderByNode{
					FieldExpr: &ast.ExpressionNode{
						Type:     "FIELD",
						Value:    fieldTok.Value,
						Position: fieldTok.Position,
					},
					Direction: dir,
					Position:  fieldTok.Position,
				})
				p.match(",")
			}
			continue
		}

		// Unknown token - break to avoid infinite loop
		break
	}

	// Consume )
	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return expr, nil
}