package parser

import (
	"strings"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/ast"
	"github.com/omniql-engine/omniql/engine/lexer"
)

// =============================================================================
// HELPER: ExpressionNode Creators (DRY)
// =============================================================================

func makeFieldExpr(value string, position int) *ast.ExpressionNode {
	return &ast.ExpressionNode{
		Type:     "FIELD",
		Value:    value,
		Position: position,
	}
}

func makeLiteralExpr(value string, position int) *ast.ExpressionNode {
	return &ast.ExpressionNode{
		Type:     "LITERAL",
		Value:    value,
		Position: position,
	}
}

// =============================================================================
// HELPER PARSERS
// =============================================================================

// parseFieldList parses: * | field1, field2, ... (100% TrueAST)
func (p *Parser) parseFieldList() ([]*ast.ExpressionNode, error) {
	var fields []*ast.ExpressionNode

	if p.current().Value == "*" {
		tok := p.advance()
		return []*ast.ExpressionNode{makeFieldExpr("*", tok.Position)}, nil
	}

	for {
		tok := p.current()
		field, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		fields = append(fields, makeFieldExpr(field, tok.Position))

		if !p.match(",") {
			break
		}
	}

	return fields, nil
}

// parseIdentifierListAsExpressions parses: id1, id2, ... as []*ExpressionNode
func (p *Parser) parseIdentifierListAsExpressions() ([]*ast.ExpressionNode, error) {
	var result []*ast.ExpressionNode

	for {
		if p.current().Type != lexer.TOKEN_IDENTIFIER {
			break
		}

		tok := p.current()
		id, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		result = append(result, makeFieldExpr(id, tok.Position))

		if !p.match(",") {
			break
		}
	}

	return result, nil
}

// parseFieldAssignments parses: field:value, field2:value2, ... (100% TrueAST)
// Also handles expressions: field = value + 10, field = UPPER(name)
func (p *Parser) parseFieldAssignments() ([]ast.FieldNode, error) {
	var fields []ast.FieldNode

	for {
		tok := p.advance()

		// Check if it's "field:value" combined or separate tokens
		if strings.Contains(tok.Value, ":") {
			parts := strings.SplitN(tok.Value, ":", 2)
			fields = append(fields, ast.FieldNode{
				NameExpr:  makeFieldExpr(parts[0], tok.Position),
				ValueExpr: makeLiteralExpr(parts[1], tok.Position),
				Position:  tok.Position,
			})
		} else {
			// Separate tokens: field : value or field = value
			name := tok.Value
			if !p.match(":", "=") {
				return nil, p.error("expected ':' or '=' after field name")
			}

			// Parse the value/expression
			field, err := p.parseFieldValue(name, tok.Position)
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}

		if !p.match(",") {
			break
		}
	}

	return fields, nil
}

// parseFieldValue parses a field value, detecting expressions (100% TrueAST)
func (p *Parser) parseFieldValue(name string, pos int) (ast.FieldNode, error) {
	field := ast.FieldNode{
		NameExpr: makeFieldExpr(name, pos),
		Position: pos,
	}

	// Check for CASE WHEN
	if strings.ToUpper(p.current().Value) == "CASE" {
		expr, err := p.parseCaseExpression()
		if err != nil {
			return field, err
		}
		field.ValueExpr = expr
		return field, nil
	}

	// Check for function call: UPPER(name), CONCAT(a, b)
	if p.current().Type == lexer.TOKEN_IDENTIFIER && p.pos+1 < len(p.tokens) && p.peek(1).Value == "(" {
		funcTok := p.current()
		funcName := strings.ToUpper(p.advance().Value)
		p.advance() // consume (

		var args []*ast.ExpressionNode
		for !p.isAtEnd() && p.current().Value != ")" {
			argTok := p.current()
			argVal := p.advance().Value
			// Determine if arg is field or literal
			if isFieldName(argVal) {
				args = append(args, makeFieldExpr(argVal, argTok.Position))
			} else {
				args = append(args, makeLiteralExpr(argVal, argTok.Position))
			}
			p.match(",")
		}
		p.expect(")")

		field.ValueExpr = &ast.ExpressionNode{
			Type:         "FUNCTION",
			FunctionName: funcName,
			FunctionArgs: args,
			Position:     funcTok.Position,
		}
		return field, nil
	}

	// Try to parse as expression using the expression parser
	expr, err := p.parseAdditive()
	if err != nil {
		return field, err
	}
	field.ValueExpr = expr

	return field, nil
}

// parseCaseExpression parses: CASE WHEN cond THEN val [WHEN...] [ELSE val] END (100% TrueAST)
func (p *Parser) parseCaseExpression() (*ast.ExpressionNode, error) {
	return p.parseCaseWhen()
}

// isFieldName checks if a string looks like a field name (not a number)
func isFieldName(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// If it starts with a digit or is a quoted string, it's a value
	first := s[0]
	if first >= '0' && first <= '9' {
		return false
	}
	if first == '\'' || first == '"' {
		return false
	}
	return true
}

// parseColumnDefinitions parses: col:TYPE(size), col2:TYPE2:constraint, ... (100% TrueAST)
func (p *Parser) parseColumnDefinitions() ([]ast.FieldNode, error) {
	var columns []ast.FieldNode

	for !p.isAtEnd() {
		tok := p.advance()

		// Stop at clause keywords
		if mapping.IsClause(strings.ToUpper(tok.Value)) {
			p.pos-- // Put it back
			break
		}

		// Token is "name:TYPE" or "name:TYPE:CONSTRAINT"
		parts := strings.Split(tok.Value, ":")
		if len(parts) < 2 {
			return nil, p.error("expected column definition 'name:TYPE'")
		}

		name := parts[0]
		typ := strings.ToUpper(parts[1])

		col := ast.FieldNode{
			NameExpr:  makeFieldExpr(name, tok.Position),
			ValueExpr: makeLiteralExpr(typ, tok.Position),
			Position:  tok.Position,
		}

		// Constraints after second colon (parts[2], parts[3], ...)
		for i := 2; i < len(parts); i++ {
			col.Constraints = append(col.Constraints, strings.ToUpper(parts[i]))
		}

		// Check for size in parentheses: STRING(100) or DECIMAL(10,2)
		if p.match("(") {
			var sizeParts []string
			for !p.isAtEnd() && p.current().Value != ")" {
				sizeParts = append(sizeParts, p.advance().Value)
				p.match(",")
			}
			p.expect(")")
			col.ValueExpr = makeLiteralExpr(typ+"("+strings.Join(sizeParts, ",")+")", tok.Position)
		}

		columns = append(columns, col)

		if !p.match(",") {
			break
		}
	}

	return columns, nil
}