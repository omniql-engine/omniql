package parser

import (
	"strconv"
	"strings"
	"fmt"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/ast"
	"github.com/omniql-engine/omniql/engine/lexer"
)

// parseClauses parses optional clauses after main statement
// Uses mapping.QueryClauses as SSOT for clause recognition
func (p *Parser) parseClauses(node *ast.QueryNode) error {
	for !p.isAtEnd() {
		clause := strings.ToUpper(p.current().Value)

		// Check two-word clauses first (ORDER BY, GROUP BY, etc.)
		if p.pos+1 < len(p.tokens) {
			twoWord := clause + " " + strings.ToUpper(p.peek(1).Value)
			if mapping.IsClause(twoWord) {
				clause = twoWord
			}
		}

		// Validate against mapping (SSOT)
		if !mapping.IsClause(clause) {
			// Check if it looks like a typo of a clause keyword
			if p.current().Type == lexer.TOKEN_IDENTIFIER {
				suggestion := lexer.SuggestSimilar(p.current().Value)
				if suggestion != "" && mapping.IsClause(suggestion) {
					return p.error(fmt.Sprintf("unknown keyword '%s'. Did you mean '%s'?", p.current().Value, suggestion))
				}
			}
			return nil // Unknown clause - stop parsing clauses
		}

		// Dispatch based on clause - validated above via SSOT
		switch clause {
		case "WHERE":
			if err := p.parseWhereClause(node); err != nil {
				return err
			}
		case "ORDER BY":
			if err := p.parseOrderByClause(node); err != nil {
				return err
			}
		case "GROUP BY":
			if err := p.parseGroupByClause(node); err != nil {
				return err
			}
		case "HAVING":
			if err := p.parseHavingClause(node); err != nil {
				return err
			}
		case "LIMIT":
			if err := p.parseLimitClause(node); err != nil {
				return err
			}
		case "OFFSET":
			if err := p.parseOffsetClause(node); err != nil {
				return err
			}
		case "DISTINCT":
			if err := p.parseDistinctClause(node); err != nil {
				return err
			}
		case "WITH":
			// WITH in GET context = SELECT expressions
			if node.Operation == "GET" {
				if err := p.parseSelectExpressions(node); err != nil {
					return err
				}
			} else {
				return nil
			}
		}
	}
	return nil
}

// parseWhereClause parses: WHERE condition [AND|OR condition]*
func (p *Parser) parseWhereClause(node *ast.QueryNode) error {
	p.advance() // consume WHERE
	conditions, err := p.parseConditions()
	if err != nil {
		return err
	}
	node.Conditions = &ast.WhereNode{
		Conditions: conditions,
		Position:   p.current().Position,
	}
	return nil
}

// parseOrderByClause parses: ORDER BY field [ASC|DESC], ... (100% TrueAST)
func (p *Parser) parseOrderByClause(node *ast.QueryNode) error {
	cur := strings.ToUpper(p.current().Value)
	p.advance() // consume ORDER BY (or just ORDER)
	// If separate tokens, consume BY as well
	if cur == "ORDER" && strings.ToUpper(p.current().Value) == "BY" {
		p.advance()
	}

	for !p.isAtEnd() {
		// Check if we hit another clause or EOF
		if p.current().Type == lexer.TOKEN_EOF {
			break
		}
		curUpper := strings.ToUpper(p.current().Value)
		if mapping.IsClause(curUpper) && curUpper != "ASC" && curUpper != "DESC" {
			break
		}
		// Stop at aggregate keywords
		if mapping.IsAggregate(curUpper) {
			break
		}

		// Parse field as expression (100% TrueAST)
		fieldTok := p.current()
		field, err := p.expectIdentifier()
		if err != nil {
			return err
		}

		order := ast.OrderByNode{
			Direction: "ASC", // default
			Position:  fieldTok.Position,
		}

		// Handle colon syntax: field:ASC or field:DESC
		if strings.Contains(field, ":") {
			parts := strings.SplitN(field, ":", 2)
			order.FieldExpr = &ast.ExpressionNode{
				Type:     "FIELD",
				Value:    parts[0],
				Position: fieldTok.Position,
			}
			if strings.ToUpper(parts[1]) == "DESC" {
				order.Direction = "DESC"
			} else if strings.ToUpper(parts[1]) == "ASC" {
				order.Direction = "ASC"
			}
		} else {
			order.FieldExpr = &ast.ExpressionNode{
				Type:     "FIELD",
				Value:    field,
				Position: fieldTok.Position,
			}
			if p.match("ASC") {
				order.Direction = "ASC"
			} else if p.match("DESC") {
				order.Direction = "DESC"
			}
		}

		node.OrderBy = append(node.OrderBy, order)

		if !p.match(",") {
			break
		}
	}
	return nil
}

// parseGroupByClause parses: GROUP BY field, ... (100% TrueAST)
func (p *Parser) parseGroupByClause(node *ast.QueryNode) error {
	cur := strings.ToUpper(p.current().Value)
	p.advance() // consume GROUP BY (or just GROUP)
	// If separate tokens, consume BY as well
	if cur == "GROUP" && strings.ToUpper(p.current().Value) == "BY" {
		p.advance()
	}

	for !p.isAtEnd() {
		// Check if we hit another clause or EOF
		if p.current().Type == lexer.TOKEN_EOF {
			break
		}
		curUpper := strings.ToUpper(p.current().Value)
		if mapping.IsClause(curUpper) {
			break
		}
		// Stop at aggregate keywords
		if mapping.IsAggregate(curUpper) {
			break
		}

		fieldTok := p.current()
		field, err := p.expectIdentifier()
		if err != nil {
			return err
		}
		
		// 100% TrueAST
		node.GroupBy = append(node.GroupBy, &ast.ExpressionNode{
			Type:     "FIELD",
			Value:    field,
			Position: fieldTok.Position,
		})

		if !p.match(",") {
			break
		}
	}
	return nil
}

// parseHavingClause parses: HAVING condition [AND|OR condition]*
func (p *Parser) parseHavingClause(node *ast.QueryNode) error {
	p.advance() // consume HAVING

	conditions, err := p.parseConditions()
	if err != nil {
		return err
	}
	node.Having = conditions
	return nil
}

// parseLimitClause parses: LIMIT number
func (p *Parser) parseLimitClause(node *ast.QueryNode) error {
	p.advance() // consume LIMIT

	tok := p.advance()
	val, err := strconv.Atoi(tok.Value)
	if err != nil {
		return p.error("LIMIT requires integer")
	}
	node.Limit = &val
	return nil
}

// parseOffsetClause parses: OFFSET number
func (p *Parser) parseOffsetClause(node *ast.QueryNode) error {
	p.advance() // consume OFFSET

	tok := p.advance()
	val, err := strconv.Atoi(tok.Value)
	if err != nil {
		return p.error("OFFSET requires integer")
	}
	node.Offset = &val
	return nil
}

// parseDistinctClause parses: DISTINCT [column, ...] (100% TrueAST)
func (p *Parser) parseDistinctClause(node *ast.QueryNode) error {
	p.advance() // consume DISTINCT
	node.Distinct = true

	// Track if we found any columns
	var distinctColumns []*ast.ExpressionNode

	// Parse optional columns after DISTINCT
	for !p.isAtEnd() {
		cur := strings.ToUpper(p.current().Value)
		// Check two-word clauses first
		if p.pos+1 < len(p.tokens) {
			twoWord := cur + " " + strings.ToUpper(p.peek(1).Value)
			if mapping.IsTerminatingClause(twoWord) {
				break
			}
		}
		if mapping.IsClause(cur) {
			break
		}
		// Collect column as ExpressionNode
		colTok := p.current()
		col, err := p.expectIdentifier()
		if err != nil {
			return err
		}
		distinctColumns = append(distinctColumns, &ast.ExpressionNode{
			Type:     "FIELD",
			Value:    col,
			Position: colTok.Position,
		})
		if !p.match(",") {
			break
		}
	}

	// If DISTINCT has specific columns, replace default *
	if len(distinctColumns) > 0 {
		node.Columns = distinctColumns
	}

	return nil
}

// looksLikeGroupedCondition checks if ( starts a grouped condition like (a = 1 OR b = 2)
// vs an arithmetic expression like (price - cost) * quantity
func (p *Parser) looksLikeGroupedCondition() bool {
	if p.pos+2 >= len(p.tokens) {
		return false
	}
	
	// Token after ( 
	tok1 := p.peek(1)
	
	// If nested paren, assume grouped condition
	if tok1.Value == "(" {
		return true
	}
	
	// Token after the identifier
	tok2 := p.peek(2)
	
	// If comparison operator follows, it's a grouped condition
	if isComparisonOperator(strings.ToUpper(tok2.Value)) {
		return true
	}
	
	// Otherwise it's an arithmetic expression
	return false
}

// parseConditions parses: condition [AND|OR condition]*
// Also handles grouped conditions: (condition OR condition)
// Logic convention: Logic field on condition N indicates how to JOIN it to previous
// Example: "a = 1 OR b = 2" -> cond[0].Logic="", cond[1].Logic="OR"
func (p *Parser) parseConditions() ([]ast.ConditionNode, error) {
	var conditions []ast.ConditionNode
	var pendingLogic string // Logic to apply to NEXT condition

	for {
		// Check for grouped conditions: (...) - wrap in Nested
		// But NOT arithmetic expressions like (price - cost) * quantity
		if p.current().Value == "(" && p.looksLikeGroupedCondition() {
			p.advance() // consume (
			
			// Parse conditions inside parentheses
			nested, err := p.parseConditions()
			if err != nil {
				return nil, err
			}
			
			if err := p.expect(")"); err != nil {
				return nil, err
			}
			
			// Create wrapper node with nested conditions
			wrapper := ast.ConditionNode{
				Operator: "GROUP",
				Nested:   nested,
				Position: p.current().Position,
			}
			
			// Apply pending logic to wrapper
			if pendingLogic != "" {
				wrapper.Logic = pendingLogic
				pendingLogic = ""
			}
			
			conditions = append(conditions, wrapper)
		} else {
			cond, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			
			// Apply pending logic to this condition
			if pendingLogic != "" {
				cond.Logic = pendingLogic
				pendingLogic = ""
			}
			
			conditions = append(conditions, cond)
		}

		// Check for AND/OR - save for NEXT condition
		if p.match("AND") {
			pendingLogic = "AND"
			continue
		}
		if p.match("OR") {
			pendingLogic = "OR"
			continue
		}
		break
	}
	return conditions, nil
}

// parseConditionSide parses one side of a condition as ExpressionNode
// Uses parseAdditive to avoid consuming AND/OR which are condition separators
func (p *Parser) parseConditionSide() (*ast.ExpressionNode, error) {
	return p.parseAdditive()
}

// parseComparisonOperator parses comparison operator including multi-word
func (p *Parser) parseComparisonOperator() string {
	op := strings.ToUpper(p.current().Value)
	p.advance()

	// Handle multi-word operators: NOT IN, NOT LIKE, NOT BETWEEN
	if op == "NOT" {
		next := strings.ToUpper(p.current().Value)
		if mapping.IsComparisonOperator(next) {
			op = "NOT_" + next
			p.advance()
		}
	}

	// Handle IS NULL, IS NOT NULL
	if op == "IS" {
		if p.match("NOT") {
			op = "IS_NOT_NULL"
			p.match("NULL")
		} else if p.match("NULL") {
			op = "IS_NULL"
		}
	}

	return op
}

// validateOperator checks if operator is valid and returns error with suggestion if not
func (p *Parser) validateOperator(op string) error {
	// Check if it's a valid comparison operator
	if mapping.IsComparisonOperator(op) {
		return nil
	}
	// Check for typo suggestion
	suggestion := lexer.SuggestSimilar(op)
	if suggestion != "" && mapping.IsComparisonOperator(suggestion) {
		return p.error(fmt.Sprintf("unknown operator '%s'. Did you mean '%s'?", op, suggestion))
	}
	return nil // Let it pass - might be handled elsewhere
}

// isComparisonOperator checks if token is a comparison operator
func isComparisonOperator(val string) bool {
	return mapping.IsComparisonOperator(val)
}

// isArithmeticOperator checks if token is arithmetic (not comparison)
func isArithmeticOperator(val string) bool {
	return mapping.IsArithmeticOperator(val)
}

// parseCondition parses: expression operator value (100% TrueAST)
func (p *Parser) parseCondition() (ast.ConditionNode, error) {
	cond := ast.ConditionNode{Position: p.current().Position}

	// Parse left side as ExpressionNode
	leftExpr, err := p.parseConditionSide()
	if err != nil {
		return cond, err
	}
	cond.FieldExpr = leftExpr

	// Check for end or logic operator (no comparison)
	if p.isAtEnd() {
		return cond, nil
	}
	curUpper := strings.ToUpper(p.current().Value)
	if curUpper == "AND" || curUpper == "OR" || p.current().Value == ")" {
		return cond, nil
	}
	if mapping.IsTerminatingClause(curUpper) {
		return cond, nil
	}

	// Parse comparison operator
	cond.Operator = p.parseComparisonOperator()

	// Validate operator (check for typos)
	if err := p.validateOperator(cond.Operator); err != nil {
		return cond, err
	}

	// Parse right side based on operator category (SSOT)
	switch mapping.GetOperatorCategory(cond.Operator) {
	case "MULTI_VALUE":
		cond.ValuesExpr, err = p.parseInValues()
	case "RANGE":
		cond.ValueExpr, cond.Value2Expr, err = p.parseBetweenValues()
	case "NULLCHECK":
		// No value needed
	default:
		cond.ValueExpr, err = p.parseConditionSide()
	}

	return cond, err
}

// parseInValues parses: (val1, val2, val3) as []*ExpressionNode
func (p *Parser) parseInValues() ([]*ast.ExpressionNode, error) {
	var values []*ast.ExpressionNode

	if err := p.expect("("); err != nil {
		return nil, err
	}

	for !p.isAtEnd() && p.current().Value != ")" {
		expr, err := p.parseConditionSide()
		if err != nil {
			return nil, err
		}
		values = append(values, expr)
		p.match(",")
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return values, nil
}

// parseBetweenValues parses: val1 AND val2 as two ExpressionNodes
func (p *Parser) parseBetweenValues() (*ast.ExpressionNode, *ast.ExpressionNode, error) {
	// First value
	val1, err := p.parseBetweenSingleValue()
	if err != nil {
		return nil, nil, err
	}

	// Expect AND
	if err := p.expect("AND"); err != nil {
		return nil, nil, err
	}

	// Second value
	val2, err := p.parseBetweenSingleValue()
	if err != nil {
		return nil, nil, err
	}

	return val1, val2, nil
}

// parseBetweenSingleValue parses single value for BETWEEN (stops at AND) (100% TrueAST)
func (p *Parser) parseBetweenSingleValue() (*ast.ExpressionNode, error) {
	return p.parseConditionSide()
}

// parseSelectExpressions parses: WITH expr AS alias, expr2 AS alias2, ... (100% TrueAST)
func (p *Parser) parseSelectExpressions(node *ast.QueryNode) error {
	p.advance() // consume WITH

	for !p.isAtEnd() {
		col := ast.SelectColumnNode{
			Position: p.current().Position,
		}

		// Check if this is "alias = expression" format
		if p.pos+1 < len(p.tokens) && p.peek(1).Value == "=" {
			alias, err := p.expectIdentifier()
			if err != nil {
				return err
			}
			col.Alias = alias
			p.advance() // consume =
		}

		curUpper := strings.ToUpper(p.current().Value)

		// Check for CASE WHEN
		if curUpper == "CASE" {
			expr, err := p.parseCaseWhen()
			if err != nil {
				return err
			}
			col.ExpressionObj = expr
		} else if p.isWindowFunctionStart() {
			// Window function: ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
			expr, err := p.parseWindowFunctionExpr()
			if err != nil {
				return err
			}
			col.ExpressionObj = expr
		} else {
			// Regular expression - parse using expression parser (100% TrueAST)
			expr, err := p.parseSelectExpression()
			if err != nil {
				return err
			}
			col.ExpressionObj = expr
		}

	
		// Optional AS alias (only if not already set via = format)
		if col.Alias == "" && p.match("AS") {
			alias, err := p.expectIdentifier()
			if err != nil {
				return err
			}
			col.Alias = alias
		}

		node.SelectColumns = append(node.SelectColumns, col)

		if !p.match(",") {
			break
		}
	}

	return nil
}

// parseSelectExpression parses expression in SELECT until AS, comma, or terminating clause (100% TrueAST)
func (p *Parser) parseSelectExpression() (*ast.ExpressionNode, error) {
	// Use parseAdditive which handles +, -, *, /, %
	// It stops at non-arithmetic tokens naturally
	return p.parseAdditive()
}