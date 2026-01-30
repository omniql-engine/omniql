package parser

import (
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/engine/ast"
	"github.com/omniql-engine/omniql/engine/lexer"
)

// =============================================================================
// DQL DISPATCHER
// =============================================================================

func (p *Parser) parseDQL(op string) (*ast.QueryNode, error) {
	switch op {
	// Aggregates
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return p.parseAggregate(op)
	// Joins
	case "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN":
		return p.parseJoin(op)
	// Set operations
	case "UNION", "UNION ALL", "INTERSECT", "EXCEPT":
		return p.parseSetOperation(op)
	// Window functions
	case "ROW NUMBER", "RANK", "DENSE RANK", "LAG", "LEAD", "NTILE":
		return p.parseWindowFunction(op)
	// Advanced queries
	case "CTE":
		return p.parseCTE()
	case "SUBQUERY":
		return p.parseSubquery()
	case "EXISTS":
		return p.parseExists()
	case "CASE":
		return p.parseCase()
	default:
		return nil, p.error("unimplemented DQL: " + op)
	}
}

// =============================================================================
// DQL PARSERS
// =============================================================================

// COUNT|SUM|AVG|MIN|MAX field FROM entity [WHERE ...]
func (p *Parser) parseAggregate(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
		Aggregate: &ast.AggregateNode{
			Function: op,
			Position: p.current().Position,
		},
	}
	p.advance() // consume aggregate function

	// Optional field (COUNT can be COUNT *)
	if p.current().Value != "FROM" && p.current().Value != "*" {
		pos := p.current().Position
		field, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Aggregate.FieldExpr = makeFieldExpr(field, pos)
	}
	if p.match("*") {
		node.Aggregate.FieldExpr = makeFieldExpr("*", p.current().Position)
	}

	// FROM
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// Optional clauses
	if err := p.parseClauses(node); err != nil {
		return nil, err
	}

	return node, nil
}

// INNER JOIN|LEFT JOIN|... entity1 entity2 ON field1 = field2
func (p *Parser) parseJoin(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
	}
	p.advance() // consume JOIN operation

	join := ast.JoinNode{
		Type:     strings.Split(op, " ")[0], // INNER, LEFT, etc.
		Position: p.current().Position,
	}

	// First table
	table1, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table1

	// Second table
	table2, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	join.Table = table2


	// ON - required for all JOINs except CROSS
	if join.Type != "CROSS" {
		if err := p.expect("ON"); err != nil {
			return nil, err
		}
		// Parse ON condition using parseCondition for expression support (100% TrueAST)
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		join.LeftExpr = cond.FieldExpr
		join.RightExpr = cond.ValueExpr
	}

	node.Joins = append(node.Joins, join)

	// Optional clauses (WHERE, ORDER BY, LIMIT, etc.)
	if err := p.parseClauses(node); err != nil {
		return nil, err
	}

	return node, nil
}

// UNION|UNION ALL|INTERSECT|EXCEPT (query1) (query2)
// Format: UNION (GET User WHERE age > 25) (GET User WHERE status = 'active')
func (p *Parser) parseSetOperation(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
		SetOperation: &ast.SetOperationNode{
			Type:     op,
			Position: p.current().Position,
		},
	}
	p.advance() // consume set operation

	// Parse left query in parentheses
	if err := p.expect("("); err != nil {
		return nil, p.error("SET operation requires (query1) (query2) format")
	}

	leftQuery, err := p.parseNested()
	if err != nil {
		return nil, err
	}
	node.SetOperation.LeftQuery = leftQuery

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	// Parse right query in parentheses
	if err := p.expect("("); err != nil {
		return nil, p.error("SET operation requires second query in parentheses")
	}

	rightQuery, err := p.parseNested()
	if err != nil {
		return nil, err
	}
	node.SetOperation.RightQuery = rightQuery

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return node, nil
}

// =============================================================================
// WINDOW FUNCTIONS
// =============================================================================

// ROW NUMBER|RANK|DENSE RANK|LAG|LEAD|NTILE OVER (PARTITION BY field ORDER BY field)
func (p *Parser) parseWindowFunction(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
	}
	p.advance() // consume window function name

	window := ast.WindowNode{
		Function: op,
		Position: p.current().Position,
	}

	// Optional field for LAG/LEAD (100% TrueAST)
	if op == "LAG" || op == "LEAD" {
		if p.current().Type == lexer.TOKEN_IDENTIFIER {
			pos := p.current().Position
			field, _ := p.expectIdentifier()
			window.FieldExpr = makeFieldExpr(field, pos)
		}
	}

	// NTILE requires bucket count
	if op == "NTILE" {
		if p.current().Type == lexer.TOKEN_NUMBER {
			tok := p.advance()
			buckets, _ := strconv.Atoi(tok.Value)
			window.Buckets = buckets
		}
	}

	// OVER clause
	if p.match("OVER") {
		if err := p.expect("("); err != nil {
			return nil, err
		}

		// PARTITION BY (100% TrueAST)
		if p.match("PARTITION") {
			p.expect("BY")
			for !p.isAtEnd() {
				if strings.ToUpper(p.current().Value) == "ORDER" || p.current().Value == ")" {
					break
				}
				pos := p.current().Position
				field, err := p.expectIdentifier()
				if err != nil {
					return nil, err
				}
				window.PartitionBy = append(window.PartitionBy, makeFieldExpr(field, pos))
				p.match(",")
			}
		}

		// ORDER BY (100% TrueAST)
		if p.match("ORDER") {
			p.expect("BY")
			for !p.isAtEnd() && p.current().Value != ")" {
				pos := p.current().Position
				field, err := p.expectIdentifier()
				if err != nil {
					return nil, err
				}
				order := ast.OrderByNode{
					FieldExpr: makeFieldExpr(field, pos),
					Direction: "ASC",
					Position:  pos,
				}
				if p.match("ASC") {
					order.Direction = "ASC"
				} else if p.match("DESC") {
					order.Direction = "DESC"
				}
				window.OrderBy = append(window.OrderBy, order)
				p.match(",")
			}
		}

		p.expect(")")
	}

	node.WindowFunctions = append(node.WindowFunctions, window)

	// FROM entity
	if p.match("FROM") {
		entity, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Entity = entity
	}

	return node, nil
}

// =============================================================================
// ADVANCED QUERIES
// =============================================================================

// CTE: WITH name AS (query)
// Format: CTE temp_users AS (GET User WHERE active = true)
func (p *Parser) parseCTE() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CTE",
		Position:  p.current().Position,
	}
	p.advance() // consume CTE/WITH

	// CTE name
	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	// Parse CTE query in parentheses (100% TrueAST)
	if err := p.expect("("); err != nil {
		return nil, err
	}

	cteQuery, err := p.parseNested()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = cteQuery

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return node, nil
}

// SUBQUERY field IN (query)
// Format: SUBQUERY id IN (GET User WHERE active = true)
func (p *Parser) parseSubquery() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "SUBQUERY",
		Position:  p.current().Position,
	}
	p.advance() // consume SUBQUERY

	// Field that will be matched
	fieldTok := p.current()
	field, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Columns = []*ast.ExpressionNode{makeFieldExpr(field, fieldTok.Position)}

	// IN keyword
	if err := p.expect("IN"); err != nil {
		return nil, err
	}

	// Parse subquery in parentheses (100% TrueAST)
	if err := p.expect("("); err != nil {
		return nil, err
	}

	subQuery, err := p.parseNested()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = subQuery

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return node, nil
}

// EXISTS (query)
// Format: EXISTS (GET User WHERE id = 5)
func (p *Parser) parseExists() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "EXISTS",
		Position:  p.current().Position,
	}
	p.advance() // consume EXISTS

	// Parse query in parentheses (100% TrueAST)
	if err := p.expect("("); err != nil {
		return nil, err
	}

	existsQuery, err := p.parseNested()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = existsQuery

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return node, nil
}

// CASE - standalone CASE is not valid, use within GET expressions
// Format: GET User WITH CASE WHEN age > 25 THEN 'adult' ELSE 'minor' END AS category
func (p *Parser) parseCase() (*ast.QueryNode, error) {
	return nil, p.error("CASE must be used within GET expression: GET Entity WITH CASE WHEN ... END AS alias")
}