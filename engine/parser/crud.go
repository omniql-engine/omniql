package parser

import (
	"strings"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/ast"
)

// =============================================================================
// CRUD DISPATCHER
// =============================================================================

func (p *Parser) parseCRUD(op string) (*ast.QueryNode, error) {
	switch op {
	case "GET":
		return p.parseGet()
	case "CREATE":
		return p.parseCreate()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "UPSERT":
		return p.parseUpsert()
	case "BULK INSERT":
		return p.parseBulkInsert()
	case "REPLACE":
		return p.parseReplace()
	default:
		return nil, p.error("unimplemented CRUD: " + op)
	}
}

// =============================================================================
// CRUD PARSERS (100% TrueAST)
// =============================================================================

func (p *Parser) parseGet() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "GET",
		Position:  p.current().Position,
	}
	p.advance() // consume GET

	// Check if next token is FROM or a clause (WHERE, ORDER, LIMIT)
	// If so, current token is the entity
	// Otherwise, it could be fields or entity

	firstTok := p.current()
	first, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	// Check what comes next
	nextUpper := strings.ToUpper(p.current().Value)

	// If FROM follows, first was field list start
	if nextUpper == "FROM" {
		// Parse remaining fields if comma
		node.Columns = []*ast.ExpressionNode{makeFieldExpr(first, firstTok.Position)}
		// (fields already parsed as single, FROM is next)
		p.advance() // consume FROM

		entity, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Entity = entity
	} else if nextUpper == "WHERE" || nextUpper == "ORDER" || nextUpper == "LIMIT" || nextUpper == "GROUP" || p.isAtEnd() {
		// No FROM - first token is the entity
		node.Entity = first
		node.Columns = []*ast.ExpressionNode{makeFieldExpr("*", firstTok.Position)} // default to all columns
	} else if p.current().Value == "," {
		// Multiple fields: GET field1, field2 FROM entity
		node.Columns = []*ast.ExpressionNode{makeFieldExpr(first, firstTok.Position)}
		for p.match(",") {
			fieldTok := p.current()
			field, err := p.expectIdentifier()
			if err != nil {
				return nil, err
			}
			node.Columns = append(node.Columns, makeFieldExpr(field, fieldTok.Position))
		}
		if err := p.expect("FROM"); err != nil {
			return nil, err
		}
		entity, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Entity = entity
	} else {
		// Assume first is entity
		node.Entity = first
		node.Columns = []*ast.ExpressionNode{makeFieldExpr("*", firstTok.Position)}
	}

	// Check for aggregate BEFORE clauses (GET User COUNT WHERE id = 1)
	if !p.isAtEnd() {
		aggUpper := strings.ToUpper(p.current().Value)
		if mapping.IsAggregate(aggUpper) {
			aggTok := p.current()
			p.advance()

			node.Operation = aggUpper
			node.Aggregate = &ast.AggregateNode{
				Function: aggUpper,
				Position: aggTok.Position,
			}

			// Check for DISTINCT modifier
			if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "DISTINCT" {
				p.advance()
				node.Distinct = true
			}

			// Optional field for aggregate (e.g., SUM amount)
			if !p.isAtEnd() {
				curUpper := strings.ToUpper(p.current().Value)
				twoWord := ""
				if p.pos+1 < len(p.tokens) {
					twoWord = curUpper + " " + strings.ToUpper(p.peek(1).Value)
				}
				if !mapping.IsClause(curUpper) && !mapping.IsClause(twoWord) {
					fieldTok := p.current()
					fieldVal := p.advance().Value
					node.Aggregate.FieldExpr = makeFieldExpr(fieldVal, fieldTok.Position)
				}
			}
		}
	}

	// Optional clauses (WHERE, ORDER BY, LIMIT, GROUP BY, HAVING)
	if err := p.parseClauses(node); err != nil {
		return nil, err
	}

	return node, nil
}

// CREATE entity WITH field:value, ...
func (p *Parser) parseCreate() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE

	// Entity
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// WITH
	if err := p.expect("WITH"); err != nil {
		return nil, err
	}

	// Fields
	fields, err := p.parseFieldAssignments()
	if err != nil {
		return nil, err
	}
	node.Fields = fields

	return node, nil
}

// UPDATE entity SET field:value, ... WHERE ...
func (p *Parser) parseUpdate() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "UPDATE",
		Position:  p.current().Position,
	}
	p.advance() // consume UPDATE

	// Entity
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// SET
	if err := p.expect("SET"); err != nil {
		return nil, err
	}

	// Fields
	fields, err := p.parseFieldAssignments()
	if err != nil {
		return nil, err
	}
	node.Fields = fields

	// Optional WHERE
	if err := p.parseClauses(node); err != nil {
		return nil, err
	}

	return node, nil
}

// DELETE [FROM] entity WHERE ...
func (p *Parser) parseDelete() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DELETE",
		Position:  p.current().Position,
	}
	p.advance() // consume DELETE

	// Optional FROM
	p.match("FROM")

	// Entity
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// Optional WHERE
	if err := p.parseClauses(node); err != nil {
		return nil, err
	}

	return node, nil
}

// UPSERT entity WITH field:value ON conflict_field
func (p *Parser) parseUpsert() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "UPSERT",
		Position:  p.current().Position,
	}
	p.advance() // consume UPSERT

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if err := p.expect("WITH"); err != nil {
		return nil, err
	}

	fields, err := p.parseFieldAssignments()
	if err != nil {
		return nil, err
	}
	node.Fields = fields

	if p.match("ON") {
		node.Upsert = &ast.UpsertNode{Position: p.current().Position}

		// Parse conflict fields as ExpressionNodes
		conflicts, err := p.parseIdentifierListAsExpressions()
		if err != nil {
			return nil, err
		}
		node.Upsert.ConflictFields = conflicts

		// Copy non-conflict fields to UpdateFields
		conflictSet := make(map[string]bool)
		for _, c := range conflicts {
			conflictSet[c.Value] = true
		}
		for _, f := range fields {
			// Get field name from NameExpr
			if f.NameExpr != nil && !conflictSet[f.NameExpr.Value] {
				node.Upsert.UpdateFields = append(node.Upsert.UpdateFields, f)
			}
		}
	}

	return node, nil
}

// BULK INSERT entity WITH [...] [...] ...
// Format: BULK INSERT User WITH [name = Alice, age = 28] [name = Bob, age = 32]
func (p *Parser) parseBulkInsert() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "BULK INSERT",
		Position:  p.current().Position,
	}
	p.advance() // consume BULK INSERT

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if err := p.expect("WITH"); err != nil {
		return nil, err
	}

	// Parse multiple [...] blocks, each is a row
	for !p.isAtEnd() && p.current().Value == "[" {
		p.advance() // consume [

		// Parse fields until ]
		var fields []ast.FieldNode
		for !p.isAtEnd() && p.current().Value != "]" {
			tok := p.advance()

			// Check if combined field:value or separate
			if strings.Contains(tok.Value, ":") {
				parts := strings.SplitN(tok.Value, ":", 2)
				fields = append(fields, ast.FieldNode{
					NameExpr:  makeFieldExpr(parts[0], tok.Position),
					ValueExpr: makeLiteralExpr(parts[1], tok.Position),
					Position:  tok.Position,
				})
			} else {
				name := tok.Value
				if !p.match(":", "=") {
					return nil, p.error("expected ':' or '=' after field name")
				}

				// Consume value until comma or ]
				var valueParts []string
				for !p.isAtEnd() {
					cur := p.current().Value
					if cur == "," || cur == "]" {
						break
					}
					valueParts = append(valueParts, p.advance().Value)
				}

				fields = append(fields, ast.FieldNode{
					NameExpr:  makeFieldExpr(name, tok.Position),
					ValueExpr: makeLiteralExpr(strings.Join(valueParts, " "), tok.Position),
					Position:  tok.Position,
				})
			}
			p.match(",")
		}

		if err := p.expect("]"); err != nil {
			return nil, err
		}

		node.BulkData = append(node.BulkData, fields)
	}

	return node, nil
}

// REPLACE entity WITH field:value
func (p *Parser) parseReplace() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "REPLACE",
		Position:  p.current().Position,
	}
	p.advance() // consume REPLACE

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if err := p.expect("WITH"); err != nil {
		return nil, err
	}

	fields, err := p.parseFieldAssignments()
	if err != nil {
		return nil, err
	}
	node.Fields = fields

	return node, nil
}