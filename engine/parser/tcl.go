package parser

import (
	"github.com/omniql-engine/omniql/engine/ast"
)

// =============================================================================
// TCL DISPATCHER & PARSER
// =============================================================================

func (p *Parser) parseTCL(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
		Transaction: &ast.TransactionNode{
			Operation: op,
			Position:  p.current().Position,
		},
	}
	p.advance() // consume operation

	// BEGIN and START are aliases
	if op == "BEGIN" || op == "START" {
		// No additional parsing needed
		return node, nil
	}

	// SAVEPOINT name
	if op == "SAVEPOINT" {
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Transaction.SavepointName = name
	}

	// ROLLBACK TO name
	if op == "ROLLBACK TO" {
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Transaction.SavepointName = name
	}

	// RELEASE SAVEPOINT name
	if op == "RELEASE SAVEPOINT" {
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Transaction.SavepointName = name
	}

	// SET TRANSACTION ISOLATION LEVEL ...
	if op == "SET TRANSACTION" {
		if p.match("ISOLATION") {
			p.expect("LEVEL")
			level, err := p.expectIdentifier()
			if err != nil {
				return nil, err
			}
			node.Transaction.IsolationLevel = level
		}
	}

	return node, nil
}