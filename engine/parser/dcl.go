package parser

import (
	"strings"
	"github.com/omniql-engine/omniql/engine/ast"
)

// =============================================================================
// DCL DISPATCHER
// =============================================================================

func (p *Parser) parseDCL(op string) (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
		Permission: &ast.PermissionNode{
			Operation: op,
			Position:  p.current().Position,
		},
	}
	p.advance() // consume operation

	switch op {
	case "GRANT", "REVOKE":
		return p.parseGrantRevoke(node, op)
	case "CREATE USER":
		return p.parseCreateUser(node)
	case "DROP USER":
		return p.parseDropUser(node)
	case "ALTER USER":
		return p.parseAlterUser(node)
	case "CREATE ROLE":
		return p.parseCreateRole(node)
	case "DROP ROLE":
		return p.parseDropRole(node)
	case "ASSIGN ROLE":
		return p.parseAssignRole(node)
	case "REVOKE ROLE":
		return p.parseRevokeRole(node)
	default:
		return nil, p.error("unimplemented DCL: " + op)
	}
}

// =============================================================================
// DCL PARSERS
// =============================================================================

// GRANT|REVOKE permissions ON entity TO|FROM target
func (p *Parser) parseGrantRevoke(node *ast.QueryNode, op string) (*ast.QueryNode, error) {
	// Permissions list - parse until ON keyword
	var perms []string
	for !p.isAtEnd() {
		curUpper := strings.ToUpper(p.current().Value)
		// Stop at ON keyword
		if curUpper == "ON" {
			break
		}
		// Get permission name (can be operation keywords like UPDATE, DELETE)
		perms = append(perms, p.advance().Value)
		// Skip comma if present
		p.match(",")
	}
	node.Permission.Permissions = perms

	// ON
	if err := p.expect("ON"); err != nil {
		return nil, err
	}

// Handle * (all) or identifier
	var entity string
	if p.current().Value == "*" {
		entity = "*"
		p.advance()
	} else {
		var err error
		entity, err = p.expectIdentifier()
		if err != nil {
			return nil, err
		}
	}
	node.Entity = entity

	// TO (GRANT) or FROM (REVOKE)
	if op == "GRANT" {
		if err := p.expect("TO"); err != nil {
			return nil, err
		}
	} else {
		if err := p.expect("FROM"); err != nil {
			return nil, err
		}
	}

	target, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.Target = target

	return node, nil
}

func (p *Parser) parseCreateUser(node *ast.QueryNode) (*ast.QueryNode, error) {
	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.UserName = name

	if p.match("WITH") {
		if p.match("PASSWORD") {
			tok := p.advance()
			node.Permission.Password = tok.Value
		}
	}

	return node, nil
}

func (p *Parser) parseDropUser(node *ast.QueryNode) (*ast.QueryNode, error) {
	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.UserName = name

	return node, nil
}

func (p *Parser) parseAlterUser(node *ast.QueryNode) (*ast.QueryNode, error) {
	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.UserName = name

	// WITH PASSWORD
	if p.match("WITH") {
		if p.match("PASSWORD") {
			tok := p.advance()
			node.Permission.Password = tok.Value
		}
	}

	return node, nil
}

func (p *Parser) parseCreateRole(node *ast.QueryNode) (*ast.QueryNode, error) {
	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.RoleName = name

	return node, nil
}

func (p *Parser) parseDropRole(node *ast.QueryNode) (*ast.QueryNode, error) {
	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.RoleName = name

	return node, nil
}

func (p *Parser) parseAssignRole(node *ast.QueryNode) (*ast.QueryNode, error) {
	role, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.RoleName = role

	if err := p.expect("TO"); err != nil {
		return nil, err
	}

	user, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.UserName = user

	return node, nil
}

func (p *Parser) parseRevokeRole(node *ast.QueryNode) (*ast.QueryNode, error) {
	role, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.RoleName = role

	if err := p.expect("FROM"); err != nil {
		return nil, err
	}

	user, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Permission.UserName = user

	return node, nil
}