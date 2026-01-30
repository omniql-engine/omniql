package parser

import (
	"strings"

	"github.com/omniql-engine/omniql/engine/ast"
)


// =============================================================================
// DDL DISPATCHER
// =============================================================================

func (p *Parser) parseDDL(op string) (*ast.QueryNode, error) {
	switch op {
	case "CREATE TABLE":
		return p.parseCreateTable()
	case "DROP TABLE":
		return p.parseDropTable()
	case "ALTER TABLE":
		return p.parseAlterTable()
	case "TRUNCATE TABLE", "TRUNCATE":
		return p.parseTruncate()
	case "CREATE INDEX":
		return p.parseCreateIndex()
	case "DROP INDEX":
		return p.parseDropIndex()
	case "CREATE DATABASE":
		return p.parseCreateDatabase()
	case "DROP DATABASE":
		return p.parseDropDatabase()
	case "CREATE VIEW":
		return p.parseCreateView()
	case "DROP VIEW":
		return p.parseDropView()
	case "ALTER VIEW":
		return p.parseAlterView()
	case "RENAME TABLE":
		return p.parseRenameTable()
	case "CREATE COLLECTION":
		return p.parseCreateCollection()
	case "DROP COLLECTION":
		return p.parseDropCollection()
		
	// PostgreSQL-specific DDL
	case "CREATE SEQUENCE":
		return p.parseCreateSequence()
	case "ALTER SEQUENCE":
		return p.parseAlterSequence()
	case "DROP SEQUENCE":
		return p.parseDropSequence()
	case "CREATE EXTENSION":
		return p.parseCreateExtension()
	case "DROP EXTENSION":
		return p.parseDropExtension()
	case "CREATE SCHEMA":
		return p.parseCreateSchema()
	case "DROP SCHEMA":
		return p.parseDropSchema()
	case "CREATE TYPE":
		return p.parseCreateType()
	case "ALTER TYPE":
		return p.parseAlterType()
	case "DROP TYPE":
		return p.parseDropType()
	case "CREATE DOMAIN":
		return p.parseCreateDomain()
	case "DROP DOMAIN":
		return p.parseDropDomain()
	case "CREATE FUNCTION":
		return p.parseCreateFunction()
	case "ALTER FUNCTION":
		return p.parseAlterFunction()
	case "DROP FUNCTION":
		return p.parseDropFunction()
	case "CREATE TRIGGER":
		return p.parseCreateTrigger()
	case "DROP TRIGGER":
		return p.parseDropTrigger()
	case "CREATE POLICY":
		return p.parseCreatePolicy()
	case "DROP POLICY":
		return p.parseDropPolicy()
	case "CREATE RULE":
		return p.parseCreateRule()
	case "DROP RULE":
		return p.parseDropRule()
	case "COMMENT ON":
		return p.parseCommentOn()
	default:
		return nil, p.error("unimplemented DDL: " + op)
	}
}

// =============================================================================
// DDL PARSERS
// =============================================================================

// CREATE TABLE name WITH columns
func (p *Parser) parseCreateTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE TABLE

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if err := p.expect("WITH"); err != nil {
		return nil, err
	}

	columns, err := p.parseColumnDefinitions()
	if err != nil {
		return nil, err
	}
	node.Fields = columns

	return node, nil
}

// DROP TABLE name [CASCADE]
func (p *Parser) parseDropTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP TABLE

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// ALTER TABLE name action
// Format: ALTER TABLE products ADD_COLUMN:description:TEXT
//         ALTER TABLE products DROP_COLUMN:description
//         ALTER TABLE products RENAME_COLUMN:name:product_name
func (p *Parser) parseAlterTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume ALTER TABLE

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// Parse action: ADD name:TYPE or DROP name or RENAME old:new
	// Also accepts: ADD_COLUMN:name:TYPE, DROP_COLUMN:name, RENAME_COLUMN:old:new
	if p.isAtEnd() {
		return node, nil
	}
	actionTok := p.advance()
	action := strings.ToUpper(actionTok.Value)
	pos := actionTok.Position

	// Handle SQL-like syntax: ADD name:type, DROP name, RENAME old TO new
	if action == "ADD" || action == "DROP" || action == "RENAME" || action == "MODIFY" {
		if p.isAtEnd() {
			return nil, p.error("expected column specification after " + action)
		}
		colTok := p.advance()
		parts := strings.Split(colTok.Value, ":")

		switch action {
		case "ADD":
		node.AlterAction = "ADD_COLUMN"
		if len(parts) < 2 {
			return nil, p.error("ADD requires column:type")
		}
		typ := parts[1]
		if p.match("(") {
			var sizeParts []string
			for !p.isAtEnd() && p.current().Value != ")" {
				sizeParts = append(sizeParts, p.advance().Value)
				p.match(",")
			}
			p.expect(")")
			typ = typ + "(" + strings.Join(sizeParts, ",") + ")"
		}
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr:  makeFieldExpr(parts[0], pos),
			ValueExpr: makeLiteralExpr(typ, pos),
			Position:  pos,
		})
		case "DROP":
			node.AlterAction = "DROP_COLUMN"
			node.Fields = append(node.Fields, ast.FieldNode{
				NameExpr: makeFieldExpr(parts[0], pos),
				Position: pos,
			})
		case "RENAME":
			node.AlterAction = "RENAME_COLUMN"
			if len(parts) < 2 {
				return nil, p.error("RENAME requires old_name:new_name")
			}
			node.Fields = append(node.Fields, ast.FieldNode{
				NameExpr:  makeFieldExpr(parts[0], pos),
				ValueExpr: makeFieldExpr(parts[1], pos), // new name
				Position:  pos,
			})
		case "MODIFY":
			node.AlterAction = "MODIFY_COLUMN"
			if len(parts) < 2 {
				return nil, p.error("MODIFY requires column:new_type")
			}
			node.Fields = append(node.Fields, ast.FieldNode{
				NameExpr:  makeFieldExpr(parts[0], pos),
				ValueExpr: makeLiteralExpr(parts[1], pos),
				Position:  pos,
			})
		}
		return node, nil
	}

	// Handle legacy format: ADD_COLUMN:name:type
	parts := strings.Split(actionTok.Value, ":")
	if len(parts) < 2 {
		return nil, p.error("expected ALTER action like ADD column:TYPE")
	}
	action = strings.ToUpper(parts[0])

	switch action {
	case "ADD_COLUMN":
		node.AlterAction = "ADD_COLUMN"
		if len(parts) < 3 {
			return nil, p.error("ADD_COLUMN requires column:type")
		}
		typ := parts[2]
		if p.match("(") {
			var sizeParts []string
			for !p.isAtEnd() && p.current().Value != ")" {
				sizeParts = append(sizeParts, p.advance().Value)
				p.match(",")
			}
			p.expect(")")
			typ = typ + "(" + strings.Join(sizeParts, ",") + ")"
		}
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr:  makeFieldExpr(parts[1], pos),
			ValueExpr: makeLiteralExpr(typ, pos),
			Position:  pos,
		})

	case "DROP_COLUMN":
		node.AlterAction = "DROP_COLUMN"
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr: makeFieldExpr(parts[1], pos),
			Position: pos,
		})

	case "RENAME_COLUMN":
		node.AlterAction = "RENAME_COLUMN"
		if len(parts) < 3 {
			return nil, p.error("RENAME_COLUMN requires old_name:new_name")
		}
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr:  makeFieldExpr(parts[1], pos),
			ValueExpr: makeFieldExpr(parts[2], pos),
			Position:  pos,
		})

	case "MODIFY_COLUMN":
		node.AlterAction = "MODIFY_COLUMN"
		if len(parts) < 3 {
			return nil, p.error("MODIFY_COLUMN requires column:new_type")
		}
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr:  makeFieldExpr(parts[1], pos),
			ValueExpr: makeLiteralExpr(parts[2], pos),
			Position:  pos,
		})

	default:
		return nil, p.error("unknown ALTER action: " + action)
	}

	return node, nil
}

// TRUNCATE [TABLE] name
func (p *Parser) parseTruncate() (*ast.QueryNode, error) {
	op := strings.ToUpper(p.current().Value) // preserve "TRUNCATE" or "TRUNCATE TABLE"
	node := &ast.QueryNode{
		Operation: op,
		Position:  p.current().Position,
	}
	p.advance() // consume TRUNCATE [TABLE]

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	return node, nil
}

// CREATE INDEX table index_name:column [UNIQUE]
func (p *Parser) parseCreateIndex() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE INDEX",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE INDEX

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if p.isAtEnd() {
		return nil, p.error("expected index_name:column")
	}

	indexTok := p.advance()
	parts := strings.Split(indexTok.Value, ":")
	pos := indexTok.Position

	if len(parts) < 2 {
		return nil, p.error("expected index_name:column format")
	}

	field := ast.FieldNode{
		NameExpr:  makeFieldExpr(parts[0], pos),
		ValueExpr: makeFieldExpr(parts[1], pos),
		Position:  pos,
	}

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "UNIQUE" {
		p.advance()
		field.Constraints = append(field.Constraints, "UNIQUE")
	}

	node.Fields = append(node.Fields, field)

	return node, nil
}

// DROP INDEX table index_name
func (p *Parser) parseDropIndex() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP INDEX",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP INDEX

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if !p.isAtEnd() {
		nameTok := p.current()
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr: makeFieldExpr(name, nameTok.Position),
			Position: nameTok.Position,
		})
	}

	return node, nil
}

// CREATE DATABASE name
func (p *Parser) parseCreateDatabase() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE DATABASE",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE DATABASE

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DatabaseName = name

	return node, nil
}

// DROP DATABASE name [CASCADE]
func (p *Parser) parseDropDatabase() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP DATABASE",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP DATABASE

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DatabaseName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE VIEW name AS query
func (p *Parser) parseCreateView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE VIEW

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	viewQuery, err := p.Parse()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = viewQuery

	return node, nil
}

// DROP VIEW name [CASCADE]
func (p *Parser) parseDropView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP VIEW

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// RENAME TABLE name TO newname
func (p *Parser) parseRenameTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "RENAME TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume RENAME TABLE

	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	if err := p.expect("TO"); err != nil {
		return nil, err
	}

	newName, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.NewName = newName

	return node, nil
}

// ALTER VIEW name AS query
func (p *Parser) parseAlterView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume ALTER VIEW

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	viewQuery, err := p.Parse()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = viewQuery

	return node, nil
}

// CREATE COLLECTION name (MongoDB)
func (p *Parser) parseCreateCollection() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE COLLECTION",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE COLLECTION

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = name

	return node, nil
}

// DROP COLLECTION name (MongoDB)
func (p *Parser) parseDropCollection() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP COLLECTION",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP COLLECTION

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = name

	return node, nil
}