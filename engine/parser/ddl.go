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
	default:
		return nil, p.error("unimplemented DDL: " + op)
	}
}

// =============================================================================
// DDL PARSERS - Grammar: OPERATION keyword* identifier ...
// =============================================================================

// CREATE TABLE [keywords] name WITH columns
func (p *Parser) parseCreateTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE TABLE

	// Grammar: skip any keywords until identifier
	p.skipKeywords()

	// Now at identifier = table name
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// WITH columns
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

// DROP TABLE [keywords] name
func (p *Parser) parseDropTable() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP TABLE",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP TABLE

	// Grammar: skip any keywords until identifier
	p.skipKeywords()

	// Now at identifier = table name
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

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
			node.Fields = append(node.Fields, ast.FieldNode{
				NameExpr:  makeFieldExpr(parts[0], pos),
				ValueExpr: makeLiteralExpr(parts[1], pos),
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
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr:  makeFieldExpr(parts[1], pos),
			ValueExpr: makeLiteralExpr(parts[2], pos),
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

	// Don't use skipKeywords - just get entity directly
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	return node, nil
}

// CREATE INDEX [keywords] table index_name:column [UNIQUE]
// Format: CREATE INDEX products idx_product_name:product_name UNIQUE
func (p *Parser) parseCreateIndex() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE INDEX",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE INDEX

	// Don't use skipKeywords() - it eats the table name
	// Just get table name directly
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// Index name:column (as single token with colon)
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

	// Check for UNIQUE modifier
	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "UNIQUE" {
		p.advance()
		field.Constraints = append(field.Constraints, "UNIQUE")
	}

	node.Fields = append(node.Fields, field)

	return node, nil
}

// DROP INDEX [keywords] table index_name
// Format: DROP INDEX products idx_product_name
func (p *Parser) parseDropIndex() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP INDEX",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP INDEX

	// Don't use skipKeywords() - just get table name directly
	entity, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = entity

	// Index name
	if !p.isAtEnd() {
		nameTok := p.current()
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		// Store index name in Fields
		node.Fields = append(node.Fields, ast.FieldNode{
			NameExpr: makeFieldExpr(name, nameTok.Position),
			Position: nameTok.Position,
		})
	}

	return node, nil
}

// CREATE DATABASE [keywords] name
func (p *Parser) parseCreateDatabase() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE DATABASE",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE DATABASE

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DatabaseName = name

	return node, nil
}

// DROP DATABASE [keywords] name
func (p *Parser) parseDropDatabase() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP DATABASE",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP DATABASE

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DatabaseName = name

	return node, nil
}

// CREATE VIEW name AS query (100% TrueAST)
// Format: CREATE VIEW active_users AS GET User WHERE active = true
func (p *Parser) parseCreateView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE VIEW

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	// Parse the view query as *QueryNode (100% TrueAST - no fallback)
	viewQuery, err := p.Parse()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = viewQuery

	return node, nil
}

// DROP VIEW [keywords] name
func (p *Parser) parseDropView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP VIEW

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

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

// ALTER VIEW name AS query (100% TrueAST)
// Format: ALTER VIEW active_users AS GET User WHERE status = 'active'
func (p *Parser) parseAlterView() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER VIEW",
		Position:  p.current().Position,
	}
	p.advance() // consume ALTER VIEW

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ViewName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	// Parse the view query as *QueryNode (100% TrueAST - no fallback)
	viewQuery, err := p.Parse()
	if err != nil {
		return nil, err
	}
	node.ViewQuery = viewQuery

	return node, nil
}

// CREATE COLLECTION [keywords] name (MongoDB)
func (p *Parser) parseCreateCollection() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE COLLECTION",
		Position:  p.current().Position,
	}
	p.advance() // consume CREATE COLLECTION

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = name

	return node, nil
}

// DROP COLLECTION [keywords] name (MongoDB)
func (p *Parser) parseDropCollection() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP COLLECTION",
		Position:  p.current().Position,
	}
	p.advance() // consume DROP COLLECTION

	p.skipKeywords()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = name

	return node, nil
}