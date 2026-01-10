package parser

import (
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/engine/ast"
)

// =============================================================================
// POSTGRESQL-SPECIFIC DDL PARSERS
// =============================================================================

// CREATE SEQUENCE name [START:n] [INCREMENT:n] [MIN:n] [MAX:n] [CACHE:n] [CYCLE]
func (p *Parser) parseCreateSequence() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE SEQUENCE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.SequenceName = name

	for !p.isAtEnd() {
		tok := p.current()
		upper := strings.ToUpper(tok.Value)

		if upper == "CASCADE" {
			p.advance()
			node.Cascade = true
			continue
		}
		if upper == "CYCLE" {
			p.advance()
			node.SequenceCycle = true
			continue
		}

		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 {
			key := strings.ToUpper(parts[0])
			val, _ := strconv.ParseInt(parts[1], 10, 64)
			switch key {
			case "START":
				node.SequenceStart = val
			case "INCREMENT":
				node.SequenceIncrement = val
			case "MINVALUE", "MIN":
				node.SequenceMin = val
			case "MAXVALUE", "MAX":
				node.SequenceMax = val
			case "CACHE":
				node.SequenceCache = val
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// ALTER SEQUENCE name [RESTART:n] [INCREMENT:n]
func (p *Parser) parseAlterSequence() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER SEQUENCE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.SequenceName = name

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 {
			key := strings.ToUpper(parts[0])
			val, _ := strconv.ParseInt(parts[1], 10, 64)
			switch key {
			case "RESTART":
				node.SequenceRestart = val
			case "INCREMENT":
				node.SequenceIncrement = val
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// DROP SEQUENCE name [CASCADE]
func (p *Parser) parseDropSequence() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP SEQUENCE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.SequenceName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE EXTENSION name [SCHEMA:schema]
func (p *Parser) parseCreateExtension() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE EXTENSION",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ExtensionName = name

	if !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 && strings.ToUpper(parts[0]) == "SCHEMA" {
			node.SchemaName = parts[1]
			p.advance()
		}
	}

	return node, nil
}

// DROP EXTENSION name [CASCADE]
func (p *Parser) parseDropExtension() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP EXTENSION",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.ExtensionName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE SCHEMA name [OWNER:owner]
func (p *Parser) parseCreateSchema() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE SCHEMA",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.SchemaName = name

	if !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 && strings.ToUpper(parts[0]) == "OWNER" {
			node.SchemaOwner = parts[1]
			p.advance()
		}
	}

	return node, nil
}

// DROP SCHEMA name [CASCADE]
func (p *Parser) parseDropSchema() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP SCHEMA",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.SchemaName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE TYPE name AS ENUM VALUES:val1,val2,val3
// CREATE TYPE name AS COMPOSITE WITH field1:type1, field2:type2
func (p *Parser) parseCreateType() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE TYPE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.TypeName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	kindTok := p.advance()
	kind := strings.ToUpper(kindTok.Value)

	if kind == "ENUM" {
		node.TypeKind = "ENUM"
		if p.isAtEnd() {
			return nil, p.error("ENUM requires VALUES:val1,val2,...")
		}
		valuesTok := p.advance()
		parts := strings.Split(valuesTok.Value, ":")
		if len(parts) == 2 && strings.ToUpper(parts[0]) == "VALUES" {
			// Start with first value
			values := []string{parts[1]}
			
			// Keep reading comma-separated values (lexer splits on commas)
			for !p.isAtEnd() && p.current().Value == "," {
				p.advance() // consume comma
				if p.isAtEnd() {
					break
				}
				values = append(values, p.current().Value)
				p.advance() // consume value
			}
			
			node.EnumValues = values
		} else {
			return nil, p.error("ENUM requires VALUES:val1,val2,...")
		}
	} else if kind == "COMPOSITE" {
		node.TypeKind = "COMPOSITE"
		if err := p.expect("WITH"); err != nil {
			return nil, err
		}
		columns, err := p.parseColumnDefinitions()
		if err != nil {
			return nil, err
		}
		node.Fields = columns
	} else {
		return nil, p.error("CREATE TYPE requires AS ENUM or AS COMPOSITE")
	}

	return node, nil
}

// ALTER TYPE name ADD_VALUE:value or RENAME_VALUE:old:new
func (p *Parser) parseAlterType() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER TYPE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.TypeName = name

	if p.isAtEnd() {
		return nil, p.error("ALTER TYPE requires action")
	}

	actionTok := p.advance()
	parts := strings.Split(actionTok.Value, ":")

	if len(parts) >= 2 {
		action := strings.ToUpper(parts[0])
		switch action {
		case "ADD_VALUE":
			node.AlterAction = "ADD_VALUE"
			node.EnumValue = parts[1]
		case "RENAME_VALUE":
			if len(parts) < 3 {
				return nil, p.error("RENAME_VALUE requires old:new")
			}
			node.AlterAction = "RENAME_VALUE"
			node.EnumValue = parts[1]
			node.NewEnumValue = parts[2]
		default:
			return nil, p.error("ALTER TYPE requires ADD_VALUE or RENAME_VALUE")
		}
	}

	return node, nil
}

// DROP TYPE name [CASCADE]
func (p *Parser) parseDropType() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP TYPE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.TypeName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE DOMAIN name AS type [DEFAULT:value] [CHECK:constraint]
func (p *Parser) parseCreateDomain() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE DOMAIN",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DomainName = name

	if err := p.expect("AS"); err != nil {
		return nil, err
	}

	typeName, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DomainType = typeName

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 {
			key := strings.ToUpper(parts[0])
			switch key {
			case "DEFAULT":
				node.DomainDefault = parts[1]
			case "CHECK":
				node.DomainConstraint = parts[1]
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// DROP DOMAIN name [CASCADE]
func (p *Parser) parseDropDomain() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP DOMAIN",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.DomainName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE FUNCTION name ARGS:arg1,arg2 RETURNS:type LANGUAGE:lang BODY:$$code$$
func (p *Parser) parseCreateFunction() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE FUNCTION",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.FuncName = name

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) >= 2 {
			key := strings.ToUpper(parts[0])
			value := strings.Join(parts[1:], ":")
			switch key {
			case "ARGS":
				node.FuncArgs = strings.Split(value, ",")
			case "RETURNS":
				node.FuncReturns = value
			case "LANGUAGE":
				node.FuncLanguage = value
			case "BODY":
				if value == "" {
					p.advance()
					if !p.isAtEnd() {
						node.FuncBody = p.current().Value
					}
				} else {
					node.FuncBody = value
				}
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// ALTER FUNCTION name OWNER:owner or SCHEMA:schema
func (p *Parser) parseAlterFunction() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "ALTER FUNCTION",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.FuncName = name

	if !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 {
			key := strings.ToUpper(parts[0])
			switch key {
			case "OWNER":
				node.FuncOwner = parts[1]
			case "SCHEMA":
				node.SchemaName = parts[1]
			}
			p.advance()
		}
	}

	return node, nil
}

// DROP FUNCTION name [CASCADE]
func (p *Parser) parseDropFunction() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP FUNCTION",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.FuncName = name

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE TRIGGER name ON table TIMING:BEFORE EVENTS:INSERT,UPDATE FOREACH:ROW FUNCTION:funcname
func (p *Parser) parseCreateTrigger() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE TRIGGER",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.TriggerName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) == 2 {
			key := strings.ToUpper(parts[0])
			switch key {
			case "TIMING":
				node.TriggerTiming = parts[1]
			case "EVENTS":
				node.TriggerEvents = parts[1]
			case "FOREACH":
				node.TriggerForEach = parts[1]
			case "FUNCTION":
				node.FuncName = parts[1]
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// DROP TRIGGER name ON table [CASCADE]
func (p *Parser) parseDropTrigger() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP TRIGGER",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.TriggerName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// CREATE POLICY name ON table FOR:SELECT TO:role USING:expression CHECK:expression
func (p *Parser) parseCreatePolicy() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE POLICY",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.PolicyName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) >= 2 {
			key := strings.ToUpper(parts[0])
			value := strings.Join(parts[1:], ":")
			switch key {
			case "FOR":
				node.PolicyFor = value
			case "TO":
				node.PolicyTo = value
			case "USING":
				node.PolicyUsing = value
			case "CHECK":
				node.PolicyCheck = value
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// DROP POLICY name ON table
func (p *Parser) parseDropPolicy() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP POLICY",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.PolicyName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	return node, nil
}

// CREATE RULE name ON table EVENT:INSERT ACTION:NOTHING
func (p *Parser) parseCreateRule() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "CREATE RULE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.RuleName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	for !p.isAtEnd() {
		tok := p.current()
		parts := strings.Split(tok.Value, ":")
		if len(parts) >= 2 {
			key := strings.ToUpper(parts[0])
			value := strings.Join(parts[1:], ":")
			switch key {
			case "EVENT":
				node.RuleEvent = value
			case "ACTION":
				node.RuleAction = value
			}
			p.advance()
		} else {
			break
		}
	}

	return node, nil
}

// DROP RULE name ON table [CASCADE]
func (p *Parser) parseDropRule() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "DROP RULE",
		Position:  p.current().Position,
	}
	p.advance()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.RuleName = name

	if err := p.expect("ON"); err != nil {
		return nil, err
	}

	table, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}
	node.Entity = table

	if !p.isAtEnd() && strings.ToUpper(p.current().Value) == "CASCADE" {
		p.advance()
		node.Cascade = true
	}

	return node, nil
}

// COMMENT ON target TEXT:comment (collects all remaining tokens as comment text)
func (p *Parser) parseCommentOn() (*ast.QueryNode, error) {
	node := &ast.QueryNode{
		Operation: "COMMENT ON",
		Position:  p.current().Position,
	}
	p.advance()

	var targetParts []string
	for !p.isAtEnd() {
		tok := p.current()
		if strings.HasPrefix(strings.ToUpper(tok.Value), "TEXT:") {
			parts := strings.SplitN(tok.Value, ":", 2)
			if len(parts) == 2 {
				// Start with the text after TEXT:
				textParts := []string{parts[1]}
				p.advance()
				
				// Collect ALL remaining tokens as part of the comment
				for !p.isAtEnd() {
					textParts = append(textParts, p.current().Value)
					p.advance()
				}
				
				node.CommentText = strings.Join(textParts, " ")
			}
			break
		}
		targetParts = append(targetParts, tok.Value)
		p.advance()
	}

	node.CommentTarget = strings.Join(targetParts, " ")

	return node, nil
}