package parser

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/ast"
	"github.com/omniql-engine/omniql/engine/lexer"
	"github.com/omniql-engine/omniql/engine/models"
)

// Parser implements a recursive descent parser for OQL
type Parser struct {
	tokens []lexer.Token
	pos    int
}

// Parse is the package-level entry point for parsing OQL
// Returns models.Query for compatibility with existing translators
func Parse(input string) (*models.Query, error) {
	p, err := New(input)
	if err != nil {
		return nil, err
	}

	node, err := p.Parse()
	if err != nil {
		return nil, err
	}

	// Convert AST node to models.Query for translator compatibility
	return nodeToQuery(node), nil
}

// New creates a new parser from input string
func New(input string) (*Parser, error) {
	tokens, err := lexer.Tokenize(input)
	if err != nil {
		return nil, err
	}
	return &Parser{
		tokens: tokens,
		pos:    0,
	}, nil
}

// Parse parses the input and returns an AST node
func (p *Parser) Parse() (*ast.QueryNode, error) {
    if len(p.tokens) == 0 {
        return nil, p.error("empty input")
    }

    // Get operation from first token
    op := strings.ToUpper(p.current().Value)

    // Validate against mapping (SSOT)
    group, exists := mapping.OperationGroups[op]
    if !exists {
        return nil, p.errorWithSuggestion(op)
    }

    // Dispatch based on group
    var node *ast.QueryNode
    var err error
    
    switch group {
    case "CRUD":
        node, err = p.parseCRUD(op)
    case "DDL":
        node, err = p.parseDDL(op)
    case "DQL":
        node, err = p.parseDQL(op)
    case "TCL":
        node, err = p.parseTCL(op)
    case "DCL":
        node, err = p.parseDCL(op)
    default:
        return nil, p.error(fmt.Sprintf("unknown group '%s'", group))
    }
    
    if err != nil {
        return nil, err
    }
    
    // NEW: Ensure all tokens were consumed
    // If there are leftover tokens, it means something wasn't recognized
    if !p.isAtEnd() {
        tok := p.current()
        suggestion := lexer.SuggestSimilar(tok.Value)
        if suggestion != "" {
            return nil, p.error(fmt.Sprintf("unexpected '%s'. Did you mean '%s'?", tok.Value, suggestion))
        }
        return nil, p.error(fmt.Sprintf("unexpected '%s' after %s statement", tok.Value, op))
    }
    
    return node, nil
}

// =============================================================================
// TOKEN NAVIGATION
// =============================================================================

// current returns current token without advancing
func (p *Parser) current() lexer.Token {
	if p.pos >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

// advance moves to next token, returns previous
func (p *Parser) advance() lexer.Token {
	tok := p.current()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

// peek looks ahead without advancing
func (p *Parser) peek(offset int) lexer.Token {
	pos := p.pos + offset
	if pos < 0 || pos >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[pos]
}

// isAtEnd checks if all tokens consumed
func (p *Parser) isAtEnd() bool {
	return p.pos >= len(p.tokens) || p.current().Type == lexer.TOKEN_EOF
}

// match checks current token and advances if matched
func (p *Parser) match(values ...string) bool {
	cur := strings.ToUpper(p.current().Value)
	for _, v := range values {
		if cur == strings.ToUpper(v) {
			p.advance()
			return true
		}
	}
	return false
}

// expect consumes token if it matches, otherwise error
func (p *Parser) expect(value string) error {
	if strings.ToUpper(p.current().Value) != strings.ToUpper(value) {
		return p.error(fmt.Sprintf("expected '%s', got '%s'", value, p.current().Value))
	}
	p.advance()
	return nil
}

// expectIdentifier consumes and returns identifier
func (p *Parser) expectIdentifier() (string, error) {
	tok := p.current()
	if tok.Type != lexer.TOKEN_IDENTIFIER {
		return "", p.error(fmt.Sprintf("expected identifier, got '%s'", tok.Value))
	}
	p.advance()
	return tok.Value, nil
}

// skipToIdentifier advances until current token is an identifier
// followed by a clause keyword (WITH, ON, etc.) or EOF
// This is how grammar handles optional modifiers without special cases
func (p *Parser) skipKeywords() {
	for !p.isAtEnd() {
		cur := p.current()
		next := p.peek(1)

		// If current is identifier and next is clause or EOF, we found our target
		if cur.Type == lexer.TOKEN_IDENTIFIER {
			if next.Type == lexer.TOKEN_CLAUSE || next.Type == lexer.TOKEN_EOF {
				return
			}
		}
		p.advance()
	}
}

// =============================================================================
// CLAUSE DETECTION (using mapping as SSOT)
// =============================================================================

// isClause checks if current token is a clause keyword
func (p *Parser) isClause() bool {
	return mapping.IsClause(strings.ToUpper(p.current().Value))
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

// error creates parse error at current position
func (p *Parser) error(message string) error {
	tok := p.current()
	return &lexer.ParseError{
		Message:  message,
		Position: tok.Position,
		Line:     tok.Line,
		Column:   tok.Column,
		Token:    tok.Value,
	}
}

// errorWithSuggestion adds "did you mean" suggestion
func (p *Parser) errorWithSuggestion(unknown string) error {
	suggestion := lexer.SuggestSimilar(unknown)
	msg := fmt.Sprintf("unknown operation '%s'", unknown)
	if suggestion != "" {
		msg += fmt.Sprintf(". Did you mean '%s'?", suggestion)
	}
	return p.error(msg)
}

// =============================================================================
// AST TO MODELS CONVERSION (100% TrueAST)
// =============================================================================

// astExprToModelExpr converts AST ExpressionNode to models.Expression (recursive)
func astExprToModelExpr(expr *ast.ExpressionNode) *models.Expression {
	if expr == nil {
		return nil
	}

	result := &models.Expression{
		Type:          expr.Type,
		Value:         expr.Value,
		Operator:      expr.Operator,
		FunctionName:  expr.FunctionName,
		WindowOffset:  expr.WindowOffset,
		WindowBuckets: expr.WindowBuckets,
	}

	// Recursive: Left and Right for BINARY
	result.Left = astExprToModelExpr(expr.Left)
	result.Right = astExprToModelExpr(expr.Right)

	// Recursive: FunctionArgs
	for _, arg := range expr.FunctionArgs {
		result.FunctionArgs = append(result.FunctionArgs, astExprToModelExpr(arg))
	}

	// Recursive: CaseConditions
	for _, cc := range expr.CaseConditions {
		result.CaseConditions = append(result.CaseConditions, &models.CaseCondition{
			Condition: astCondToModelCond(cc.Condition),
			ThenExpr:  astExprToModelExpr(cc.ThenExpr),
		})
	}

	// Recursive: CaseElse
	result.CaseElse = astExprToModelExpr(expr.CaseElse)

	// Recursive: PartitionBy (for WINDOW)
	for _, pb := range expr.PartitionBy {
		result.PartitionBy = append(result.PartitionBy, astExprToModelExpr(pb))
	}

	// Recursive: WindowOrderBy - cast string to SortDirection
	for _, ob := range expr.WindowOrderBy {
		result.WindowOrderBy = append(result.WindowOrderBy, models.OrderBy{
			FieldExpr: astExprToModelExpr(ob.FieldExpr),
			Direction: models.SortDirection(ob.Direction),
		})
	}

	return result
}

// astCondToModelCond converts AST ConditionNode to models.Condition
func astCondToModelCond(cond *ast.ConditionNode) *models.Condition {
	if cond == nil {
		return nil
	}

	result := &models.Condition{
		FieldExpr:  astExprToModelExpr(cond.FieldExpr),
		Operator:   cond.Operator,
		ValueExpr:  astExprToModelExpr(cond.ValueExpr),
		Value2Expr: astExprToModelExpr(cond.Value2Expr),
		Logic:      cond.Logic,
	}

	// ValuesExpr (for IN operator)
	for _, v := range cond.ValuesExpr {
		result.ValuesExpr = append(result.ValuesExpr, astExprToModelExpr(v))
	}

	// Nested conditions
	for _, n := range cond.Nested {
		result.Nested = append(result.Nested, *conditionNodeToModel(n))
	}

	return result
}

// conditionNodeToModel converts AST condition to models.Condition (value type)
func conditionNodeToModel(c ast.ConditionNode) *models.Condition {
	return astCondToModelCond(&c)
}

// nodeToQuery converts AST node to models.Query (100% TrueAST)
func nodeToQuery(node *ast.QueryNode) *models.Query {
	q := &models.Query{
		Operation:    node.Operation,
		Entity:       node.Entity,
		Distinct:     node.Distinct,
		DatabaseName: node.DatabaseName,
		ViewName:     node.ViewName,
		NewName:      node.NewName,
		AlterAction:  node.AlterAction,

		// PostgreSQL DDL
		SequenceName:      node.SequenceName,
		SequenceStart:     node.SequenceStart,
		SequenceIncrement: node.SequenceIncrement,
		SequenceMin:       node.SequenceMin,
		SequenceMax:       node.SequenceMax,
		SequenceCache:     node.SequenceCache,
		SequenceCycle:     node.SequenceCycle,
		SequenceRestart:   node.SequenceRestart,
		ExtensionName:     node.ExtensionName,
		SchemaName:        node.SchemaName,
		SchemaOwner:       node.SchemaOwner,
		TypeName:          node.TypeName,
		TypeKind:          node.TypeKind,
		EnumValues:        node.EnumValues,
		EnumValue:         node.EnumValue,
		NewEnumValue:      node.NewEnumValue,
		DomainName:        node.DomainName,
		DomainType:        node.DomainType,
		DomainDefault:     node.DomainDefault,
		DomainConstraint:  node.DomainConstraint,
		FuncName:          node.FuncName,
		FuncBody:          node.FuncBody,
		FuncArgs:          node.FuncArgs,
		FuncReturns:       node.FuncReturns,
		FuncLanguage:      node.FuncLanguage,
		FuncOwner:         node.FuncOwner,
		TriggerName:       node.TriggerName,
		TriggerTiming:     node.TriggerTiming,
		TriggerEvents:     node.TriggerEvents,
		TriggerForEach:    node.TriggerForEach,
		PolicyName:        node.PolicyName,
		PolicyFor:         node.PolicyFor,
		PolicyTo:          node.PolicyTo,
		PolicyUsing:       node.PolicyUsing,
		PolicyCheck:       node.PolicyCheck,
		RuleName:          node.RuleName,
		RuleEvent:         node.RuleEvent,
		RuleAction:        node.RuleAction,
		CommentTarget:     node.CommentTarget,
		CommentText:       node.CommentText,
		Cascade:           node.Cascade,
	}

	// Columns (100% TrueAST)
	for _, col := range node.Columns {
		q.Columns = append(q.Columns, astExprToModelExpr(col))
	}

	// GroupBy (100% TrueAST)
	for _, gb := range node.GroupBy {
		q.GroupBy = append(q.GroupBy, astExprToModelExpr(gb))
	}

	// ViewQuery (100% TrueAST)
	if node.ViewQuery != nil {
		q.ViewQuery = nodeToQuery(node.ViewQuery)
	}

	// Limit/Offset
	if node.Limit != nil {
		q.Limit = *node.Limit
	}
	if node.Offset != nil {
		q.Offset = *node.Offset
	}

	// Fields (100% TrueAST)
	for _, f := range node.Fields {
		q.Fields = append(q.Fields, models.Field{
			NameExpr:    astExprToModelExpr(f.NameExpr),
			ValueExpr:   astExprToModelExpr(f.ValueExpr),
			Constraints: f.Constraints,
		})
	}

	// Conditions (WHERE) - 100% TrueAST
	if node.Conditions != nil {
		for _, c := range node.Conditions.Conditions {
			q.Conditions = append(q.Conditions, *conditionNodeToModel(c))
		}
	}

	// Having (100% TrueAST)
	for _, c := range node.Having {
		q.Having = append(q.Having, *conditionNodeToModel(c))
	}

	// OrderBy (100% TrueAST) - cast string to SortDirection
	for _, o := range node.OrderBy {
		q.OrderBy = append(q.OrderBy, models.OrderBy{
			FieldExpr: astExprToModelExpr(o.FieldExpr),
			Direction: models.SortDirection(o.Direction),
		})
	}

	// Joins (100% TrueAST) - cast string to JoinType
	for _, j := range node.Joins {
		q.Joins = append(q.Joins, models.Join{
			Type:      models.JoinType(j.Type),
			Table:     j.Table,
			LeftExpr:  astExprToModelExpr(j.LeftExpr),
			RightExpr: astExprToModelExpr(j.RightExpr),
		})
	}

	// Aggregate (100% TrueAST) - cast string to AggregateFunc
	if node.Aggregate != nil {
		q.Aggregate = &models.Aggregation{
			Function:  models.AggregateFunc(node.Aggregate.Function),
			FieldExpr: astExprToModelExpr(node.Aggregate.FieldExpr),
		}
	}

	// WindowFunctions (100% TrueAST) - cast string to WindowFunc
	for _, wf := range node.WindowFunctions {
		mwf := models.WindowFunction{
			Function:  models.WindowFunc(wf.Function),
			FieldExpr: astExprToModelExpr(wf.FieldExpr),
			Alias:     wf.Alias,
			Offset:    wf.Offset,
			Buckets:   wf.Buckets,
		}
		for _, pb := range wf.PartitionBy {
			mwf.PartitionBy = append(mwf.PartitionBy, astExprToModelExpr(pb))
		}
		for _, ob := range wf.OrderBy {
			mwf.OrderBy = append(mwf.OrderBy, models.OrderBy{
				FieldExpr: astExprToModelExpr(ob.FieldExpr),
				Direction: models.SortDirection(ob.Direction),
			})
		}
		q.WindowFunctions = append(q.WindowFunctions, mwf)
	}

	// Upsert (100% TrueAST)
	if node.Upsert != nil {
		q.Upsert = &models.Upsert{}
		for _, cf := range node.Upsert.ConflictFields {
			q.Upsert.ConflictFields = append(q.Upsert.ConflictFields, astExprToModelExpr(cf))
		}
		for _, f := range node.Upsert.UpdateFields {
			q.Upsert.UpdateFields = append(q.Upsert.UpdateFields, models.Field{
				NameExpr:    astExprToModelExpr(f.NameExpr),
				ValueExpr:   astExprToModelExpr(f.ValueExpr),
				Constraints: f.Constraints,
			})
		}
	}

	// BulkData (100% TrueAST)
	for _, row := range node.BulkData {
		var fields []models.Field
		for _, f := range row {
			fields = append(fields, models.Field{
				NameExpr:    astExprToModelExpr(f.NameExpr),
				ValueExpr:   astExprToModelExpr(f.ValueExpr),
				Constraints: f.Constraints,
			})
		}
		q.BulkData = append(q.BulkData, fields)
	}

	// Transaction (unchanged - no expressions)
	if node.Transaction != nil {
		q.Transaction = &models.Transaction{
			Operation:      node.Transaction.Operation,
			SavepointName:  node.Transaction.SavepointName,
			IsolationLevel: node.Transaction.IsolationLevel,
			ReadOnly:       node.Transaction.ReadOnly,
		}
	}

	// Permission (unchanged - no expressions)
	if node.Permission != nil {
		q.Permission = &models.Permission{
			Operation:   node.Permission.Operation,
			Permissions: node.Permission.Permissions,
			Target:      node.Permission.Target,
			RoleName:    node.Permission.RoleName,
			UserName:    node.Permission.UserName,
			Password:    node.Permission.Password,
			Roles:       node.Permission.Roles,
		}
	}

	// SelectColumns (100% TrueAST)
	for _, sc := range node.SelectColumns {
		q.SelectColumns = append(q.SelectColumns, models.SelectColumn{
			ExpressionObj: astExprToModelExpr(sc.ExpressionObj),
			Alias:         sc.Alias,
		})
	}

	// SetOperation (100% TrueAST) - cast string to SetOperationType
	if node.SetOperation != nil {
		q.SetOperation = &models.SetOperation{
			Type: models.SetOperationType(node.SetOperation.Type),
		}
		if node.SetOperation.LeftQuery != nil {
			q.SetOperation.LeftQuery = nodeToQuery(node.SetOperation.LeftQuery)
		}
		if node.SetOperation.RightQuery != nil {
			q.SetOperation.RightQuery = nodeToQuery(node.SetOperation.RightQuery)
		}
	}

	return q
}