package query

import (
	"fmt"
	"strings"

	"github.com/eqimd/accord/internal/common"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

type exprVisitor struct {
	prev string
	keys common.Set[string]
}

func (v *exprVisitor) Visit(node *ast.Node) {
	cur := (*node).String()

	if v.prev == "GET" || v.prev == "SET" {
		cur = strings.Trim(cur, "\"")

		v.keys.Add(cur)
	}

	v.prev = cur
}

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) QueryKeys(query string) ([]string, error) {
	visitor := &exprVisitor{
		keys: make(common.Set[string]),
	}

	_, err := expr.Compile(query, expr.Patch(visitor))
	if err != nil {
		return nil, fmt.Errorf("query compile error: %w", err)
	}

	return visitor.keys.Slice(), nil
}

func (e *Executor) Execute(query string, reads map[string]string) (string, map[string]string, error) {
	executor := &queryExecutor{
		preValues: reads,
		setValues: map[string]string{},
	}

	env := map[string]any{
		"GET": executor.get,
		"SET": executor.set,
	}

	result, err := expr.Eval(query, env)
	if err != nil {
		return "", nil, fmt.Errorf("cannot execute query: %w", err)
	}

	return result.(string), executor.setValues, nil
}

type queryExecutor struct {
	// (key -> value) mapping known before executing query; also used for further execution
	preValues map[string]string
	// values that should be set after execution
	setValues map[string]string
}

func (e *queryExecutor) get(key string) string {
	return e.preValues[key]
}

func (e *queryExecutor) set(key, value string) string {
	oldVal := e.preValues[key]

	e.preValues[key] = value
	e.setValues[key] = value

	return oldVal
}
