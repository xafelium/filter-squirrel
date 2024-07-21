package filtersquirrel

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/xafelium/filter"
	"reflect"
	"strings"
)

const (
	// Portable true/false literals.
	sqlFalse = "(1=0)"
)

var (
	conditionBuilders = make(map[string]func(c filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error))
)

func init() {
	conditionBuilders[filter.AndConditionType] = applyAnd
	conditionBuilders[filter.ArrayContainsConditionType] = applyArrayContains
	conditionBuilders[filter.ArrayContainsArrayConditionType] = applyArrayContainsArray
	conditionBuilders[filter.ArrayIsContainedConditionType] = applyArrayIsContained
	conditionBuilders[filter.ArraysOverlapConditionType] = applyArraysOverlap
	conditionBuilders[filter.ContainsConditionType] = applyContains
	conditionBuilders[filter.EqualsConditionType] = applyEquals
	conditionBuilders[filter.GreaterThanConditionType] = applyGreaterThan
	conditionBuilders[filter.GreaterThanOrEqualConditionType] = applyGreaterThanOrEqual
	conditionBuilders[filter.GroupConditionType] = applyGroup
	conditionBuilders[filter.InConditionType] = applyIn
	conditionBuilders[filter.IsNilConditionType] = applyIsNil
	conditionBuilders[filter.LowerThanConditionType] = applyLowerThan
	conditionBuilders[filter.LowerThanOrEqualConditionType] = applyLowerThanOrEqual
	conditionBuilders[filter.NotConditionType] = applyNot
	conditionBuilders[filter.NotEqualsConditionType] = applyNotEquals
	conditionBuilders[filter.NotNilConditionType] = applyNotNil
	conditionBuilders[filter.NotRegexConditionType] = applyNotRegex
	conditionBuilders[filter.OrConditionType] = applyOr
	conditionBuilders[filter.OverlapsConditionType] = applyOverlaps
	conditionBuilders[filter.RegexConditionType] = applyRegex
	conditionBuilders[filter.WhereConditionType] = applyWhere
}

func ApplyFilter(b sq.SelectBuilder, condition filter.Condition, opts ...Option) (sq.SelectBuilder, []string, error) {
	if condition == nil {
		return b, nil, nil
	}
	options := FromDefaultOptions(opts...)
	tableAliasesMap := make(map[string]bool)
	sqlizers, err := applyFilter(condition, options.MapperFunc, tableAliasesMap)
	if err != nil {
		return b, nil, err
	}
	var tableAliases []string
	for alias := range tableAliasesMap {
		tableAliases = append(tableAliases, alias)
	}
	if sqlizers != nil {
		return b.Where(sqlizers), tableAliases, nil
	}
	return b, tableAliases, nil
}

func applyFilter(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	applyFunc, ok := conditionBuilders[condition.Type()]
	if !ok {
		return nil, fmt.Errorf("unknown condition: %s", condition.Type())
	}
	return applyFunc(condition, mf, tableAliases)
}

func applyWhere(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, nil
	}
	c, ok := condition.(*filter.WhereCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no WhereCondition")
	}
	if c.Condition == nil {
		return nil, nil
	}
	return applyFilter(c.Condition, mf, tableAliases)
}

func applyGroup(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.GroupCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no GroupCondition")
	}
	if c.Condition == nil {
		return nil, nil
	}
	return applyFilter(c.Condition, mf, tableAliases)
}

func applyOr(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.OrCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no OrCondition")
	}
	if len(c.Conditions) < 2 {
		return nil, fmt.Errorf("OR condition must have at least two conditions")
	}

	return applyOrConjunction(c.Conditions, mf, tableAliases)
}

func applyAnd(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.AndCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no AndCondition")
	}
	if len(c.Conditions) < 2 {
		return nil, fmt.Errorf("AND condition must have at least two conditions")
	}
	return applyAndConjunction(c.Conditions, mf, tableAliases)
}

func applyOrConjunction(conditions []filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	conj := sq.Or{}
	for _, condition := range conditions {
		sqlObj, err := applyFilter(condition, mf, tableAliases)
		if err != nil {
			return nil, err
		}
		if item, ok := sqlObj.(sq.Sqlizer); ok {
			conj = append(conj, item)
			continue
		} else if items, ok := sqlObj.([]sq.Sqlizer); ok {
			for _, item := range items {
				conj = append(conj, item)
			}
			continue
		}
		return nil, fmt.Errorf("unexpected data type: %T", sqlObj)
	}
	return conj, nil
}

func applyAndConjunction(conditions []filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	conj := sq.And{}
	for _, condition := range conditions {
		sqlObj, err := applyFilter(condition, mf, tableAliases)
		if err != nil {
			return nil, err
		}
		if item, ok := sqlObj.(sq.Sqlizer); ok {
			conj = append(conj, item)
			continue
		} else if items, ok := sqlObj.([]sq.Sqlizer); ok {
			for _, item := range items {
				conj = append(conj, item)
			}
			continue
		}
		return nil, fmt.Errorf("unexpected data type: %T", sqlObj)
	}
	return conj, nil
}

func applyEquals(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.EqualsCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no EqualsCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.Eq{fieldName: c.Value}, nil
}

func applyGreaterThan(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.GreaterThanCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no GreaterThanCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.Gt{fieldName: c.Value}, nil
}

func applyGreaterThanOrEqual(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.GreaterThanOrEqualCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no GreaterThanOrEqualCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.GtOrEq{fieldName: c.Value}, nil
}

func applyLowerThan(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.LowerThanCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no LowerThanCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.Lt{fieldName: c.Value}, nil
}

func applyLowerThanOrEqual(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.LowerThanOrEqualCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no LowerThanOrEqualCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.LtOrEq{fieldName: c.Value}, nil
}

func applyContains(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.ContainsCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no ContainsCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.ILike{fieldName: fmt.Sprintf("%%%s%%", c.Value)}, nil
}

func applyIn(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.InCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no InCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.Eq{fieldName: c.Value}, nil
}

func applyArrayContains(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.ArrayContainsCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no ArrayContainsCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &ArrayContains{fieldName: fieldName, value: c.Value}, nil
}

type ArrayContains struct {
	fieldName string
	value     any
}

func (a *ArrayContains) ToSql() (string, []interface{}, error) {
	if a.value == nil {
		return "", nil, fmt.Errorf("value cannot be nil")
	}
	return fmt.Sprintf("%s = ANY (%s)", a.fieldName, sq.Placeholders(1)), []any{a.value}, nil
}

func applyArrayContainsArray(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.ArrayContainsArrayCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no ArrayContainsArrayCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &ArrayContainsArray{fieldName: fieldName, value: c.Value}, nil
}

type ArrayContainsArray struct {
	fieldName string
	value     any
}

func (a *ArrayContainsArray) ToSql() (sql string, args []interface{}, err error) {
	if a.value == nil {
		sql = sqlFalse
		return
	}
	val := a.value
	if isListType(val) {
		valVal := reflect.ValueOf(val)
		if valVal.Len() == 0 {
			sql = sqlFalse
			return
		}
		for i := 0; i < valVal.Len(); i++ {
			args = append(args, valVal.Index(i).Interface())
		}
	} else {
		args = append(args, val)
	}
	sql = fmt.Sprintf("%s @> ARRAY[%s]", a.fieldName, sq.Placeholders(len(args)))
	return
}

func applyArrayIsContained(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.ArrayIsContainedCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no ArrayIsContainedCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &ArrayIsContained{fieldName: fieldName, value: c.Value}, nil
}

type ArrayIsContained struct {
	fieldName string
	value     any
}

func (a *ArrayIsContained) ToSql() (sql string, args []interface{}, err error) {
	if a.value == nil {
		sql = sqlFalse
		return
	}
	val := a.value
	if isListType(val) {
		valVal := reflect.ValueOf(val)
		if valVal.Len() == 0 {
			sql = sqlFalse
			return
		}
		for i := 0; i < valVal.Len(); i++ {
			args = append(args, valVal.Index(i).Interface())
		}
	} else {
		args = append(args, val)
	}
	sql = fmt.Sprintf("%s <@ ARRAY[%s]", a.fieldName, sq.Placeholders(len(args)))
	return
}

func applyRegex(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.RegexCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no RegexCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &Regex{
		fieldName:  fieldName,
		expression: c.Expression,
	}, nil
}

type Regex struct {
	fieldName  string
	expression string
}

func (r *Regex) ToSql() (string, []interface{}, error) {
	return fmt.Sprintf("%s ~ %s", r.fieldName, sq.Placeholders(1)), []any{r.expression}, nil
}

func applyNotRegex(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.NotRegexCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no NotRegexCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &NotRegex{
		fieldName:  fieldName,
		expression: c.Expression,
	}, nil
}

type NotRegex struct {
	fieldName  string
	expression string
}

func (r *NotRegex) ToSql() (string, []interface{}, error) {
	return fmt.Sprintf("%s !~ %s", r.fieldName, sq.Placeholders(1)), []any{r.expression}, nil
}

func applyIsNil(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.IsNilCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no IsNilCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.Eq{fieldName: nil}, nil
}

func applyNot(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.NotCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no NotCondition")
	}

	inner, err := applyFilter(c.Condition, mf, tableAliases)
	if err != nil {
		return nil, err
	}
	return &Not{
		inner: inner,
	}, nil
}

type Not struct {
	inner any
}

func (n *Not) ToSql() (string, []interface{}, error) {
	sqlObj, ok := n.inner.(sq.Sqlizer)
	if !ok {
		return "", nil, fmt.Errorf("expected sq.Sqlizer but got %T", n.inner)
	}
	sql, args, err := sqlObj.ToSql()
	return fmt.Sprintf("NOT (%s)", sql), args, err
}

func applyNotEquals(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.NotEqualsCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no NotEqualsCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.NotEq{fieldName: c.Value}, nil
}

func applyNotNil(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.NotNilCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no NotNilCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return sq.NotEq{fieldName: nil}, nil
}

func applyOverlaps(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.OverlapsCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no OverlapsCondition")
	}
	fieldName, err := mf(c.Field)
	if err != nil {
		return nil, err
	}
	addTableAlias(fieldName, tableAliases)
	return &Overlaps{fieldName: fieldName, value: c.Value}, nil
}

type Overlaps struct {
	fieldName string
	value     any
}

func (o *Overlaps) ToSql() (sql string, args []interface{}, err error) {
	if o.value == nil {
		sql = sqlFalse
		return
	}
	val := o.value
	if isListType(val) {
		valVal := reflect.ValueOf(val)
		if valVal.Len() == 0 {
			sql = sqlFalse
			return
		}
		for i := 0; i < valVal.Len(); i++ {
			args = append(args, valVal.Index(i).Interface())
		}
	} else {
		args = append(args, val)
	}
	sql = fmt.Sprintf("%s && ARRAY[%s]", o.fieldName, sq.Placeholders(len(args)))
	return
}

func applyArraysOverlap(condition filter.Condition, mf FieldMapperFunc, tableAliases map[string]bool) (any, error) {
	if condition == nil {
		return nil, fmt.Errorf("condition is nil")
	}
	c, ok := condition.(*filter.ArraysOverlapCondition)
	if !ok {
		return nil, fmt.Errorf("condition is no ArraysOverlapCondition")
	}
	return applyOverlaps(filter.Overlaps(c.Field, c.Value), mf, tableAliases)
}

func isListType(val any) bool {
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}

func addTableAlias(fieldName string, tableAliases map[string]bool) {
	tokens := strings.Split(fieldName, ".")
	if len(tokens) < 2 {
		return
	}
	tableAliases[tokens[0]] = true
}
