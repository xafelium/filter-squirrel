package filtersquirrel

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	"github.com/xafelium/filter"
	"sort"
	"testing"
)

func TestImplementsAllConditionTypes(t *testing.T) {
	var actual []string
	for t := range conditionBuilders {
		actual = append(actual, t)
	}
	sort.Strings(actual)
	expected := filter.AllConditionTypes()
	sort.Strings(expected)
	require.Equal(t, expected, actual)
}

//goland:noinspection SqlNoDataSourceInspection,SqlResolve
func TestApplyFilter(t *testing.T) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	tests := []struct {
		name                 string
		filter               filter.Condition
		builder              sq.SelectBuilder
		expectedSql          string
		expectedArgs         []any
		expectedTableAliases []string
		errContains          string
		mapperFunc           FieldMapperFunc
	}{
		{
			name:        "nil condition",
			filter:      nil,
			builder:     psql.Select("*").From("users u"),
			expectedSql: "SELECT * FROM users u",
		},
		{
			name:        "empty where",
			filter:      filter.Where(nil),
			builder:     sq.Select("*").From("users"),
			expectedSql: "SELECT * FROM users",
		},
		{
			name: "where with equals",
			filter: filter.Where(
				filter.Equals("id", 1234),
			),
			builder:      psql.Select("*").From("users"),
			expectedSql:  "SELECT * FROM users WHERE id = $1",
			expectedArgs: []any{1234},
		},
		{
			name: "where with not equals",
			filter: filter.Where(
				filter.NotEquals("id", 1234),
			),
			builder:      psql.Select("*").From("users"),
			expectedSql:  "SELECT * FROM users WHERE id <> $1",
			expectedArgs: []any{1234},
		},
		{
			name: "where with equals and alias",
			filter: filter.Where(
				filter.Equals("id", 1234),
			),
			builder:              psql.Select("*").From("users u"),
			expectedSql:          "SELECT * FROM users u WHERE u.id = $1",
			expectedArgs:         []any{1234},
			expectedTableAliases: []string{"u"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("u.%s", fieldName), nil
			},
		},
		{
			name:        "equals with invalid field name",
			filter:      filter.Equals("notExistingFieldName", "abc"),
			errContains: "field name error",
			mapperFunc: func(fieldName string) (string, error) {
				if fieldName == "notExistingFieldName" {
					return "", fmt.Errorf("field name error")
				}
				return "", fmt.Errorf("should not be reached")
			},
		},
		{
			name:         "where with regex",
			filter:       filter.Where(filter.Regex("name", "frau$")),
			builder:      psql.Select("*").From("users"),
			expectedSql:  "SELECT * FROM users WHERE name ~ $1",
			expectedArgs: []any{"frau$"},
		},
		{
			name:         "where with not regex",
			filter:       filter.Where(filter.NotRegex("name", "mann$")),
			builder:      psql.Select("*").From("users"),
			expectedSql:  "SELECT * FROM users WHERE name !~ $1",
			expectedArgs: []any{"mann$"},
		},
		{
			name: "where with NOT",
			filter: filter.Where(
				filter.Not(
					filter.LowerThan("regressions", 5)),
			),
			builder:      psql.Select("*").From("images"),
			expectedSql:  "SELECT * FROM images WHERE NOT (regressions < $1)",
			expectedArgs: []any{5},
		},
		{
			name: "where with OR",
			filter: filter.Where(
				filter.Or(
					filter.Equals("label", "abc"),
					filter.Equals("label", "def"),
				),
			),
			builder:      psql.Select("*").From("images"),
			expectedSql:  "SELECT * FROM images WHERE (label = $1 OR label = $2)",
			expectedArgs: []any{"abc", "def"},
		},
		{
			name: "where with OR and alias",
			filter: filter.Where(
				filter.Or(
					filter.Equals("label", "abc"),
					filter.Equals("label", "def"),
				),
			),
			builder:              psql.Select("*").From("images i"),
			expectedSql:          "SELECT * FROM images i WHERE (i.label = $1 OR i.label = $2)",
			expectedArgs:         []any{"abc", "def"},
			expectedTableAliases: []string{"i"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("i.%s", fieldName), nil
			},
		},
		{
			name: "where with multiple ORs",
			filter: filter.Where(
				filter.Or(
					filter.Equals("label", "abc"),
					filter.Equals("label", "def"),
					filter.Equals("address_id", 123),
				),
			),
			builder:      psql.Select("*").From("names"),
			expectedSql:  "SELECT * FROM names WHERE (label = $1 OR label = $2 OR address_id = $3)",
			expectedArgs: []any{"abc", "def", 123},
		},
		{
			name: "where with multiple ORs and alias",
			filter: filter.Where(
				filter.Or(
					filter.Equals("label", "abc"),
					filter.Equals("label", "def"),
					filter.Equals("address_id", 123),
				),
			),
			builder:              psql.Select("*").From("names n"),
			expectedSql:          "SELECT * FROM names n WHERE (n.label = $1 OR n.label = $2 OR n.address_id = $3)",
			expectedArgs:         []any{"abc", "def", 123},
			expectedTableAliases: []string{"n"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("n.%s", fieldName), nil
			},
		},
		{
			name: "empty OR",
			filter: filter.Where(
				filter.Or(),
			),
			errContains: "OR condition must have at least two conditions",
		},
		{
			name: "single OR argument",
			filter: filter.Or(
				filter.Equals("a", "a"),
			),
			errContains: "OR condition must have at least two conditions",
		},
		{
			name:        "empty AND",
			filter:      filter.Where(filter.And()),
			errContains: "AND condition must have at least two conditions",
		},
		{
			name:        "single AND argument",
			filter:      filter.Where(filter.And(filter.Equals("a", "a"))),
			errContains: "AND condition must have at least two conditions",
		},
		{
			name: "multiple ANDs",
			filter: filter.Where(
				filter.And(
					filter.Equals("label", "abc"),
					filter.Equals("label", "def"),
					filter.Equals("label", "ghi"),
					filter.Equals("label", "jkl"),
				),
			),
			builder:              psql.Select("*").From("meds m"),
			expectedSql:          "SELECT * FROM meds m WHERE (m.label = $1 AND m.label = $2 AND m.label = $3 AND m.label = $4)",
			expectedArgs:         []any{"abc", "def", "ghi", "jkl"},
			expectedTableAliases: []string{"m"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("m.%s", fieldName), nil
			},
		},
		{
			name: "grouping",
			filter: filter.Where(
				filter.And(
					filter.Equals("a", 2),
					filter.Group(
						filter.Or(
							filter.Equals("b", "b"),
							filter.Equals("c", "c"),
							filter.GreaterThan("d", "e"),
							filter.GreaterThanOrEqual("f", "g"),
							filter.LowerThan("h", "i"),
							filter.LowerThanOrEqual("j", "k"),
						),
					),
				),
			),
			builder:              psql.Select("*").From("xxx"),
			expectedSql:          "SELECT * FROM xxx WHERE (aa.a = $1 AND (bbb.b = $2 OR c.c = $3 OR d.d > $4 OR f.f >= $5 OR h.h < $6 OR j.j <= $7))",
			expectedArgs:         []any{2, "b", "c", "e", "g", "i", "k"},
			expectedTableAliases: []string{"aa", "bbb", "c", "d", "f", "h", "j"},
			mapperFunc: func(fieldName string) (string, error) {
				switch fieldName {
				case "a":
					return "aa.a", nil
				case "b":
					return "bbb.b", nil
				default:
					return fmt.Sprintf("%s.%s", fieldName, fieldName), nil
				}
			},
		},
		{
			name: "contains",
			filter: filter.Where(
				filter.Contains("label", "abc"),
			),
			builder:      psql.Select("*").From("g"),
			expectedSql:  "SELECT * FROM g WHERE label ILIKE $1",
			expectedArgs: []any{"%abc%"},
		},
		{
			name: "contains with alias",
			filter: filter.Where(
				filter.Contains("label", "abc"),
			),
			builder:              psql.Select("*").From("g x"),
			expectedSql:          "SELECT * FROM g x WHERE x.label ILIKE $1",
			expectedArgs:         []any{"%abc%"},
			expectedTableAliases: []string{"x"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("x.%s", fieldName), nil
			},
		},
		{
			name:        "contains with invalid field name",
			filter:      filter.Contains("notExistingFieldName", "abc"),
			errContains: "field name error",
			mapperFunc: func(fieldName string) (string, error) {
				if fieldName == "notExistingFieldName" {
					return "", fmt.Errorf("field name error")
				}
				return "", fmt.Errorf("should not be reached")
			},
		},
		{
			name: "in with slice",
			filter: filter.Where(
				filter.In("id", []int{1, 2}),
			),
			builder:      psql.Select("*").From("x"),
			expectedSql:  "SELECT * FROM x WHERE id IN ($1,$2)",
			expectedArgs: []any{1, 2},
		},
		{
			name: "in with slice and alias",
			filter: filter.Where(
				filter.In("id", []int{1, 2}),
			),
			builder:              psql.Select("*").From("x y"),
			expectedSql:          "SELECT * FROM x y WHERE y.id IN ($1,$2)",
			expectedArgs:         []any{1, 2},
			expectedTableAliases: []string{"y"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("y.%s", fieldName), nil
			},
		},
		{
			name: "array contains",
			filter: filter.Where(
				filter.ArrayContains("ids", 4),
			),
			builder:      psql.Select("*").From("a"),
			expectedSql:  "SELECT * FROM a WHERE ids = ANY ($1)",
			expectedArgs: []any{4},
		},
		{
			name: "array contains with alias",
			filter: filter.Where(
				filter.ArrayContains("ids", 4),
			),
			builder:              psql.Select("*").From("a b"),
			expectedSql:          "SELECT * FROM a b WHERE b.ids = ANY ($1)",
			expectedArgs:         []any{4},
			expectedTableAliases: []string{"b"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("b.%s", fieldName), nil
			},
		},
		{
			name:        "is not nil",
			filter:      filter.Where(filter.NotNil("test")),
			builder:     psql.Select("*").From("foo"),
			expectedSql: "SELECT * FROM foo WHERE test IS NOT NULL",
		},
		{
			name:                 "is not nil and alias",
			filter:               filter.Where(filter.NotNil("test")),
			builder:              psql.Select("*").From("foo bar"),
			expectedSql:          "SELECT * FROM foo bar WHERE bar.test IS NOT NULL",
			expectedTableAliases: []string{"bar"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("bar.%s", fieldName), nil
			},
		},
		{
			name:         "overlaps",
			filter:       filter.Where(filter.Overlaps("tags", []int{1, 2, 3})),
			builder:      psql.Select("*").From("a"),
			expectedSql:  "SELECT * FROM a WHERE tags && ARRAY[$1,$2,$3]",
			expectedArgs: []any{1, 2, 3},
		},
		{
			name:                 "overlaps with alias",
			filter:               filter.Where(filter.Overlaps("tags", []int{1, 2, 3})),
			builder:              psql.Select("*").From("a"),
			expectedSql:          "SELECT * FROM a WHERE a.tags && ARRAY[$1,$2,$3]",
			expectedArgs:         []any{1, 2, 3},
			expectedTableAliases: []string{"a"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("a.%s", fieldName), nil
			},
		},
		{
			name:         "overlaps with single value",
			filter:       filter.Where(filter.Overlaps("tags", "test")),
			builder:      psql.Select("*").From("a"),
			expectedSql:  "SELECT * FROM a WHERE tags && ARRAY[$1]",
			expectedArgs: []any{"test"},
		},
		{
			name:                 "overlaps with single value and alias",
			filter:               filter.Where(filter.Overlaps("tags", "test")),
			builder:              psql.Select("*").From("a b"),
			expectedSql:          "SELECT * FROM a b WHERE b.tags && ARRAY[$1]",
			expectedArgs:         []any{"test"},
			expectedTableAliases: []string{"b"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("b.%s", fieldName), nil
			},
		},
		{
			name:        "overlaps with nil",
			filter:      filter.Where(filter.Overlaps("bar", nil)),
			builder:     psql.Select("*").From("x"),
			expectedSql: "SELECT * FROM x WHERE (1=0)",
		},
		{
			name:        "overlaps with empty slice",
			filter:      filter.Where(filter.Overlaps("bar", []int{})),
			builder:     psql.Select("*").From("x"),
			expectedSql: "SELECT * FROM x WHERE (1=0)",
		},
		{
			name:         "array contains array",
			filter:       filter.Where(filter.ArrayContainsArray("tags", []string{"a", "b"})),
			builder:      psql.Select("*").From("y"),
			expectedSql:  "SELECT * FROM y WHERE tags @> ARRAY[$1,$2]",
			expectedArgs: []any{"a", "b"},
		},
		{
			name:                 "array contains array with alias",
			filter:               filter.Where(filter.ArrayContainsArray("tags", []string{"a", "b"})),
			builder:              psql.Select("*").From("y z"),
			expectedSql:          "SELECT * FROM y z WHERE z.tags @> ARRAY[$1,$2]",
			expectedArgs:         []any{"a", "b"},
			expectedTableAliases: []string{"z"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("z.%s", fieldName), nil
			},
		},
		{
			name:         "array is contained",
			filter:       filter.Where(filter.ArrayIsContained("tags", []string{"d", "e", "f"})),
			builder:      psql.Select("*").From("abc"),
			expectedSql:  "SELECT * FROM abc WHERE tags <@ ARRAY[$1,$2,$3]",
			expectedArgs: []any{"d", "e", "f"},
		},
		{
			name:                 "array is contained with alias",
			filter:               filter.Where(filter.ArrayIsContained("tags", []string{"d", "e", "f"})),
			builder:              psql.Select("*").From("abc def"),
			expectedSql:          "SELECT * FROM abc def WHERE def.tags <@ ARRAY[$1,$2,$3]",
			expectedArgs:         []any{"d", "e", "f"},
			expectedTableAliases: []string{"def"},
			mapperFunc: func(fieldName string) (string, error) {
				return fmt.Sprintf("def.%s", fieldName), nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test := test
			mapperFunc := test.mapperFunc
			if mapperFunc == nil {
				mapperFunc = func(fieldName string) (string, error) {
					return fieldName, nil
				}
			}

			var opts []Option
			if test.mapperFunc != nil {
				opts = append(opts, WithMapperFunc(test.mapperFunc))
			}

			builder, tableAliases, err := ApplyFilter(test.builder, test.filter, opts...)

			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
				sql, args, err := builder.ToSql()
				require.NoError(t, err)
				require.Equal(t, test.expectedSql, sql)
				require.Equal(t, test.expectedArgs, args)
				assertEqualElements(t, test.expectedTableAliases, tableAliases)
			}
		})
	}
}

func assertEqualElements(t *testing.T, expected []string, actual []string) {
	expectedMap := make(map[string]bool)
	actualMap := make(map[string]bool)
	for _, v := range expected {
		expectedMap[v] = true
	}
	for _, v := range actual {
		actualMap[v] = true
	}
	require.Equal(t, expectedMap, actualMap)
}
