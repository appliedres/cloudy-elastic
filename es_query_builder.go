package elastic

import (
	"fmt"

	"github.com/Jeffail/gabs/v2"
)

type ElasticSearchQueryBuilder struct {
	Size     int
	From     int
	Source   []string
	NoSource bool
	Sort     []*Sort
	Query    *QueryBuilder
}

type Builder interface {
	Build(parent *gabs.Container)
}

type BooleanCollector struct {
	Must             *ConditionCollector
	MustNot          *ConditionCollector
	Should           *ConditionCollector
	MinShouldInclude int
	Filter           *ConditionCollector
}

func NewQuery() *ElasticSearchQueryBuilder {
	q := &ElasticSearchQueryBuilder{}
	q.Size = -1
	q.Query = NewQueryBuilder()
	return q
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		Bool:      NewBooleanCollector(),
		Collector: NewConditionCollector(),
	}
}

func NewBooleanCollector() *BooleanCollector {
	return &BooleanCollector{
		Must:             NewConditionCollector(),
		MustNot:          NewConditionCollector(),
		Should:           NewConditionCollector(),
		Filter:           NewConditionCollector(),
		MinShouldInclude: 1,
	}
}

func NewConditionCollector() *ConditionCollector {
	return &ConditionCollector{}
}

func (es *ElasticSearchQueryBuilder) Build() string {
	container := es.BuildContainer()
	return container.StringIndent("", "   ")
}

func (es *ElasticSearchQueryBuilder) Print() {
	fmt.Println(es.Build())
}

func (es *ElasticSearchQueryBuilder) BuildContainer() *gabs.Container {
	root := gabs.New()
	if es.Size > -1 {
		root.Set(es.Size, "size")
	}

	if len(es.Source) > 0 {
		for _, src := range es.Source {
			root.ArrayAppend(src, "_source")
		}
	}
	if es.NoSource {
		root.Set(false, "_source")
	}

	if es.From > 0 {
		root.Set(es.From, "from")
	}

	if es.Query != nil && es.Query.Valid() {
		es.Query.Build(root)
	}
	return root
}

func (es *ElasticSearchQueryBuilder) AddSort(field string, direction string) {
	es.Sort = append(es.Sort, &Sort{field, direction})
}

func (es *ElasticSearchQueryBuilder) SetSort(field string, direction string) {
	es.Sort = []*Sort{
		&Sort{field, direction},
	}
}

type Sort struct {
	field     string
	direction string
}

// ---------------

type QueryBuilder struct {
	Bool           *BooleanCollector
	MatchAll       bool
	MatchCondition *MatchCondition
	RangeCondition *RangeCondition
	Collector      *ConditionCollector
}

func (qb *QueryBuilder) Match(field string, value string) {
	qb.MatchCondition = &MatchCondition{
		Field: field,
		Value: value,
	}
}

func (qb *QueryBuilder) Range(field string, gte string, lte string) {
	qb.Collector.Range(field, gte, lte)
}

func (qb *QueryBuilder) Build(parent *gabs.Container) {
	var q struct{}
	query := gabs.New()
	if qb.Bool != nil && qb.Bool.Valid() {
		qb.Bool.Build(query)
	} else if qb.Collector.Valid() {
		qb.Collector.Build(query)
	} else if qb.MatchCondition != nil {
		qb.MatchCondition.Build(query)
	} else if qb.MatchAll {
		query.Set(q, "match_all")
	}
	parent.Set(query, "query")
}

func (qb *QueryBuilder) Valid() bool {
	return (qb.Bool != nil && qb.Bool.Valid()) || (qb.MatchCondition != nil) || qb.MatchAll
}

// ---------------
func (c *BooleanCollector) Valid() bool {
	return c.Must.Valid() || c.Should.Valid() || c.MustNot.Valid() || c.Filter.Valid()
}

func (c *BooleanCollector) Build(parent *gabs.Container) {
	if c.Must.Valid() {
		arr := c.Must.BuildArray()
		for _, v := range arr {
			parent.ArrayAppend(v, "bool", "must")
		}
	}
	if c.Should.Valid() {
		arr := c.Should.BuildArray()
		for _, v := range arr {
			parent.ArrayAppend(v, "bool", "should")
		}
		if c.MinShouldInclude > -1 {
			parent.Set(c.MinShouldInclude, "bool", "minimum_should_match")
		}
	}
	if c.MustNot.Valid() {
		arr := c.MustNot.BuildArray()
		for _, v := range arr {
			parent.ArrayAppend(v, "bool", "must_not")
		}
	}
	if c.Filter.Valid() {
		arr := c.Filter.BuildArray()
		for _, v := range arr {
			parent.ArrayAppend(v, "bool", "filter")
		}
	}
}

// ---------------

type MultiMatchCondition struct {
	Query     string
	Fields    []string
	Fuzziness string
}

func (m *MultiMatchCondition) Build(parent *gabs.Container) {
	cond := gabs.New()
	cond.Set(m.Query, "query")
	cond.Set(m.Fields, "fields")
	if m.Fuzziness != "" {
		cond.Set(m.Fuzziness, "fuzziness")
	}
	parent.Set(cond, "multi_match")
}

type MatchCondition struct {
	Field string
	Value string
}

func (m *MatchCondition) Build(parent *gabs.Container) {
	cond := gabs.New()
	cond.Set(m.Value, m.Field)
	parent.Set(cond, "match")
}

type TermsCondition struct {
	Field  string
	Values []string
}

func (m *TermsCondition) Build(parent *gabs.Container) {
	cond := gabs.New()
	for _, term := range m.Values {
		cond.ArrayAppend(term, m.Field)
	}
	parent.Set(cond, "terms")
}

type ExistsCondition struct {
	Field string
}

func (m *ExistsCondition) Build(parent *gabs.Container) {
	cond := gabs.New()
	cond.Set(m.Field, "field")
	parent.Set(cond, "exists")
}

type RangeCondition struct {
	Field string
	Lte   string
	Gte   string
	Lt    string
	Gt    string
}

func (c *RangeCondition) Build(parent *gabs.Container) {
	rangCond := gabs.New()
	if c.Gte != "" {
		rangCond.Set(c.Gte, "gte")
	}
	if c.Lte != "" {
		rangCond.Set(c.Lte, "lte")
	}
	if c.Gt != "" {
		rangCond.Set(c.Gt, "gt")
	}
	if c.Lt != "" {
		rangCond.Set(c.Lt, "lt")
	}
	cond := gabs.New()
	cond.Set(rangCond, c.Field)

	parent.Set(cond, "range")
}

type ConditionCollector struct {
	conditions []Builder
}

func (cc *ConditionCollector) Add(condition Builder) {
	cc.conditions = append(cc.conditions, condition)
}

func (cc *ConditionCollector) BuildArray() []*gabs.Container {
	var rtn []*gabs.Container
	for _, builder := range cc.conditions {
		container := gabs.New()
		builder.Build(container)
		rtn = append(rtn, container)
	}
	return rtn
}

func (cc *ConditionCollector) Build(parent *gabs.Container) {
	for _, c := range cc.conditions {
		c.Build(parent)
	}
}

func (cc *ConditionCollector) Valid() bool {
	return len(cc.conditions) > 0
}

func (cc *ConditionCollector) Match(field string, value string) {
	match := &MatchCondition{
		Field: field,
		Value: value,
	}
	cc.Add(match)
}

func (cc *ConditionCollector) Exists(field string) {
	exist := &ExistsCondition{
		Field: field,
	}
	cc.Add(exist)
}

func (cc *ConditionCollector) Range(field string, gte string, lte string) {
	rng := &RangeCondition{
		Field: field,
		Gte:   gte,
		Lte:   lte,
	}
	cc.Add(rng)
}

func (cc *ConditionCollector) RangeExt(field string, gte string, lte string, gt string, lt string) {
	rng := &RangeCondition{
		Field: field,
		Gte:   gte,
		Lte:   lte,
		Gt:    gt,
		Lt:    lt,
	}
	cc.Add(rng)
}

func (cc *ConditionCollector) Terms(field string, values ...string) {
	terms := &TermsCondition{
		Field:  field,
		Values: values,
	}
	cc.Add(terms)
}

func (cc *ConditionCollector) MultiMatch(fields []string, query string, fuzziness string) {
	terms := &MultiMatchCondition{
		Fields:    fields,
		Query:     query,
		Fuzziness: fuzziness,
	}
	cc.Add(terms)
}
