package elastic

// import (
// 	"reflect"

// 	"github.com/Jeffail/gabs/v2"
// )

// func ParseQuery(queryIn string) (*ElasticSearchQueryBuilder, error) {
// 	parsed, err := gabs.ParseJSON([]byte(queryIn))
// 	if err != nil {
// 		return nil, err
// 	}

// 	q := &ElasticSearchQueryBuilder{}

// 	SizeParser{}.Parse(parsed, q)
// 	SourceParser{}.Parse(parsed, q)
// 	QueryParser{}.Parse(parsed, q)

// 	return q, nil
// }

// // UTILS
// func getJsonInt(container *gabs.Container) int {
// 	if container == nil {
// 		return 0
// 	}
// 	flt := container.Data().(float64)
// 	return int(flt)
// }

// func firstProperty(container *gabs.Container) (string, *gabs.Container) {
// 	children := container.ChildrenMap()
// 	if len(children) == 0 {
// 		return "", nil
// 	}
// 	for key, child := range children {
// 		return key, child
// 	}
// 	return "", nil
// }

// func firstProperyAndValue(container *gabs.Container) (string, string, bool) {
// 	key, first := firstProperty(container)
// 	if first == nil {
// 		return "", "", false
// 	}
// 	val := first.Data().(string)
// 	return key, val, true
// }

// // PARSERS

// type SizeParser struct{}

// func (p SizeParser) Parse(parsed *gabs.Container, q *ElasticSearchQueryBuilder) {
// 	if parsed.Path("size") != nil {
// 		q.Size = getJsonInt(parsed.Path("size"))
// 	}
// }

// type SourceParser struct{}

// func (p SourceParser) Parse(parsed *gabs.Container, q *ElasticSearchQueryBuilder) {
// 	if parsed.Exists("_source") == false {
// 		return
// 	}

// 	result := parsed.Path("_source").Data()
// 	datatype := reflect.TypeOf(result).Kind()

// 	if datatype == reflect.Bool {
// 		// boolVal := result.(bool)
// 		// DONT KNOW WHAT TO DO
// 	} else if datatype == reflect.String {
// 		q.Source = []string{result.(string)}
// 	} else if datatype == reflect.Array {
// 		q.Source = result.([]string)
// 	} else {
// 		items := result.([]interface{})
// 		var vals []string
// 		for _, item := range items {
// 			vals = append(vals, item.(string))
// 		}
// 		q.Source = vals
// 	}
// }

// type SortParser struct{}

// func (p SortParser) Parse(jsonParsed *gabs.Container, q *ElasticSearchQueryBuilder) {

// }

// type QueryParser struct{}

// func (p QueryParser) Parse(jsonParsed *gabs.Container, q *ElasticSearchQueryBuilder) {
// 	qb := NewQueryBuilder()

// 	query := jsonParsed.S("query")
// 	if query == nil {
// 		return
// 	}

// 	boolCond := BoolParser{}.Parse(query)
// 	if boolCond != nil {
// 		qb.Bool = boolCond
// 	}

// 	matchCond := MatchConditionParser{}.Parse(query)
// 	if matchCond != nil {
// 		qb.Quer = matchCond
// 	}

// 	if query.S("match_all") != nil {
// 		qb.MatchAll()
// 	}

// 	q.Query = qb
// }

// type BoolParser struct{}

// func (p BoolParser) Parse(jsonParsed *gabs.Container) *BooleanCollector {
// 	boolCondJson := jsonParsed.S("bool")
// 	if boolCondJson == nil {
// 		return nil
// 	}
// 	boolCond := NewBooleanCollector()
// 	condParser := ConditionCollectorParser{}

// 	must := condParser.Parse(boolCondJson.S("must"))
// 	should := condParser.Parse(boolCondJson.S("should"))
// 	must_not := condParser.Parse(boolCondJson.S("must_not"))
// 	filter := condParser.Parse(boolCondJson.S("filter"))

// 	if must != nil {
// 		boolCond.Must = must
// 	}
// 	if should != nil {
// 		boolCond.Should = should
// 	}
// 	if must_not != nil {
// 		boolCond.MustNot = must_not
// 	}
// 	if filter != nil {
// 		boolCond.Filter = filter
// 	}
// 	if boolCondJson.S("minimum_should_include") != nil {
// 		boolCond.MinShouldInclude = getJsonInt(boolCondJson.S("minimum_should_include"))
// 	}

// 	return boolCond
// }

// type ConditionCollectorParser struct{}

// func (p ConditionCollectorParser) Parse(condJson *gabs.Container) *ConditionCollector {

// 	if condJson == nil {
// 		return nil
// 	}
// 	cond := NewConditionCollector()

// 	for _, child := range condJson.Children() {
// 		// Bool, Match, Range
// 		boolCond := BoolParser{}.Parse(child)
// 		if boolCond != nil {
// 			cond.boolCollector = boolCond
// 			cond.Add(boolCond)
// 		}

// 		matchCond := MatchConditionParser{}.Parse(child)
// 		if matchCond != nil {
// 			cond.Add(matchCond)
// 		}
// 		rangeCond := RangeConditionParser{}.Parse(child)
// 		if rangeCond != nil {
// 			cond.Add(rangeCond)
// 		}

// 	}

// 	return cond
// }

// type MatchConditionParser struct{}

// func (p MatchConditionParser) Parse(condJson *gabs.Container) *MatchCondition {
// 	matchCondJson := condJson.S("match")
// 	if matchCondJson == nil {
// 		return nil
// 	}
// 	match := &MatchCondition{}
// 	field, value, ok := firstProperyAndValue(matchCondJson)
// 	if ok {
// 		match.Field = field
// 		match.Value = value
// 	}
// 	return nil
// }

// type RangeConditionParser struct{}

// func (p RangeConditionParser) Parse(condJson *gabs.Container) *RangeConditon {
// 	rangeCondJson := condJson.S("range")
// 	if rangeCondJson == nil {
// 		return nil
// 	}
// 	rangeCond := &RangeConditon{}
// 	field, fieldJson := firstProperty(rangeCondJson)
// 	rangeCond.Field = field
// 	gte := fieldJson.S("gte")
// 	lte := fieldJson.S("lte")

// 	if gte != nil {
// 		rangeCond.Gte = gte.Data().(float64)
// 	}

// 	if lte != nil {
// 		rangeCond.Lte = lte.Data().(float64)
// 	}
// 	return rangeCond
// }

// type AggParser struct{}

// func (p AggParser) Parse(jsonParsed *gabs.Container, q ElasticSearchQueryBuilder) {

// }
