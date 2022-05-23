package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"

	"github.com/elastic/go-elasticsearch/v7"
)

type ElasticJsonDataStore[T any] struct {
	Client *elasticsearch.Client
	Index  string
	Model  interface{}
}

func NewElasticJsonDataStore[T any](index string) *ElasticJsonDataStore[T] {
	es := &ElasticJsonDataStore[T]{
		Index: index,
	}
	return es
}

// func (st *ElasticJsonDataStore[T]) MapToConfig(m map[string]interface{}) (interface{}, error) {
// 	cfgMap := m.(map[string]interface{})
// 	if cfgMap == nil {
// 		return nil, fmt.Errorf("Invalid or missing configuration.. Not a map")
// 	}

// 	conn := &ConnectionInfo{
// 		Endpoint: cloudy.MapKey(cfgMap, "endpoint"),
// 		Username: cloudy.MapKey(cfgMap, "username"),
// 		Password: cloudy.MapKey(cfgMap, "password"),
// 	}

// 	return conn, nil
// }

func (st *ElasticJsonDataStore[T]) Open(ctx context.Context, config interface{}) error {
	var conn *ConnectionInfo
	var err error

	conn = config.(*ConnectionInfo)
	if conn == nil {
		return fmt.Errorf("invalid or missing configuration")
	}

	client, err := NewClient(conn)
	if err != nil {
		return err
	}
	st.Client = client

	// Check if the index exists
	resp, err := client.Indices.Exists([]string{st.Index})
	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		return nil
	}

	// Create the index
	createResp, err := client.Indices.Create(st.Index)
	if err != nil {
		return err
	}

	if createResp.StatusCode > 201 {
		data, _ := ioutil.ReadAll(createResp.Body)
		return fmt.Errorf("error creating index %v, %v", st.Index, string(data))
	}

	return nil
}

func (st *ElasticJsonDataStore[T]) Close(ctx context.Context) error {
	// Nothing to do
	return nil
}

// Saves an item into the Elastic Search. This item MUST be JSON data.
// The key is used as the ID for the document and is required to be unique
// for this index
func (st *ElasticJsonDataStore[T]) Save(ctx context.Context, item *T, key string) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return IndexData(st.Client, data, key, st.Index)
}

func (st *ElasticJsonDataStore[T]) Get(ctx context.Context, key string) (*T, error) {
	data, err := LoadByID(st.Client, key, st.Index)
	if err != nil {
		return nil, err
	}

	model, err := cloudy.UnmarshallT[T](data)

	return model, err
}

func (st *ElasticJsonDataStore[T]) GetAll(ctx context.Context) ([]*T, error) {

	query := NewQuery()
	query.Size = 10000
	query.Query.MatchAll = true

	results, err := Query(st.Client, st.Index, query.Build())
	if err != nil {
		return nil, err
	}

	items, err := ParseResultsTyped[T](results)

	return items, err
}

func (st *ElasticJsonDataStore[T]) Exists(ctx context.Context, key string) (bool, error) {
	idQuery := GenerateIDQuery([]string{key})

	results, err := Query(st.Client, st.Index, idQuery)
	if err != nil {
		return false, err
	}
	if IsError(results) {
		return false, ToError(results)
	}

	return Hits(results) > 0, nil
}

func (st *ElasticJsonDataStore[T]) Delete(ctx context.Context, key string) error {
	return RemoveData(st.Client, key, st.Index)
}

func (st *ElasticJsonDataStore[T]) Query(ctx context.Context, query *datastore.SimpleQuery) ([]*T, error) {
	esQuery := new(ElasticQueryConverter).Convert(query)
	results, err := Query(st.Client, st.Index, esQuery)
	if err != nil {
		return nil, err
	}
	return ParseResultsTyped[T](results)
}

type ElasticQueryConverter struct {
}

func (qc *ElasticQueryConverter) Convert(c *datastore.SimpleQuery) string {
	q := NewQuery()
	qc.ConvertSelect(c, q)

	return q.Build()
}

func (qc *ElasticQueryConverter) ConvertSelect(c *datastore.SimpleQuery, q *ElasticSearchQueryBuilder) {

	if len(c.Colums) == 0 {
		// Nothing
	} else {
		q.Source = c.Colums
	}

	if c.Size > 0 {
		q.Size = c.Size
	}

	if c.Offset > 0 {
		q.From = c.Offset
	}
}

func (qc *ElasticQueryConverter) ConvertSort(sortbys []*datastore.SortBy, q *ElasticSearchQueryBuilder) {
	if len(sortbys) == 0 {
		return
	}
	for _, sortBy := range sortbys {
		qc.ConvertASort(sortBy, q)
	}
}
func (qc *ElasticQueryConverter) ConvertASort(c *datastore.SortBy, q *ElasticSearchQueryBuilder) {
	if c.Descending {
		q.AddSort(c.Field, "DESC")
	} else {
		q.AddSort(c.Field, "ASC")
	}
}

func (qc *ElasticQueryConverter) ConvertCondition(c *datastore.SimpleQueryCondition, collector *ConditionCollector) {
	field := c.Data[0]
	switch c.Type {
	case "eq":
		collector.Match(field, c.Data[1])
	case "neq":
		// "???"
	case "between":
		collector.Range(field, c.Data[1], c.Data[2])
	case "lt":
		collector.RangeExt(field, "", "", "", c.Data[1])
	case "lte":
		collector.Range(field, "", c.Data[1])
	case "gt":
		collector.RangeExt(field, "", "", c.Data[1], "")
	case "gte":
		collector.Range(field, c.Data[1], "")
	case "?":
		// return fmt.Sprintf("(data->>'%v')::numeric  ? '%v'", field, c.Data[1])
	case "contains":
		collector.Match(field, c.Data[1])
	}

}

func (qc *ElasticQueryConverter) ConvertConditionGroup(cg *datastore.SimpleQueryConditionGroup, bg *BooleanCollector) {
	if len(cg.Conditions) == 0 && len(cg.Groups) == 0 {
		return
	}

	for _, c := range cg.Conditions {
		if cg.Operator == "and" {
			collector := bg.Must
			qc.ConvertCondition(c, collector)
		} else if cg.Operator == "or" {
			collector := bg.Should
			qc.ConvertCondition(c, collector)
			bg.MinShouldInclude = 1
		}
	}
	for _, c := range cg.Groups {
		// Create new collector
		newBg := NewBooleanCollector()
		qc.ConvertConditionGroup(c, newBg)
		bg.Must.Add(newBg)
	}
}
