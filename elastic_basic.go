package cloudyelastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/appliedres/cloudy"
	"github.com/cenkalti/backoff/v4"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	// "github.com/elastic/go-elasticsearch/v7/esutil"
)

// ConnectionInfo connection information
type ConnectionInfo struct {
	Endpoint string `json:"endpoint"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func NewClientFromEnv(env cloudy.Environment) (*elasticsearch.Client, error) {
	host := env.Force("ES_HOST")
	user := env.Force("ES_USER")
	pass := env.Force("ES_PASS")

	// Connect to elastic search
	return NewClient(&ConnectionInfo{Endpoint: host, Username: user, Password: pass})
}

// NewClient creates a new Client
func NewClient(info *ConnectionInfo) (*elasticsearch.Client, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},

		// Retry up to 5 attempts
		MaxRetries: 5,
		Addresses: []string{
			info.Endpoint,
		},
		Username: info.Username,
		Password: info.Password,
	})
	if err != nil {
		// fmt.Printf("retrieved secret: %v, %v\n", info.Endpoint, info.Username)
		panic(err)
		// return nil, fmt.Errorf("Error creating the cli ent: %s", err)
	}
	return es, nil
}

// Index an item in the elastic search
func Index(client *elasticsearch.Client, item interface{}, ID string, indexName string) error {

	// Build the request body.
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	return IndexData(client, data, ID, indexName)
}

func CreateIndex(client *elasticsearch.Client, indexName string) error {
	// Set up the request object.
	req := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}
	// Perform the request with the client.
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return fmt.Errorf("error getting response: %s", err)
	}
	if res.StatusCode == 200 {
		return nil
	}

	reqCreate := esapi.IndicesCreateRequest{
		Index: indexName,
	}
	res, err = reqCreate.Do(context.Background(), client)
	if err != nil {
		return fmt.Errorf("error getting response: %s", err)
	}
	if res.StatusCode >= 300 {
		message, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("error Creating index %v, %v", indexName, string(message))
	}

	return nil
}

// Index an item in the elastic search
func IndexData(client *elasticsearch.Client, data []byte, ID string, indexName string) error {
	// Set up the request object.
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: ID,
		Body:       strings.NewReader(string(data)),
		Refresh:    "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return fmt.Errorf("error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("[%s] Error indexing document ID=%v", res.Status(), ID)
	}

	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		fmt.Printf("Error parsing the response body: %s", err)
	} else {
		// Print the response status and indexed document version.
		fmt.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
	}

	return nil
}

func RemoveData(client *elasticsearch.Client, ID string, indexName string) error {
	// Set up the request object.
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: ID,
	}
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return fmt.Errorf("error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("[%s] Error indexing document ID=%v", res.Status(), ID)
	}

	return nil
}

// ElasticLoadByID Loads an item from Elastic Search
func LoadByID(client *elasticsearch.Client, ID string, index string) ([]byte, error) {
	query := fmt.Sprintf(`{
		"query": {
			"match": {
			  "_id": "%v"
			}
		}
	}`, ID)

	results, err := Query(client, index, query)
	if err != nil {
		return nil, err
	}

	jsonParsed, err := gabs.ParseJSON([]byte(results))
	value := jsonParsed.Path("hits.hits.0._source").Bytes()
	if value == nil || string(value) == "null" {
		return nil, nil
	}

	return value, err
}

// ElaticSearch basic elasic search
func Query(es *elasticsearch.Client, index string, query string) (string, error) {

	// Issue the search
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index),
		es.Search.WithBody(strings.NewReader(query)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// Query Elastic Search
func Count(es *elasticsearch.Client, index string) (string, error) {
	res, err := es.Count(
		es.Count.WithContext(context.Background()),
		es.Count.WithIndex(index),
	)
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func Hits(results string) int {
	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return 0
	}

	hitsObj := jsonParsed.Path("hits.total.value")
	if hitsObj != nil {
		hits := hitsObj.Data().(float64)
		return int(hits)
	}

	return -1
}

// GetIDsFromResults Gets a list of IDs from the elasic search results
func IDsFromResults(results string) []string {
	var rtn []string
	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return rtn
	}
	children := jsonParsed.Path("hits.hits").Children()
	for _, item := range children {
		id := item.S("_id").String()
		id = strings.ReplaceAll(id, "\"", "")
		rtn = append(rtn, id)
	}
	return rtn
}

func ValueFromResults(results string, name string) []string {
	var rtn []string
	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return rtn
	}
	children := jsonParsed.Path("hits.hits").Children()
	for _, item := range children {
		id := item.S("_source", name).String()
		id = strings.ReplaceAll(id, "\"", "")
		rtn = append(rtn, id)
	}
	return rtn
}

// GenerateIDQuery generates a query for all the ids
func GenerateIDQuery(ids []string) string {
	es := NewQuery()
	es.Size = 1000
	es.Query.Bool.Must.Terms("_id", ids...)
	es.NoSource = true

	return es.Build()
}

func ToError(results string) error {
	data, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return err
	}
	errorMsg := data.Path("error.root_cause.reason").String()
	if errorMsg != "null" {
		return fmt.Errorf("elastic error: %v", errorMsg)
	}

	return nil
}

// IsError determines if the results are an error
func IsError(results string) bool {
	return false
}

// ParseResults loads all the results as objects
func ParseResults(results string) ([][]byte, error) {
	var rtn [][]byte

	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return rtn, err
	}
	children := jsonParsed.Path("hits.hits").Children()
	for _, item := range children {
		source := item.S("_source").String()
		if err != nil {
			// record error
		} else {
			rtn = append(rtn, []byte(source))
		}
	}
	return rtn, nil
}

func ParseResultsTyped[T any](results string) ([]*T, error) {
	var rtn []*T

	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return rtn, err
	}
	children := jsonParsed.Path("hits.hits").Children()
	for _, item := range children {
		source := item.S("_source").String()
		if err != nil {
			// record error
		} else {
			item, err := cloudy.UnmarshallT[T]([]byte(source))
			if err != nil {
				return nil, err
			}
			rtn = append(rtn, item)
		}
	}
	return rtn, nil
}

func First(results string) ([]byte, error) {
	jsonParsed, err := gabs.ParseJSON([]byte(results))
	if err != nil {
		return nil, err
	}
	children := jsonParsed.Path("hits.hits").Children()
	if len(children) == 0 {
		return nil, nil
	}

	source := children[0].S("_source").Bytes()
	return source, nil
}

func Paths(container *gabs.Container, start string, currentPath string) []*JsonPath {
	var c1 *gabs.Container
	if start != "" {
		c1 = container.Path(start)
	} else {
		c1 = container
	}
	if c1 == nil {
		return nil
	}

	var rtn []*JsonPath
	for key, child := range c1.ChildrenMap() {
		properties := child.S("properties")
		if properties != nil {
			var path string
			if currentPath == "" {
				path = key
			} else {
				path = currentPath + "." + key
			}
			childPaths := Paths(child, "", path)
			rtn = append(rtn, childPaths...)
			continue
		}

		obj := child.S("type")
		if obj != nil {
			var path string
			if currentPath == "" {
				path = key
			} else {
				path = currentPath + "." + key
			}
			rtn = append(rtn, &JsonPath{
				Path: path,
				Type: obj.Data().(string),
			})
			// Check keywords
			keyword := child.Path("fields.keyword")
			if keyword != nil {
				rtn = append(rtn, &JsonPath{
					Path: path + ".keyword",
					Type: "keyword",
				})
			}
		} else {
			childPaths := Paths(child, "", currentPath)
			rtn = append(rtn, childPaths...)
		}
	}
	return rtn
}

type JsonPath struct {
	Path string
	Type string
}
