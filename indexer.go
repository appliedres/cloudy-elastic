package elastic

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v7"
)

type ESIndexer struct {
	IndexName    string
	Client       *elasticsearch.Client
	SkipIndexing bool
}

func NewIndexer(index string, skipIndexing bool) *ESIndexer {

	idx := new(ESIndexer)
	idx.IndexName = index
	idx.SkipIndexing = skipIndexing
	return idx
}

func (es *ESIndexer) Open(ctx context.Context, config interface{}) error {
	conn := config.(*ConnectionInfo)
	if conn == nil {
		return fmt.Errorf("Invalid or missing configuration")
	}

	client, err := NewClient(conn)
	if err != nil {
		return err
	}
	es.Client = client

	// Try to create the index
	err = CreateIndex(client, es.IndexName)
	if err != nil {
		return err
	}

	return nil
}

func (es *ESIndexer) Close(ctx context.Context) error {
	return nil
}

func (es *ESIndexer) Index(ctx context.Context, id string, data []byte) error {
	if !es.SkipIndexing {
		err := IndexData(es.Client, data, id, es.IndexName)
		return err
	}
	return nil
}

func (es *ESIndexer) Remove(ctx context.Context, id string) error {
	if !es.SkipIndexing {
		err := RemoveData(es.Client, id, es.IndexName)
		return err
	}
	return nil
}

func (es *ESIndexer) Search(ctx context.Context, query interface{}) (interface{}, error) {
	return Query(es.Client, es.IndexName, query.(string))
}
