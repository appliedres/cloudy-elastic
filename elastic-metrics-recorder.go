package cloudyelastic

import (
	"context"
	"fmt"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/metrics"
	"github.com/appliedres/cloudy/vm"
	"github.com/elastic/go-elasticsearch/v7"
)

type ElasticMetricRecorder struct {
	client *elasticsearch.Client
}

func NewElasticMetricRecorder(ctx context.Context, conn *ConnectionInfo) (*ElasticMetricRecorder, error) {
	client, err := NewClient(conn)
	if err != nil {
		return nil, err
	}
	return &ElasticMetricRecorder{
		client: client,
	}, nil
}

func NewElasticMetricRecorderFromEnv(ctx context.Context, env *cloudy.Environment) (*ElasticMetricRecorder, error) {
	host := env.Get("ES_HOST")
	user := env.Get("ES_USER")
	pass := env.Get("ES_PASS")

	return NewElasticMetricRecorder(ctx, &ConnectionInfo{Endpoint: host, Username: user, Password: pass})
}

func (rec *ElasticMetricRecorder) RecordVMStatus(ctx context.Context, metric *metrics.Metric[*vm.VirtualMachineStatus]) error {
	status := metric.Value

	// VM Status is stored in the index "vmstatus"
	id := fmt.Sprintf("%v-%v", status.ID, time.Now().Unix())
	return Index(rec.client, status, id, "vmstatus")
}
