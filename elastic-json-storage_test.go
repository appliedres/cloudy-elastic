package elastic

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
)

// docker run -d --name elasticsearch  -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.14.2
var info = &ConnectionInfo{
	Endpoint: "http://localhost:9201",
}

func startDocker() error {
	fmt.Println("Starting Elasticsearch instance in docker for testing")
	cmd := exec.Command("podman", "run", "--rm", "--name", "cloudy-test-elasticsearch", "-e", "discovery.type=single-node", "-d", "-p", "9201:9200", "elasticsearch:7.14.2")
	var out bytes.Buffer
	var errs bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &errs
	err := cmd.Run()
	if err != nil {
		errstr := errs.String()
		if strings.Contains(errstr, "already in use") {
			fmt.Println("Already running")
		} else {
			fmt.Println(out.String())
			fmt.Println(errs.String())
			return err
		}
	}

	// check to see if available
	fmt.Printf("Waiting for %v to become avialable\n", info.Endpoint)
	found := cloudy.WaitForAddress(info.Endpoint, 60*time.Second)
	if !found {
		return errors.New("Unable to connect")
	}

	fmt.Println("Completed ElasticSearch Startup")
	return nil
}

func shutdownDocker() error {
	fmt.Println("Shutting down Elasticsearch")

	cmd := exec.Command("docker", "stop", "cloudy-test-elasticsearch")
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Println("Completed ElasticSearch Shutdown")
	return nil
}

func TestMain(m *testing.M) {

	// Write code here to run before tests
	err := startDocker()
	if err != nil {
		panic(err)
	}

	// Run tests
	exitVal := m.Run()

	// Write code here to run after tests
	// err = shutdownDocker()
	// if err != nil {
	// 	panic(err)
	// }

	// Exit with exit value from tests
	os.Exit(exitVal)
}

func TestJsonDataStore(t *testing.T) {
	ctx := cloudy.StartContext()

	ds := NewElasticJsonDataStore[datastore.TestItem](
		"test",
	)

	ds.Open(ctx, info)

	datastore.JsonDataStoreTest(t, ctx, ds)
}

func TestJsonDataStoreQuery(t *testing.T) {
	ctx := cloudy.StartContext()

	ds := NewElasticJsonDataStore[datastore.TestQueryItem](
		"testquery",
	)

	ds.Open(ctx, info)

	datastore.QueryJsonDataStoreTest(t, ctx, ds)
}
