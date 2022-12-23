// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/thulab/iginx-client-go/client"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.IginxPool, newProcessor)
}

type processor struct {
	session *client.Session
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	if workerNumber%2 == 0 {
		p.session = client.NewSession("172.16.17.11", "6888", "root", "root")
	} else if workerNumber%2 == 1 {
		p.session = client.NewSession("172.16.17.13", "6888", "root", "root")
	}
	if err := p.session.Open(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.Iginx)
	lag, err := Do(hq, p.session)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset []interface{}
	Count   int
	Error   string
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func Do(q *query.Iginx, session *client.Session) (lag float64, err error) {
	sql := string(q.SqlQuery)
	start := time.Now()
	// execute sql
	_, err = session.ExecuteSQL(sql)

	if err != nil {
		panic(err)
	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	return lag, err
}
