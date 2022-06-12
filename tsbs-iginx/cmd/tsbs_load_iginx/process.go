package main

import (
	"fmt"
	"github.com/thulab/iginx-client-go/rpc"
	"log"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/targets"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {}

func (p *processor) Init(numWorker int, _, _ bool) {
}

func (p *processor) Close(_ bool) {
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if !doLoad {
		return 0, 0
	}

	lines := strings.Split(batch.buf.String(), "\n")
	lines = lines[0 : len(lines)-1]

	var path []string
	var timestamp int64
	var values [][]interface{}
	var types []rpc.DataType

	i := 0
	for i = 0; i < len(lines); i++ {
		tmp := strings.Split(lines[i], " ")
		tmp[0] = "type=" + tmp[0]
		fir := strings.Split(tmp[0], ",")
		device := ""
		for j := 0; j < len(fir); j++ {
			kv := strings.Split(fir[j], "=")
			device += kv[1]
			device += "."
		}
		timestamp, _ := strconv.ParseInt(tmp[2], 10, 64)
		timestamp /= 1000000
		device = device[0 : len(device)-1]

		sec := strings.Split(tmp[1], ",")
		for j := 0; j < len(sec); j++ {
			kv := strings.Split(sec[j], "=")
			path = append(path, device + "." + kv[0])
			v, err := strconv.ParseFloat(kv[1],32)
			if err!=nil{
				log.Fatal(err)
			}
			values = append(values, []interface{}{v})
			types = append(types, rpc.DataType_DOUBLE)
		}
	}
	timestamps := []int64{timestamp}

	if err := session.InsertColumnRecords(path, timestamps, values, types); err!= nil{
		log.Fatal(err)
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}
