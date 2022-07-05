package main

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/targets"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	ilpConn (*net.TCPConn)
}

func (p *processor) Init(numWorker int, _, _ bool) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", iginxILPBindTo)
	if err != nil {
		fatal("Failed to resolve %s: %s\n", iginxILPBindTo, err.Error())
	}
	p.ilpConn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fatal("Failed connect to %s: %s\n", iginxILPBindTo, err.Error())
	}
}

func (p *processor) Close(_ bool) {
	defer p.ilpConn.Close()
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if !doLoad {
		return 0, 0
	}

	lines := strings.Split(batch.buf.String(), "\n")
	lines = lines[0 : len(lines)-1]

	json := "["
	i := 0
	for i = 0; i < len(lines); i++ {
		tmp := strings.Split(lines[i], " ")
		tmp[0] = "type=" + tmp[0]
		fir := strings.Split(tmp[0], ",")
		var tags map[string]string
		tags = make(map[string]string)
		j := 0
		for j = 0; j < len(fir); j++ {
			kv := strings.Split(fir[j], "=")
			tags[kv[0]] = kv[1]
		}
		timestamp, _ := strconv.ParseInt(tmp[2], 10, 64)
		timestamp /= 1000000
		sec := strings.Split(tmp[1], ",")
		for j = 0; j < len(sec); j++ {
			// fmt.Sprintf("[DEBUG] name: %s",sec[j])
			kv := strings.Split(sec[j], "=")
			json += fmt.Sprintf(`{
      			"name": "%s",
      			"timestamp": %d,
      			"value": %s,
      			"tags": {`, kv[0], timestamp, kv[1])
			for key, value := range tags {
				json += fmt.Sprintf(`"%s" : "%s",`, key, value)
			}
			json = json[0 : len(json)-1]
			json += "}},"
		}
	}

	json = json[0 : len(json)-1]
	json += "]"
	fmt.Println(json)

	execQuery(iginxRESTEndPoint, json)
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func execQuery(uriRoot string, query string) (QueryResponse, error) {
	var qr QueryResponse
	if strings.HasSuffix(uriRoot, "/") {
		uriRoot = uriRoot[:len(uriRoot)-1]
	}
	uriRoot = uriRoot + "/api/v1/datapoints"
	resp, err := http.Post(uriRoot, "application/x-www-form-urlencoded", strings.NewReader(query))
	if err != nil {
		return qr, err
	}
	defer resp.Body.Close()
	return qr, nil
}
