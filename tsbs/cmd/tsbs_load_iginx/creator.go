package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type dbCreator struct {
	iginxRESTEndPoint string
}

func (d *dbCreator) Init() {
	d.iginxRESTEndPoint = iginxRESTEndPoint
}

func (d *dbCreator) DBExists(dbName string) bool {
	r, err := execQuery(iginxRESTEndPoint, "SHOW TABLES")
	if err != nil {
		panic(fmt.Errorf("fatal error, failed to query iginx: %s", err))
	}
	for i, v := range r.Dataset {
		if i >= 0 && v[0] == "cpu" {
			panic(fmt.Errorf("fatal error, cpu table already exists"))
		}
	}

	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset [][]interface{}
	Count   int
	Error   string
}

func execQuery(uriRoot string, query string) (QueryResponse, error) {
	var qr QueryResponse
	if strings.HasSuffix(uriRoot, "/") {
		uriRoot = uriRoot[:len(uriRoot)-1]
	}
	uriRoot = uriRoot + "/exec?query=" + url.QueryEscape(query)
	resp, err := http.Get(uriRoot)
	if err != nil {
		return qr, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return qr, err
	}
	err = json.Unmarshal(body, &qr)
	if err != nil {
		return qr, err
	}
	if qr.Error != "" {
		return qr, errors.New(qr.Error)
	}
	return qr, nil
}
