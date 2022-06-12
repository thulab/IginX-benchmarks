package iginx

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	iotReadingsTable = "readings"
)

// IoT produces TimescaleDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	panicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	influxql := fmt.Sprintf(`SELECT "name", "driver", "latitude", "longitude" 
		FROM "readings" 
		WHERE %s 
		ORDER BY "time" 
		LIMIT 1`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "Influx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {

	influxql := fmt.Sprintf(`SELECT "latitude", "longitude" 
		FROM "readings" 
		WHERE "fleet"='%s' 
		GROUP BY "name","driver" 
		ORDER BY "time" 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "Influx last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}