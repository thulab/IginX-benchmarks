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
	iotDiagnostics ="diagnostics"
)

// IoT produces IginX-specific queries for all the iot query types.
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
	var name=i.getTruckWhereString(nTrucks)
	json := fmt.Sprintf(`
			{
			"start_absolute":1,
			"end_relative": {
				"value": "5",	
				"unit": "days"
			},
			"metrics": [
			{
				"name":"longitude",
				"aggregators":[
					{
						"name":"last",
						"sampling":{
							"value":5,
							"unit":"days"
						}
					}
				],
				"tags":{
					"type":["readings"],
					"name":["%s"]
				}
			},
			{
				"name":"latitude",
				"aggregators":[
					{
						"name":"last",
						"sampling":{
							"value":5,
							"unit":"days"
						}
					}
				],
				"tags":{
					"type":["readings"],
					"name":["%s"]
				}
			}]
			}
			`,name,name)

	humanLabel := "Iginx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	var fleet=i.GetRandomFleet()
	json := fmt.Sprintf(`
			{
			"start_absolute":1,
			"end_relative": {
				"value": "5",	
				"unit": "days"
			},
			"metrics": [
			{
				"name":"longitude",
				"aggregators":[
					{
						"name":"last",
						"sampling":{
							"value":5,
							"unit":"days"
						}
					}
				],
				"tags":{
					"type":["readings"],
					"fleet":["%s"]
				}
			},
			{
				"name":"latitude",
				"aggregators":[
					{
						"name":"last",
						"sampling":{
							"value":5,
							"unit":"days"
						}
					}
				],
				"tags":{
					"type":["readings"],
					"fleet":["%s"]
				}
			}]
			}
			`,fleet,fleet)

	humanLabel := "Iginx last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	json := fmt.Sprintf(`
			{
			"start_absolute":1,
			"end_relative": {
				"value": "5",	
				"unit": "days"
			},
			"metrics": [
			{
				"name":"fuel_state",
				"aggregators":[
					{
						"name":"filter",
						"filter_op":"lte",
						"threshold":"0.1"
					}
				],
				"tags":{
					"type":["diagnostics"],
					"fleet":["%s"]
				}
			}]
			}
			`,i.GetRandomFleet())

	humanLabel := "Iginx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	// iginxql := fmt.Sprintf("SELECT current_load, load_capacity FROM diagnostics.*.%s.*",
	// 	i.GetRandomFleet())
	var fleet=i.GetRandomFleet()

		json := fmt.Sprintf(`
		{
		"start_absolute":1,
		"end_relative": {
			"value": "5",	
			"unit": "days"
		},
		"metrics": [
		{
			"name":"current_load",
			"tags":{
				"type":["diagnostics"],
				"fleet":["%s"]
			}
		},
		{
			"name":"load_capacity",
			"tags":{
				"type":["diagnostics"],
				"fleet":["%s"]
			}
		}]
		}
		`,fleet,fleet)

	humanLabel := "Iginx trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	// iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.* where time >=%d and time <= %d",
	// 	i.GetRandomFleet(), interval.Start().Unix()*1000, interval.End().Unix()*1000)

	json := fmt.Sprintf(`
		{
		"start_absolute": %d,
		"end_absolute": %d,
		"metrics": [
		{
			"name":"velocity",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":5,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,interval.Start().Unix()*1000,interval.End().Unix()*1000,i.GetRandomFleet())
	
	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	// iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.* GROUP [%d, %d] BY 10ms",
	// 	i.GetRandomFleet(), interval.Start().Unix(), interval.End().Unix())

	json := fmt.Sprintf(`
		{
		"start_absolute": %d,
		"end_absolute": %d,
		"metrics": [
		{
			"name":"velocity",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":10,
						"unit":"milliseconds"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,interval.Start().Unix(),interval.End().Unix(),i.GetRandomFleet())

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	// iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.* GROUP [%d, %d] BY 10ms",
	// 	i.GetRandomFleet(), interval.Start().Unix(), interval.End().Unix())

	json := fmt.Sprintf(`
		{
		"start_absolute": %d,
		"end_absolute": %d,
		"metrics": [
		{
			"name":"velocity",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":10,
						"unit":"milliseconds"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,interval.Start().Unix(),interval.End().Unix(),i.GetRandomFleet())

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	// iginxql := fmt.Sprintf("SELECT AVG(fuel_consumption) FROM readings.*.%s.*", i.GetRandomFleet())

	json := fmt.Sprintf(`
		{
		"start_absolute":1,
		"end_relative": {
			"value": "5",	
			"unit": "days"
		},
		"metrics": [
		{
			"name":"fuel_consumption",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":5,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,i.GetRandomFleet())

	humanLabel := "Iginx average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	// iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.*", i.GetRandomFleet())

	json := fmt.Sprintf(`
		{
		"start_absolute":1,
		"end_relative": {
			"value": "5",	
			"unit": "days"
		},
		"metrics": [
		{
			"name":"velocity",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":5,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,i.GetRandomFleet())

	humanLabel := "Iginx average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	// iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.*", i.GetRandomFleet())

	json := fmt.Sprintf(`
		{
		"start_absolute":1,
		"end_relative": {
			"value": "5",	
			"unit": "days"
		},
		"metrics": [
		{
			"name":"velocity",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":5,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["readings"],
				"fleet":["%s"]
			}
		}]
		}
	`,i.GetRandomFleet())

	humanLabel := "Iginx average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	// iginxql := fmt.Sprintf("SELECT AVG(current_load) FROM diagnostics.*.%s.*", i.GetRandomFleet())

	json := fmt.Sprintf(`
		{
		"start_absolute":1,
		"end_relative": {
			"value": "5",	
			"unit": "days"
		},
		"metrics": [
		{
			"name":"current_load",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":5,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["diagnostics"],
				"fleet":["%s"]
			}
		}]
		}
	`,i.GetRandomFleet())

	humanLabel := "Iginx average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	start := i.Interval.Start().Unix()
	end := i.Interval.End().Unix()
	// iginxql := fmt.Sprintf(`SELECT AVG(status) FROM diagnostics.*.*.*.* GROUP [%d, %d] BY time(1d)`, start, end)

	json := fmt.Sprintf(`
		{
		"start_absolute": %d,
		"end_absolute": %d,
		"metrics": [
		{
			"name":"status",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":1,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["diagnostics"]
			}
		}]
		}
	`,start,end,i.GetRandomFleet())

	humanLabel := "Iginx daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	start := i.Interval.Start().Unix()
	end := i.Interval.End().Unix()
	// iginxql := fmt.Sprintf(`SELECT AVG(status) FROM diagnostics.*.*.*.* GROUP [%d, %d] BY time(1d)`, start, end)

	json := fmt.Sprintf(`
		{
		"start_absolute": %d,
		"end_relative": %d,
		"metrics": [
		{
			"name":"status",
			"aggregators":[
				{
					"name":"avg",
					"sampling":{
						"value":1,
						"unit":"days"
					}
				}
			],
			"tags":{
				"type":["diagnostics"]
			}
		}]
		}
	`,start,end,i.GetRandomFleet())

	humanLabel := "Iginx truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, json)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
