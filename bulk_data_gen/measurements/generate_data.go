package measurements

import (
	"fmt"
	"time"

	"math/rand"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

// scale var will be the number of measurements to generate. multiply by 60.
// constant of 20 fields for each measurement
// needs a long time range. just create points equal to the number of measurements.

const MeasSig = "Measurement-%d"
const FieldSig = "Field-%d"
const NumFields = 20
const Multiplier = 100

type MeasurementSimulatorConfig struct {
	Start time.Time
	End   time.Time

	ScaleFactor int
}

func (d *MeasurementSimulatorConfig) ToSimulator() *MeasurementSimulator {
	s := d.ScaleFactor * 60

	dg := &MeasurementSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  int64(s * Multiplier),

		FieldList: make(map[int][]byte),
		MeasList:  make(map[int][]byte),

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	for i := 0; i < s; i++ {
		dg.MeasList[i] = []byte(fmt.Sprintf(MeasSig, i))
	}

	for i := 0; i < NumFields; i++ {
		dg.FieldList[i] = []byte(fmt.Sprintf(FieldSig, i))
	}

	dg.stepTime = time.Duration(int64(dg.timestampEnd.Sub(dg.timestampStart)) / dg.maxPoints)

	return dg
}

// MeasurementSimulator fullfills the Simulator interface.
type MeasurementSimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64

	FieldList map[int][]byte
	MeasList  map[int][]byte

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	stepTime       time.Duration
}

func (g *MeasurementSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *MeasurementSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *MeasurementSimulator) Total() int64 {
	return g.maxPoints
}

func (g *MeasurementSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *MeasurementSimulator) Next(p *common.Point) {
	p.SetMeasurementName(g.MeasList[rand.Intn(len(g.MeasList))])
	p.SetTimestamp(&g.timestampNow)

	for _, f := range g.FieldList {
		p.AppendField(f, rand.Float64())
	}

	g.madePoints++
	g.madeValues++
	g.timestampNow = g.timestampNow.Add(g.stepTime)
}
