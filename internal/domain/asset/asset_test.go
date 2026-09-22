package asset

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
)

func TestLoad_Fixture(t *testing.T) {
	p, err := Load("testdata/plant.yaml")
	require.NoError(t, err)

	// Defaults applied.
	assert.Equal(t, 500*time.Millisecond, p.Sources[0].PollInterval)
	assert.Equal(t, DefaultPollInterval, p.Sources[1].PollInterval)
	assert.Equal(t, DefaultModbusMaxGap, p.Sources[0].Modbus.MaxGap)
	assert.Equal(t, 10, p.Quality.FlatlineMinSamples)

	idx := p.Index()
	assert.Equal(t, 7, idx.TagCount())
	assert.Len(t, idx.Assets(), 3)
	assert.Equal(t, "filler-01", idx.Assets()[0].Asset.ID)

	ref, ok := idx.Asset("filler-01")
	require.True(t, ok)
	assert.Equal(t, "packaging", ref.Area)
	assert.Equal(t, "line-1", ref.Line)
	assert.Equal(t, "filling", ref.Cell)
	assert.Equal(t, SourceModbus, ref.Source.Type)

	tags := idx.TagsForSource("plc-line-1")
	require.Len(t, tags, 4)
	assert.Equal(t, "acme/austin/packaging/line-1/filling/speed", tags[0].Topic.String())
	assert.Equal(t, uns.TypeFloat, tags[0].Tag.DataType)
	assert.Equal(t, modbus.ABCD, tags[0].Tag.Modbus.ByteOrder)
	assert.Equal(t, "holding", tags[0].Tag.Modbus.Function)
	assert.Equal(t, uns.TypeInt16, tags[1].Tag.DataType)
	assert.Equal(t, modbus.FuncReadInputRegisters, tags[1].Tag.Modbus.FunctionCode())
	assert.Equal(t, uns.TypeUInt16, tags[2].Tag.DataType)
	assert.Equal(t, uns.TypeUInt32, tags[3].Tag.DataType)
	assert.Equal(t, modbus.CDAB, tags[3].Tag.Modbus.ByteOrder)

	sim := idx.TagsForSource("sim-line-2")
	require.Len(t, sim, 2)
	assert.Equal(t, "vibration", sim[1].Tag.Sim.Signal, "signal defaults to tag name")

	csv := idx.TagsForSource("csv-lab")
	require.Len(t, csv, 1)
	assert.Equal(t, "pressure", csv[0].Tag.CSV.Column)
	assert.Equal(t, uns.TypeDouble, csv[0].Tag.DataType)

	tr, ok := idx.TagByTopic("acme/austin/lab/bench/rig/pressure")
	require.True(t, ok)
	assert.Equal(t, "rig-01", tr.Asset.ID)
	_, ok = idx.TagByTopic("nope")
	assert.False(t, ok)

	topics := idx.Topics()
	assert.Len(t, topics, 7)
	assert.True(t, strings.HasPrefix(topics[0], "acme/austin/lab/"), "topics are sorted")

	_, ok = idx.Source("plc-line-1")
	assert.True(t, ok)
	_, ok = idx.Source("missing")
	assert.False(t, ok)
	assert.Same(t, p, idx.Plant())
}

func TestEffectiveQuality(t *testing.T) {
	p, err := Load("testdata/plant.yaml")
	require.NoError(t, err)
	idx := p.Index()

	speed := idx.TagsForSource("plc-line-1")[0]
	q := p.EffectiveQuality(speed.Tag, speed.Source.PollInterval)
	assert.Equal(t, 5*time.Second, q.StaleAfter, "plant-level stale_after")
	assert.Equal(t, time.Duration(0), q.FlatlineAfter, "flatline disabled by default")
	assert.Equal(t, 10, q.FlatlineMinSamples)

	vib := idx.TagsForSource("sim-line-2")[1]
	q = p.EffectiveQuality(vib.Tag, vib.Source.PollInterval)
	assert.Equal(t, 30*time.Second, q.FlatlineAfter, "tag override")

	// No plant-level stale → 3 × poll interval.
	p2 := &Plant{}
	q = p2.EffectiveQuality(&Tag{}, 2*time.Second)
	assert.Equal(t, 6*time.Second, q.StaleAfter)
	assert.Equal(t, DefaultFlatlineMinSamples, q.FlatlineMinSamples)
}

func TestScale(t *testing.T) {
	var s *Scale
	assert.Equal(t, 5.0, s.Apply(5))
	s = &Scale{Factor: 0.1, Offset: -40}
	assert.InDelta(t, -30, s.Apply(100), 1e-9)
}

const minimal = `
enterprise: acme
site: austin
sources:
  - name: sim
    type: sim
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: m1
                class: Mixer
                source: sim
                tags:
                  - name: speed
                    sim: { signal: speed }
`

func TestParse_Minimal(t *testing.T) {
	p, err := Parse([]byte(minimal))
	require.NoError(t, err)
	assert.Equal(t, uns.TypeFloat, p.Areas[0].Lines[0].Cells[0].Assets[0].Tags[0].DataType)
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(string) string
		wantErr string
	}{
		{"unknown field", func(s string) string { return s + "\nbogus: 1\n" }, "field bogus not found"},
		{"bad yaml", func(string) string { return "enterprise: [" }, "decode"},
		{"missing enterprise", func(s string) string { return strings.Replace(s, "enterprise: acme", "enterprise: \"\"", 1) }, "enterprise"},
		{"bad site segment", func(s string) string { return strings.Replace(s, "site: austin", "site: aus/tin", 1) }, "site"},
		{"no sources", func(s string) string { return strings.Replace(s, "source: sim", "source: other", 1) }, "unknown source"},
		{"unknown source type", func(s string) string { return strings.Replace(s, "type: sim", "type: opcua", 1) }, "unsupported type"},
		{"missing source type", func(s string) string { return strings.Replace(s, "    type: sim\n", "", 1) }, "type is required"},
		{"sim wrong signal", func(s string) string { return strings.Replace(s, "signal: speed", "signal: torque", 1) }, "sim.signal"},
		{"missing class", func(s string) string { return strings.Replace(s, "class: Mixer", "class: \"\"", 1) }, "class is required"},
		{"bad tag segment", func(s string) string { return strings.Replace(s, "name: speed", "name: sp+eed", 1) }, "tag"},
		{"bad datatype", func(s string) string {
			return strings.Replace(s, "sim: { signal: speed }", "sim: { signal: speed }\n                    datatype: Int64", 1)
		}, "datatype"},
		{"bad range", func(s string) string {
			return strings.Replace(s, "sim: { signal: speed }", "sim: { signal: speed }\n                    range: {min: 5, max: 5}", 1)
		}, "range"},
		{"zero scale", func(s string) string {
			return strings.Replace(s, "sim: { signal: speed }", "sim: { signal: speed }\n                    scale: {factor: 0}", 1)
		}, "scale.factor"},
		{"negative quality", func(s string) string {
			return strings.Replace(s, "sim: { signal: speed }", "sim: { signal: speed }\n                    quality: {stale_after: -1s}", 1)
		}, "negative"},
		{"duplicate tag", func(s string) string {
			return s + "                  - name: speed\n                    sim: { signal: speed }\n"
		}, "duplicate tag"},
		{"duplicate asset", func(s string) string {
			block := s[strings.Index(s, "              - id: m1"):]
			return s + block
		}, "duplicate id"},
		{"no areas", func(s string) string { return s[:strings.Index(s, "areas:")] + "areas: []\n" }, "at least one area"},
		{"no tags", func(s string) string { return s[:strings.Index(s, "                tags:")] }, "at least one tag"},
		{"modbus source without block", func(s string) string { return strings.Replace(s, "type: sim", "type: modbus", 1) }, "modbus block is required"},
		{"csv source without path", func(s string) string { return strings.Replace(s, "type: sim", "type: csv", 1) }, "csv.path is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.mutate(minimal)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

const modbusPlant = `
enterprise: acme
site: austin
sources:
  - name: plc
    type: modbus
    modbus: { address: "127.0.0.1:5020", unit_id: 250 }
areas:
  - name: a
    lines:
      - name: l
        cells:
          - name: c
            assets:
              - id: m1
                class: Mixer
                source: plc
                tags:
                  - name: speed
                    modbus: { function: coil, register: 65535, type: float32, byte_order: ACBD }
                  - name: t2
                    modbus: { register: 1, type: int64 }
                  - name: t3
`

func TestParse_ModbusErrors(t *testing.T) {
	_, err := Parse([]byte(modbusPlant))
	require.Error(t, err)
	msg := err.Error()
	for _, want := range []string{"unit_id", "holding|input", "exceeds address space", "byte order", "data type", "modbus address is required"} {
		assert.Contains(t, msg, want)
	}
}

func TestParse_CSVMissingColumnDefaults(t *testing.T) {
	src := strings.Replace(minimal, "type: sim", "type: csv\n    csv: { path: x.csv }", 1)
	src = strings.Replace(src, "sim: { signal: speed }", "csv: {}", 1)
	p, err := Parse([]byte(src))
	require.NoError(t, err)
	assert.Equal(t, "speed", p.Areas[0].Lines[0].Cells[0].Assets[0].Tags[0].CSV.Column)

	// CSV source but tag has no csv block at all → error.
	src2 := strings.Replace(src, "                    csv: {}\n", "", 1)
	_, err = Parse([]byte(src2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "csv.column")
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("testdata/does-not-exist.yaml")
	require.Error(t, err)
}
