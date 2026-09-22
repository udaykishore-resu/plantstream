// Package asset defines the config-as-code plant model (one plant.yaml per
// site): the ISA-95 hierarchy, equipment classes, tags with units, engineering
// ranges and scaling, and the collector sources that feed them. Everything is
// validated once at startup so the rest of the system can trust it.
package asset

import (
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
)

// SourceType identifies a collector implementation.
type SourceType string

// Supported source types. OPC UA is deliberately not on this list; see
// docs/adr/0005-opc-ua-out-of-scope.md.
const (
	SourceModbus SourceType = "modbus"
	SourceSim    SourceType = "sim"
	SourceCSV    SourceType = "csv"
)

// Plant is the root of plant.yaml.
type Plant struct {
	Enterprise string          `yaml:"enterprise"`
	Site       string          `yaml:"site"`
	Quality    QualityDefaults `yaml:"quality"`
	Sources    []Source        `yaml:"sources"`
	Areas      []Area          `yaml:"areas"`
}

// QualityDefaults tunes the quality engine. Zero values mean "use the
// built-in default" at the plant level and "inherit" at the tag level.
type QualityDefaults struct {
	// StaleAfter marks a tag STALE when no sample arrived for this long.
	// Default: 3 × the source poll interval.
	StaleAfter time.Duration `yaml:"stale_after"`
	// FlatlineAfter marks a tag FLATLINE when its value has not changed for
	// this long AND at least FlatlineMinSamples were seen. Default: disabled.
	FlatlineAfter time.Duration `yaml:"flatline_after"`
	// FlatlineMinSamples guards against flagging slow tags. Default: 5.
	FlatlineMinSamples int `yaml:"flatline_min_samples"`
}

// Source is a collector definition.
type Source struct {
	Name         string        `yaml:"name"`
	Type         SourceType    `yaml:"type"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Modbus       *ModbusSource `yaml:"modbus,omitempty"`
	Sim          *SimSource    `yaml:"sim,omitempty"`
	CSV          *CSVSource    `yaml:"csv,omitempty"`
}

// ModbusSource configures a Modbus TCP connection.
type ModbusSource struct {
	Address string        `yaml:"address"`
	UnitID  uint8         `yaml:"unit_id"`
	Timeout time.Duration `yaml:"timeout"`
	// MaxGap is the largest hole (in registers) the read planner bridges when
	// coalescing tags into one request. Default 8.
	MaxGap int `yaml:"max_gap"`
}

// SimSource configures the in-process line simulator.
type SimSource struct {
	Seed int64 `yaml:"seed"`
}

// CSVSource replays a CSV file (header: tag,value[,timestamp]) row by row.
type CSVSource struct {
	Path string `yaml:"path"`
	Loop bool   `yaml:"loop"`
}

// Area is an ISA-95 area.
type Area struct {
	Name  string `yaml:"name"`
	Lines []Line `yaml:"lines"`
}

// Line is an ISA-95 production line.
type Line struct {
	Name  string `yaml:"name"`
	Cells []Cell `yaml:"cells"`
}

// Cell is an ISA-95 work cell; it owns the assets whose tags are published.
type Cell struct {
	Name   string  `yaml:"name"`
	Assets []Asset `yaml:"assets"`
}

// Asset is a piece of equipment.
type Asset struct {
	ID          string `yaml:"id"`
	Class       string `yaml:"class"`
	Description string `yaml:"description"`
	Source      string `yaml:"source"`
	Tags        []Tag  `yaml:"tags"`
}

// Tag is one published signal of an asset.
type Tag struct {
	Name        string           `yaml:"name"`
	Description string           `yaml:"description"`
	Unit        string           `yaml:"unit"`
	DataType    uns.DataType     `yaml:"datatype"`
	Range       *Range           `yaml:"range,omitempty"`
	Scale       *Scale           `yaml:"scale,omitempty"`
	Quality     *QualityDefaults `yaml:"quality,omitempty"`
	Modbus      *ModbusAddress   `yaml:"modbus,omitempty"`
	Sim         *SimAddress      `yaml:"sim,omitempty"`
	CSV         *CSVAddress      `yaml:"csv,omitempty"`
}

// Range is the engineering range in engineering units (after scaling).
type Range struct {
	Min float64 `yaml:"min"`
	Max float64 `yaml:"max"`
}

// Scale converts a raw register value to engineering units: eu = raw*Factor + Offset.
type Scale struct {
	Factor float64 `yaml:"factor"`
	Offset float64 `yaml:"offset"`
}

// Apply converts raw to engineering units.
func (s *Scale) Apply(raw float64) float64 {
	if s == nil {
		return raw
	}
	return raw*s.Factor + s.Offset
}

// ModbusAddress locates a tag in a Modbus device.
type ModbusAddress struct {
	// Function is "holding" (FC 3) or "input" (FC 4).
	Function  string           `yaml:"function"`
	Register  uint16           `yaml:"register"`
	Type      modbus.DataType  `yaml:"type"`
	ByteOrder modbus.ByteOrder `yaml:"byte_order"`
}

// FunctionCode maps the function name to the Modbus function code.
func (m *ModbusAddress) FunctionCode() byte {
	if m.Function == "input" {
		return modbus.FuncReadInputRegisters
	}
	return modbus.FuncReadHoldingRegisters
}

// SimAddress binds a tag to a simulator signal.
type SimAddress struct {
	Signal string `yaml:"signal"`
}

// CSVAddress binds a tag to a CSV column/tag name (defaults to the tag name).
type CSVAddress struct {
	Column string `yaml:"column"`
}
