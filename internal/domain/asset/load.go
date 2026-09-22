package asset

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/udaykishore-resu/plantstream/internal/domain/linesim"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
)

// Built-in defaults applied by Parse.
const (
	DefaultPollInterval       = time.Second
	DefaultModbusTimeout      = 2 * time.Second
	DefaultModbusMaxGap       = 8
	DefaultFlatlineMinSamples = 5
	staleMultiplier           = 3
)

// Load reads and validates a plant.yaml file.
func Load(path string) (*Plant, error) {
	b, err := os.ReadFile(filepath.Clean(path)) // path is the operator-supplied plant file from config
	if err != nil {
		return nil, fmt.Errorf("asset: read %s: %w", path, err)
	}
	p, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("asset: %s: %w", path, err)
	}
	return p, nil
}

// Parse decodes YAML, applies defaults and validates. The returned plant is
// safe to index.
func Parse(b []byte) (*Plant, error) {
	var p Plant
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("decode plant.yaml: %w", err)
	}
	p.applyDefaults()
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Plant) applyDefaults() {
	if p.Quality.FlatlineMinSamples <= 0 {
		p.Quality.FlatlineMinSamples = DefaultFlatlineMinSamples
	}
	for i := range p.Sources {
		s := &p.Sources[i]
		if s.PollInterval <= 0 {
			s.PollInterval = DefaultPollInterval
		}
		if s.Modbus != nil {
			if s.Modbus.Timeout <= 0 {
				s.Modbus.Timeout = DefaultModbusTimeout
			}
			if s.Modbus.MaxGap <= 0 {
				s.Modbus.MaxGap = DefaultModbusMaxGap
			}
			if s.Modbus.UnitID == 0 {
				s.Modbus.UnitID = 1
			}
		}
	}
	for _, a := range p.assets() {
		for j := range a.Tags {
			t := &a.Tags[j]
			if t.Modbus != nil {
				if t.Modbus.Function == "" {
					t.Modbus.Function = "holding"
				}
				if t.Modbus.ByteOrder == "" {
					t.Modbus.ByteOrder = modbus.ABCD
				}
				if t.DataType == "" {
					t.DataType = unsTypeFor(t.Modbus.Type)
				}
			}
			if t.DataType == "" {
				t.DataType = uns.TypeFloat
			}
			if t.CSV != nil && t.CSV.Column == "" {
				t.CSV.Column = t.Name
			}
			if t.Sim != nil && t.Sim.Signal == "" {
				t.Sim.Signal = t.Name
			}
		}
	}
}

func unsTypeFor(t modbus.DataType) uns.DataType {
	switch t {
	case modbus.Int16:
		return uns.TypeInt16
	case modbus.UInt16:
		return uns.TypeUInt16
	case modbus.Int32:
		return uns.TypeInt32
	case modbus.UInt32:
		return uns.TypeUInt32
	default:
		return uns.TypeFloat
	}
}

// assets returns pointers to every asset for in-place mutation.
func (p *Plant) assets() []*Asset {
	var out []*Asset
	for i := range p.Areas {
		for j := range p.Areas[i].Lines {
			for k := range p.Areas[i].Lines[j].Cells {
				cell := &p.Areas[i].Lines[j].Cells[k]
				for m := range cell.Assets {
					out = append(out, &cell.Assets[m])
				}
			}
		}
	}
	return out
}

// Validate checks the whole model and returns every problem found, joined.
func (p *Plant) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if err := uns.ValidateSegment(p.Enterprise); err != nil {
		fail("enterprise: %w", err)
	}
	if err := uns.ValidateSegment(p.Site); err != nil {
		fail("site: %w", err)
	}
	if p.Quality.StaleAfter < 0 || p.Quality.FlatlineAfter < 0 {
		fail("quality: durations must not be negative")
	}

	sources := map[string]*Source{}
	if len(p.Sources) == 0 {
		fail("at least one source is required")
	}
	for i := range p.Sources {
		s := &p.Sources[i]
		if s.Name == "" {
			fail("sources[%d]: name is required", i)
			continue
		}
		if err := uns.ValidateSegment(s.Name); err != nil {
			fail("source %q: %w", s.Name, err)
		}
		if _, dup := sources[s.Name]; dup {
			fail("source %q: duplicate name", s.Name)
		}
		sources[s.Name] = s
		switch s.Type {
		case SourceModbus:
			if s.Modbus == nil {
				fail("source %q: modbus block is required for type modbus", s.Name)
			} else {
				if s.Modbus.Address == "" {
					fail("source %q: modbus.address is required", s.Name)
				}
				if s.Modbus.UnitID > 247 {
					fail("source %q: modbus.unit_id must be 0..247", s.Name)
				}
			}
		case SourceSim:
		case SourceCSV:
			if s.CSV == nil || s.CSV.Path == "" {
				fail("source %q: csv.path is required for type csv", s.Name)
			}
		case "":
			fail("source %q: type is required", s.Name)
		default:
			fail("source %q: unsupported type %q (want modbus|sim|csv)", s.Name, s.Type)
		}
	}

	if len(p.Areas) == 0 {
		fail("at least one area is required")
	}
	assetIDs := map[string]bool{}
	topics := map[string]string{}
	anyAsset := false
	for _, ar := range p.Areas {
		if err := uns.ValidateSegment(ar.Name); err != nil {
			fail("area: %w", err)
		}
		for _, ln := range ar.Lines {
			if err := uns.ValidateSegment(ln.Name); err != nil {
				fail("area %q line: %w", ar.Name, err)
			}
			for _, cell := range ln.Cells {
				if err := uns.ValidateSegment(cell.Name); err != nil {
					fail("line %q cell: %w", ln.Name, err)
				}
				for _, a := range cell.Assets {
					anyAsset = true
					if a.ID == "" {
						fail("cell %q: asset id is required", cell.Name)
						continue
					}
					if assetIDs[a.ID] {
						fail("asset %q: duplicate id", a.ID)
					}
					assetIDs[a.ID] = true
					if a.Class == "" {
						fail("asset %q: class is required", a.ID)
					}
					src, ok := sources[a.Source]
					if !ok {
						fail("asset %q: unknown source %q", a.ID, a.Source)
					}
					if len(a.Tags) == 0 {
						fail("asset %q: at least one tag is required", a.ID)
					}
					names := map[string]bool{}
					for _, t := range a.Tags {
						if names[t.Name] {
							fail("asset %q: duplicate tag %q", a.ID, t.Name)
						}
						names[t.Name] = true
						topic := uns.Topic{Enterprise: p.Enterprise, Site: p.Site, Area: ar.Name, Line: ln.Name, Cell: cell.Name, Tag: t.Name}
						if err := topic.Validate(); err != nil {
							fail("asset %q tag %q: %w", a.ID, t.Name, err)
						} else if other, dup := topics[topic.String()]; dup {
							fail("asset %q tag %q: topic %s already used by asset %q", a.ID, t.Name, topic, other)
						} else {
							topics[topic.String()] = a.ID
						}
						for _, err := range validateTag(&t, src) {
							fail("asset %q tag %q: %w", a.ID, t.Name, err)
						}
					}
				}
			}
		}
	}
	if !anyAsset {
		fail("at least one asset is required")
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid plant definition (%d problem(s)):\n%w", len(errs), errors.Join(errs...))
}

func validateTag(t *Tag, src *Source) []error {
	var errs []error
	if _, err := uns.ParseDataType(string(t.DataType)); err != nil {
		errs = append(errs, err)
	}
	if t.Range != nil && t.Range.Min >= t.Range.Max {
		errs = append(errs, fmt.Errorf("range: min (%v) must be < max (%v)", t.Range.Min, t.Range.Max))
	}
	if t.Scale != nil && t.Scale.Factor == 0 {
		errs = append(errs, errors.New("scale.factor must not be 0"))
	}
	if t.Quality != nil && (t.Quality.StaleAfter < 0 || t.Quality.FlatlineAfter < 0) {
		errs = append(errs, errors.New("quality: durations must not be negative"))
	}
	if src == nil {
		return errs
	}
	switch src.Type {
	case SourceModbus:
		if t.Modbus == nil {
			errs = append(errs, errors.New("modbus address is required for a modbus source"))
			break
		}
		if t.Modbus.Function != "holding" && t.Modbus.Function != "input" {
			errs = append(errs, fmt.Errorf("modbus.function %q must be holding|input", t.Modbus.Function))
		}
		dt, err := modbus.ParseDataType(string(t.Modbus.Type))
		if err != nil {
			errs = append(errs, err)
		} else if int(t.Modbus.Register)+int(dt.Words()) > 0x10000 {
			errs = append(errs, errors.New("modbus.register + width exceeds address space"))
		}
		if _, err := modbus.ParseByteOrder(string(t.Modbus.ByteOrder)); err != nil {
			errs = append(errs, err)
		}
	case SourceSim:
		if t.Sim == nil || !linesim.IsSignal(t.Sim.Signal) {
			sig := ""
			if t.Sim != nil {
				sig = t.Sim.Signal
			}
			errs = append(errs, fmt.Errorf("sim.signal %q must be one of %v", sig, linesim.Signals()))
		}
	case SourceCSV:
		if t.CSV == nil || t.CSV.Column == "" {
			errs = append(errs, errors.New("csv.column is required for a csv source"))
		}
	}
	return errs
}

// EffectiveQuality resolves tag → plant → built-in defaults for a tag whose
// source polls at pollInterval.
func (p *Plant) EffectiveQuality(t *Tag, pollInterval time.Duration) QualityDefaults {
	q := p.Quality
	if t.Quality != nil {
		if t.Quality.StaleAfter > 0 {
			q.StaleAfter = t.Quality.StaleAfter
		}
		if t.Quality.FlatlineAfter > 0 {
			q.FlatlineAfter = t.Quality.FlatlineAfter
		}
		if t.Quality.FlatlineMinSamples > 0 {
			q.FlatlineMinSamples = t.Quality.FlatlineMinSamples
		}
	}
	if q.StaleAfter <= 0 {
		q.StaleAfter = staleMultiplier * pollInterval
	}
	if q.FlatlineMinSamples <= 0 {
		q.FlatlineMinSamples = DefaultFlatlineMinSamples
	}
	return q
}

// AssetRef is an asset with its position in the hierarchy.
type AssetRef struct {
	Asset  *Asset
	Area   string
	Line   string
	Cell   string
	Source *Source
}

// TagRef is a tag with everything needed to publish it.
type TagRef struct {
	AssetRef
	Tag   *Tag
	Topic uns.Topic
}

// Index is a read-only lookup structure over a validated Plant.
type Index struct {
	plant    *Plant
	assets   map[string]AssetRef
	sources  map[string]*Source
	bySource map[string][]TagRef
	byTopic  map[string]TagRef
	ordered  []AssetRef
	topics   []string
}

// Index builds lookup tables. Call only on a validated plant.
func (p *Plant) Index() *Index {
	idx := &Index{
		plant:    p,
		assets:   map[string]AssetRef{},
		sources:  map[string]*Source{},
		bySource: map[string][]TagRef{},
		byTopic:  map[string]TagRef{},
	}
	for i := range p.Sources {
		idx.sources[p.Sources[i].Name] = &p.Sources[i]
	}
	for i := range p.Areas {
		ar := &p.Areas[i]
		for j := range ar.Lines {
			ln := &ar.Lines[j]
			for k := range ln.Cells {
				cell := &ln.Cells[k]
				for m := range cell.Assets {
					a := &cell.Assets[m]
					ref := AssetRef{Asset: a, Area: ar.Name, Line: ln.Name, Cell: cell.Name, Source: idx.sources[a.Source]}
					idx.assets[a.ID] = ref
					idx.ordered = append(idx.ordered, ref)
					for n := range a.Tags {
						t := &a.Tags[n]
						tr := TagRef{AssetRef: ref, Tag: t, Topic: uns.Topic{
							Enterprise: p.Enterprise, Site: p.Site, Area: ar.Name, Line: ln.Name, Cell: cell.Name, Tag: t.Name,
						}}
						idx.bySource[a.Source] = append(idx.bySource[a.Source], tr)
						idx.byTopic[tr.Topic.String()] = tr
						idx.topics = append(idx.topics, tr.Topic.String())
					}
				}
			}
		}
	}
	sort.Slice(idx.ordered, func(i, j int) bool { return idx.ordered[i].Asset.ID < idx.ordered[j].Asset.ID })
	sort.Strings(idx.topics)
	return idx
}

// Plant returns the underlying plant.
func (i *Index) Plant() *Plant { return i.plant }

// Assets returns all assets sorted by ID.
func (i *Index) Assets() []AssetRef { return i.ordered }

// Asset looks up one asset by ID.
func (i *Index) Asset(id string) (AssetRef, bool) {
	a, ok := i.assets[id]
	return a, ok
}

// Source looks up a source by name.
func (i *Index) Source(name string) (*Source, bool) {
	s, ok := i.sources[name]
	return s, ok
}

// TagsForSource returns the tags a source must collect, in definition order.
func (i *Index) TagsForSource(name string) []TagRef { return i.bySource[name] }

// TagByTopic resolves a concrete topic.
func (i *Index) TagByTopic(topic string) (TagRef, bool) {
	t, ok := i.byTopic[topic]
	return t, ok
}

// Topics returns every data topic sorted.
func (i *Index) Topics() []string { return i.topics }

// TagCount is the total number of tags.
func (i *Index) TagCount() int { return len(i.topics) }
