// Command plc-sim is a Modbus TCP server that behaves like the PLC of a
// packaging line: speed ramps, the sealing head warms up, vibration follows
// load and spikes on faults, and the machine walks a realistic state machine.
//
// Register map (unit id ignored):
//
//	Holding (FC 3)             Input (FC 4)
//	0-1  speed        float32  0  ambient   int16  ×0.1 °C
//	2-3  temperature  float32  1  pressure  uint16 ×0.01 bar
//	4-5  vibration    float32
//	6    state        uint16   (0 STOPPED 1 STARTING 2 RUNNING 3 STARVED 4 FAULTED 5 CHANGEOVER)
//	7-8  count        uint32
//
// All 32-bit values are big-endian (ABCD) unless -byte-order says otherwise.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/linesim"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
	"github.com/udaykishore-resu/plantstream/internal/observability"
)

func main() {
	addr := flag.String("addr", ":5020", "TCP listen address (502 needs root; 5020 is the common unprivileged choice)")
	seed := flag.Int64("seed", 42, "random seed for the line model")
	tick := flag.Duration("tick", 250*time.Millisecond, "model update interval")
	order := flag.String("byte-order", "ABCD", "byte order for 32-bit registers: ABCD|DCBA|BADC|CDAB")
	level := flag.String("log-level", "info", "log level")
	printMap := flag.Bool("print-map", false, "print the register map and exit")
	flag.Parse()

	if *printMap {
		fmt.Print(registerMap)
		return
	}
	log := observability.NewLogger(os.Stdout, *level)
	bo, err := modbus.ParseByteOrder(*order)
	if err != nil {
		log.Error("invalid byte order", "err", err)
		os.Exit(2)
	}
	if err := run(*addr, *seed, *tick, bo, log); err != nil {
		log.Error("plc-sim exited with error", "err", err)
		os.Exit(1)
	}
}

const registerMap = `Holding registers (FC 3)          Input registers (FC 4)
  0-1  speed        float32 bpm     0  ambient   int16  x0.1 degC
  2-3  temperature  float32 degC    1  pressure  uint16 x0.01 bar
  4-5  vibration    float32 mm/s
  6    state        uint16  enum (0 STOPPED 1 STARTING 2 RUNNING 3 STARVED 4 FAULTED 5 CHANGEOVER)
  7-8  count        uint32  bottles
`

func run(addr string, seed int64, tick time.Duration, order modbus.ByteOrder, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bank := modbus.NewMemoryBank(16, 8)
	line := linesim.NewLine(seed)
	if err := writeRegisters(bank, line.Snapshot(), order); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	log.Info("plc-sim listening", "addr", ln.Addr().String(), "seed", seed, "tick", tick, "byte_order", order)

	srv := modbus.NewServer(bank, log)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	t := time.NewTicker(tick)
	defer t.Stop()
	lastLog := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Info("plc-sim stopping")
			return <-done
		case err := <-done:
			return err
		case <-t.C:
			line.Step(tick)
			snap := line.Snapshot()
			if err := writeRegisters(bank, snap, order); err != nil {
				return err
			}
			if time.Since(lastLog) >= 5*time.Second {
				lastLog = time.Now()
				log.Info("line", "state", snap.State.String(), "speed_bpm", snap.Speed, "temp_c", snap.Temperature,
					"vibration_mm_s", snap.Vibration, "count", snap.Count)
			}
		}
	}
}

func writeRegisters(bank *modbus.MemoryBank, s linesim.Snapshot, order modbus.ByteOrder) error {
	holding := make([]uint16, 0, 9)
	for _, v := range []float64{s.Speed, s.Temperature, s.Vibration} {
		regs, err := modbus.Encode(modbus.Float32, order, v)
		if err != nil {
			return err
		}
		holding = append(holding, regs...)
	}
	holding = append(holding, uint16(s.State))
	count, err := modbus.Encode(modbus.UInt32, order, float64(s.Count))
	if err != nil {
		return err
	}
	holding = append(holding, count...)
	if err := bank.SetHolding(0, holding); err != nil {
		return err
	}
	ambient, err := modbus.Encode(modbus.Int16, order, s.Ambient*10)
	if err != nil {
		return err
	}
	pressure, err := modbus.Encode(modbus.UInt16, order, s.Pressure*100)
	if err != nil {
		return err
	}
	return bank.SetInput(0, append(ambient, pressure...))
}
