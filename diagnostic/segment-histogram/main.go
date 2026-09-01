package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Stdin, os.Stdout, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(_ io.Reader, stdout io.Writer, args []string) error {
	fs := flag.NewFlagSet("segment-histogram", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dataDir := fs.String("data-dir", "", "hot store root (DATA_DIR)")
	coldDir := fs.String("cold-dir", "", "optional cold store root")
	tenant := fs.String("tenant", "", "tenant namespace (store ns)")
	compact := fs.Bool("compact", false, "emit JSON without indent")
	list := fs.Bool("list", false, "include per-file segment rows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	snap, err := Snapshot(Options{
		DataDir: *dataDir,
		ColdDir: *coldDir,
		Tenant:  *tenant,
	})
	if err != nil {
		return err
	}
	if !*list {
		snap.Segments = nil
	}
	return encodeReport(stdout, &snap, *compact)
}
