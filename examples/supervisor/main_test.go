package main

import (
	"testing"

	"github.com/floatdrop/golink/examples/internal/golden"
)

func TestOutput(t *testing.T) { golden.Main(t, main) }
