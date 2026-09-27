// Package golden checks what an example program prints against its
// output.txt, the output the README shows.
package golden

import (
	"bytes"
	"flag"
	"io"
	"os"
	"testing"
)

var update = flag.Bool("update", false, "rewrite output.txt with what the program prints")

// Main runs main with standard output captured and compares what it printed
// with output.txt in the test's directory.
func Main(t *testing.T, main func()) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	printed := make(chan []byte)
	go func() {
		b, _ := io.ReadAll(r)
		printed <- b
	}()
	stdout := os.Stdout
	os.Stdout = w
	func() {
		defer func() { os.Stdout = stdout }()
		main()
	}()
	_ = w.Close()
	got := <-printed

	if *update {
		if err := os.WriteFile("output.txt", got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile("output.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("printed:\n%s\noutput.txt (go test -update to rewrite it):\n%s", got, want)
	}
}
