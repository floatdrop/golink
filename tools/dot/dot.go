// Package dot draws grpcproc processes as a Graphviz digraph: one cluster per
// node, and an edge from each process to those it started, which for
// supervised processes is the supervision tree.
//
//	grpcprocctl dot --cluster | dot -Tsvg -o processes.svg
package dot

import (
	"cmp"
	"fmt"
	"io"
	"strings"

	"github.com/floatdrop/grpcproc/tools/client"
)

// Node is one node's processes, in the order to draw them.
type Node struct {
	Name      string
	Processes []client.ProcessView
}

// Render writes the digraph. Output follows the order given, so it is
// stable and can be diffed.
func Render(w io.Writer, nodes []Node) error {
	var b strings.Builder
	b.WriteString("digraph grpcproc {\n\trankdir=LR;\n\tnode [shape=box, fontname=\"monospace\", fontsize=10];\n")
	known := map[string]bool{}
	for _, n := range nodes {
		for _, p := range n.Processes {
			known[p.PID] = true
		}
	}
	for i, n := range nodes {
		fmt.Fprintf(&b, "\tsubgraph cluster_%d {\n\t\tlabel=%s;\n", i, quote(n.Name))
		for _, p := range n.Processes {
			fmt.Fprintf(&b, "\t\t%s [label=%s%s];\n", quote(p.PID), quote(label(p)), style(p))
		}
		b.WriteString("\t}\n")
	}
	for _, n := range nodes {
		for _, p := range n.Processes {
			if p.Parent != "" && known[p.Parent] {
				fmt.Fprintf(&b, "\t%s -> %s;\n", quote(p.Parent), quote(p.PID))
			}
		}
	}
	b.WriteString("}\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func label(p client.ProcessView) string {
	title := cmp.Or(p.Name, p.Label)
	lines := []string{title, p.PID}
	if p.Name != "" && p.Label != "" {
		lines = append(lines, p.Label)
	}
	if p.Mailbox > 0 {
		lines = append(lines, fmt.Sprintf("mailbox %d, oldest %s", p.Mailbox, p.OldestWait))
	}
	return strings.Join(lines, "\n")
}

func style(p client.ProcessView) string {
	var attrs []string
	if p.Label == "supervisor" {
		attrs = append(attrs, `style="rounded,bold"`)
	}
	if p.Mailbox > 0 {
		attrs = append(attrs, `color="red"`)
	}
	if len(attrs) == 0 {
		return ""
	}
	return ", " + strings.Join(attrs, ", ")
}

// quote makes a DOT string: backslashes and quotes escaped, newlines as \n.
func quote(s string) string {
	s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
	return `"` + s + `"`
}
