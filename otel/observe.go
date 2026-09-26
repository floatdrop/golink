package golinkotel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/metric"

	"github.com/floatdrop/golink"
)

// Observe registers gauges read from node's snapshots at each collection:
// processes, mailbox depth and oldest message age per label, and traffic per
// link. Unregister the returned registration when the node stops.
func (h *Hooks) Observe(node *golink.Node) (metric.Registration, error) {
	var errs []error
	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := h.meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return g
	}
	processes := gauge("golink.processes", "Live processes, by label.", "{process}")
	depth := gauge("golink.mailbox.depth", "Messages waiting in mailboxes, summed by label.", "{message}")
	oldest, err := h.meter.Float64ObservableGauge("golink.mailbox.oldest",
		metric.WithDescription("Age of the oldest waiting message, the maximum by label."), metric.WithUnit("s"))
	errs = append(errs, err)
	linkMessages, err := h.meter.Int64ObservableCounter("golink.link.messages",
		metric.WithDescription("Envelopes over links, by peer and direction."), metric.WithUnit("{message}"))
	errs = append(errs, err)
	linkBytes, err := h.meter.Int64ObservableCounter("golink.link.bytes",
		metric.WithDescription("Bytes over links, by peer and direction."), metric.WithUnit("By"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	type agg struct {
		count, depth int64
		oldest       float64
	}
	return h.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		byLabel := map[string]*agg{}
		for _, p := range node.Processes() {
			a := byLabel[p.Label]
			if a == nil {
				a = &agg{}
				byLabel[p.Label] = a
			}
			a.count++
			a.depth += int64(p.Mailbox.Depth)
			a.oldest = max(a.oldest, p.Mailbox.OldestAge.Seconds())
		}
		for label, a := range byLabel {
			attrs := metric.WithAttributes(AttrLabel.String(label))
			o.ObserveInt64(processes, a.count, attrs)
			o.ObserveInt64(depth, a.depth, attrs)
			o.ObserveFloat64(oldest, a.oldest, attrs)
		}
		for _, l := range node.Info().Links {
			dir := "in"
			if l.Outbound {
				dir = "out"
			}
			attrs := metric.WithAttributes(AttrPeer.String(l.Peer.Name), AttrDirection.String(dir))
			o.ObserveInt64(linkMessages, int64(l.Messages), attrs)
			o.ObserveInt64(linkBytes, int64(l.Bytes), attrs)
		}
		return nil
	}, processes, depth, oldest, linkMessages, linkBytes)
}
