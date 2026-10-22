package tusapi

import (
	"context"
	"log/slog"

	expslog "golang.org/x/exp/slog"
)

// tusdLogLevel is the minimum level forwarded from tusd: it logs every
// request and response body at Info, which our own request log replaces.
const tusdLogLevel = slog.LevelWarn

// newTusdLogger adapts log to the golang.org/x/exp/slog logger tusd
// expects (a distinct type from log/slog), keeping only records at
// tusdLogLevel or above.
func newTusdLogger(log *slog.Logger) *expslog.Logger {
	return expslog.New(expHandler{h: log.With("component", "tusd").Handler()})
}

// expHandler forwards x/exp/slog records to a log/slog handler.
type expHandler struct{ h slog.Handler }

func (e expHandler) Enabled(ctx context.Context, l expslog.Level) bool {
	return slog.Level(l) >= tusdLogLevel && e.h.Enabled(ctx, slog.Level(l))
}

func (e expHandler) Handle(ctx context.Context, r expslog.Record) error {
	out := slog.NewRecord(r.Time, slog.Level(r.Level), r.Message, r.PC)
	r.Attrs(func(a expslog.Attr) bool {
		out.AddAttrs(convertAttr(a))
		return true
	})
	return e.h.Handle(ctx, out)
}

func (e expHandler) WithAttrs(attrs []expslog.Attr) expslog.Handler {
	conv := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		conv[i] = convertAttr(a)
	}
	return expHandler{h: e.h.WithAttrs(conv)}
}

func (e expHandler) WithGroup(name string) expslog.Handler {
	return expHandler{h: e.h.WithGroup(name)}
}

func convertAttr(a expslog.Attr) slog.Attr {
	v := a.Value.Resolve()
	if v.Kind() != expslog.KindGroup {
		return slog.Any(a.Key, v.Any())
	}
	group := v.Group()
	conv := make([]slog.Attr, len(group))
	for i, g := range group {
		conv[i] = convertAttr(g)
	}
	return slog.Attr{Key: a.Key, Value: slog.GroupValue(conv...)}
}
