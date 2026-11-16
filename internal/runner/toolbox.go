package runner

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/config"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// NewToolbox gives investigations tremolo's tools, the cluster's, kheck's
// changes, and the tools to record and end; patrols get only the tools for
// looking. Without kheck, nothing can be changed.
func NewToolbox(t *tremolo.Client, c *cluster.Cluster, kheck config.Kheck, version string) Toolbox {
	return func(ctx context.Context, inv *tremolo.Investigation, log *actionLog) ([]agent.Tool, func(), error) {
		s, err := t.Connect(ctx, version)
		if err != nil {
			return nil, nil, err
		}
		all, err := s.Tools(ctx)
		if err != nil {
			_ = s.Close()
			return nil, nil, err
		}
		done := func() { _ = s.Close() }
		if inv == nil {
			return append(append(mcpTools(s, all, readTools), clusterReads(c)...), usageTool(c, t, time.Now)), done, nil
		}
		work := workTools
		if inv.Rehearsal {
			work = nil // a made-up problem mustn't leave runbooks or silences behind
		}
		tools := mcpTools(s, all, readTools, work)
		tools = append(tools, clusterReads(c)...)
		tools = append(tools, usageTool(c, t, time.Now))
		if kheck.URL != "" {
			// A ServiceAccount token for kheck; the kubelet rotates it, so it's read for each investigation.
			token, err := os.ReadFile(kheck.TokenFile)
			if err != nil {
				done()
				return nil, nil, fmt.Errorf("kheck token: %w", err)
			}
			k, err := tremolo.ConnectWith(ctx, kheck.URL, version, strings.TrimSpace(string(token)))
			if err != nil {
				done()
				return nil, nil, fmt.Errorf("kheck: %w", err)
			}
			done = func() { _ = s.Close(); _ = k.Close() }
			tools = append(tools, kheckActions(k, t, inv.ID, log)...)
		}
		tools = append(tools, record(t, inv.ID))
		tools = append(tools, ending(log)...)
		return tools, done, nil
	}
}
