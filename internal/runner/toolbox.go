package runner

import (
	"context"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/tremolo"
)

// NewToolbox gives investigations tremolo's tools, the cluster's, and the
// tools to record and end; patrols get only the tools for looking.
func NewToolbox(t *tremolo.Client, c *cluster.Cluster, version string) Toolbox {
	return func(ctx context.Context, inv *tremolo.Investigation) ([]agent.Tool, func(), error) {
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
			return append(mcpTools(s, all, readTools), clusterReads(c)...), done, nil
		}
		tools := mcpTools(s, all, readTools, workTools)
		tools = append(tools, clusterReads(c)...)
		tools = append(tools, clusterActions(c, t, inv.ID)...)
		tools = append(tools, record(t, inv.ID))
		tools = append(tools, ending()...)
		return tools, done, nil
	}
}
