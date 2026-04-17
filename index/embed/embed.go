// Package embed defines the Provider interface for text embedding and a noop implementation.
package embed

import "context"

// Provider computes text embeddings.
type Provider interface {
	// Embed returns one vector per input text, in the same order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Dimension returns the fixed vector length produced by this provider.
	Dimension() int
	// Name returns a stable identifier stored in files_vectors.provider.
	Name() string
	// Model returns the model/variant identifier stored in files_vectors.model.
	Model() string
}

type noop struct{}

func (noop) Embed(_ context.Context, texts []string) ([][]float32, error) {
	return make([][]float32, len(texts)), nil
}
func (noop) Dimension() int    { return 0 }
func (noop) Name() string      { return "noop" }
func (noop) Model() string     { return "" }

// Noop returns a Provider that produces nil vectors for every input.
// Useful in tests that exercise the indexing path without a real provider.
func Noop() Provider { return noop{} }
