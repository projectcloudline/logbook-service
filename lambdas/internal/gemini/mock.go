package gemini

import "context"

// MockClient implements the Client interface for testing.
type MockClient struct {
	GenerateContentFn func(ctx context.Context, model string, parts []Part, config *GenerateConfig) (string, error)
	EmbedContentFn    func(ctx context.Context, model string, text string) ([]float32, error)
}

func (m *MockClient) GenerateContent(ctx context.Context, model string, parts []Part, config *GenerateConfig) (string, error) {
	if m.GenerateContentFn != nil {
		return m.GenerateContentFn(ctx, model, parts, config)
	}
	return "", nil
}

func (m *MockClient) EmbedContent(ctx context.Context, model string, text string) ([]float32, error) {
	if m.EmbedContentFn != nil {
		return m.EmbedContentFn(ctx, model, text)
	}
	return []float32{0.1, 0.2, 0.3}, nil
}
