package anthropic

import "context"

// MockClient implements the Client interface for testing.
type MockClient struct {
	CreateMessageFn func(ctx context.Context, model string, maxTokens int64, messages []Message) (string, error)
}

func (m *MockClient) CreateMessage(ctx context.Context, model string, maxTokens int64, messages []Message) (string, error) {
	if m.CreateMessageFn != nil {
		return m.CreateMessageFn(ctx, model, maxTokens, messages)
	}
	return "", nil
}
