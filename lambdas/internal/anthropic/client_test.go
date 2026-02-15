package anthropic

import (
	"context"
	"testing"
)

func TestMockClient_CreateMessage(t *testing.T) {
	mock := &MockClient{
		CreateMessageFn: func(ctx context.Context, model string, maxTokens int64, messages []Message) (string, error) {
			if model != "claude-haiku-4-5-20251001" {
				t.Errorf("unexpected model: %s", model)
			}
			if maxTokens != 4096 {
				t.Errorf("unexpected maxTokens: %d", maxTokens)
			}
			if len(messages) != 1 {
				t.Errorf("expected 1 message, got %d", len(messages))
			}
			if messages[0].Role != "user" {
				t.Errorf("expected user role, got %s", messages[0].Role)
			}
			return `{"results":[{"verdict":"pass"}]}`, nil
		},
	}

	result, err := mock.CreateMessage(context.Background(), "claude-haiku-4-5-20251001", 4096, []Message{
		{
			Role: "user",
			Content: []ContentPart{
				{Text: "Verify this extraction"},
				{ImageData: []byte("image-data"), MIMEType: "image/jpeg"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != `{"results":[{"verdict":"pass"}]}` {
		t.Errorf("unexpected result: %s", result)
	}
}

func TestMockClient_NoFunction(t *testing.T) {
	mock := &MockClient{}

	result, err := mock.CreateMessage(context.Background(), "model", 1024, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty result, got %q", result)
	}
}

func TestContentPart_Types(t *testing.T) {
	textPart := ContentPart{Text: "hello"}
	if textPart.Text != "hello" {
		t.Error("text part mismatch")
	}

	imagePart := ContentPart{ImageData: []byte("image"), MIMEType: "image/jpeg"}
	if imagePart.MIMEType != "image/jpeg" {
		t.Error("mime type mismatch")
	}
}

func TestMessage_Roles(t *testing.T) {
	userMsg := Message{Role: "user", Content: []ContentPart{{Text: "hello"}}}
	if userMsg.Role != "user" {
		t.Error("role mismatch")
	}

	assistantMsg := Message{Role: "assistant", Content: []ContentPart{{Text: "hi"}}}
	if assistantMsg.Role != "assistant" {
		t.Error("role mismatch")
	}
}
