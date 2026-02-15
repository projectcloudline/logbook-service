package gemini

import (
	"context"
	"testing"
)

func TestMockClient_GenerateContent(t *testing.T) {
	mock := &MockClient{
		GenerateContentFn: func(ctx context.Context, model string, parts []Part, config *GenerateConfig) (string, error) {
			if model != "gemini-2.5-flash" {
				t.Errorf("unexpected model: %s", model)
			}
			if len(parts) != 2 {
				t.Errorf("expected 2 parts, got %d", len(parts))
			}
			return `{"pageType":"maintenance_entry","entries":[]}`, nil
		},
	}

	result, err := mock.GenerateContent(context.Background(), "gemini-2.5-flash", []Part{
		{Text: "Extract maintenance entries"},
		{Data: []byte("image-data"), MIMEType: "image/jpeg"},
	}, &GenerateConfig{Temperature: floatPtr(0.1)})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != `{"pageType":"maintenance_entry","entries":[]}` {
		t.Errorf("unexpected result: %s", result)
	}
}

func TestMockClient_EmbedContent(t *testing.T) {
	mock := &MockClient{
		EmbedContentFn: func(ctx context.Context, model string, text string) ([]float32, error) {
			if model != "gemini-embedding-001" {
				t.Errorf("unexpected model: %s", model)
			}
			if text != "oil change performed" {
				t.Errorf("unexpected text: %s", text)
			}
			return []float32{0.1, 0.2, 0.3, 0.4}, nil
		},
	}

	result, err := mock.EmbedContent(context.Background(), "gemini-embedding-001", "oil change performed")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 4 {
		t.Errorf("expected 4 dimensions, got %d", len(result))
	}
}

func TestPart_Types(t *testing.T) {
	textPart := Part{Text: "hello"}
	if textPart.Text != "hello" {
		t.Error("text part mismatch")
	}

	dataPart := Part{Data: []byte("image"), MIMEType: "image/jpeg"}
	if dataPart.MIMEType != "image/jpeg" {
		t.Error("mime type mismatch")
	}
}

func TestGenerateConfig(t *testing.T) {
	temp := float32(0.2)
	config := &GenerateConfig{
		Temperature:      &temp,
		ResponseMIMEType: "application/json",
	}
	if *config.Temperature != 0.2 {
		t.Error("temperature mismatch")
	}
	if config.ResponseMIMEType != "application/json" {
		t.Error("response mime type mismatch")
	}
}

func floatPtr(f float32) *float32 {
	return &f
}

func TestMockClient_NoFunctions(t *testing.T) {
	mock := &MockClient{}

	// GenerateContent with nil function should return empty string
	result, err := mock.GenerateContent(context.Background(), "model", []Part{}, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty result, got %q", result)
	}

	// EmbedContent with nil function should return default embedding
	embedding, err := mock.EmbedContent(context.Background(), "model", "text")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(embedding) != 3 {
		t.Errorf("expected default embedding of length 3, got %d", len(embedding))
	}
}
