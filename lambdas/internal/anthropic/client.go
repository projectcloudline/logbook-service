// Package anthropic provides a Claude API client interface for Lambda functions.
package anthropic

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// Client defines operations for interacting with Claude models.
type Client interface {
	CreateMessage(ctx context.Context, model string, maxTokens int64, messages []Message) (string, error)
}

// Message represents a message in a Claude conversation.
type Message struct {
	Role    string // "user" or "assistant"
	Content []ContentPart
}

// ContentPart represents a content block within a message.
type ContentPart struct {
	Text      string
	ImageData []byte
	MIMEType  string
}

type claudeClient struct {
	client anthropic.Client
}

// New creates a Claude Client using the provided API key.
func New(apiKey string) Client {
	client := anthropic.NewClient(option.WithAPIKey(apiKey))
	return &claudeClient{client: client}
}

func (c *claudeClient) CreateMessage(ctx context.Context, model string, maxTokens int64, messages []Message) (string, error) {
	var params []anthropic.MessageParam
	for _, msg := range messages {
		var blocks []anthropic.ContentBlockParamUnion
		for _, part := range msg.Content {
			if part.Text != "" {
				blocks = append(blocks, anthropic.NewTextBlock(part.Text))
			}
			if part.ImageData != nil {
				encoded := base64.StdEncoding.EncodeToString(part.ImageData)
				blocks = append(blocks, anthropic.NewImageBlockBase64(part.MIMEType, encoded))
			}
		}

		switch msg.Role {
		case "user":
			params = append(params, anthropic.NewUserMessage(blocks...))
		case "assistant":
			params = append(params, anthropic.NewAssistantMessage(blocks...))
		}
	}

	resp, err := c.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.Model(model),
		MaxTokens: maxTokens,
		Messages:  params,
	})
	if err != nil {
		return "", fmt.Errorf("create message: %w", err)
	}

	for _, block := range resp.Content {
		if block.Type == "text" {
			return block.Text, nil
		}
	}

	return "", nil
}
