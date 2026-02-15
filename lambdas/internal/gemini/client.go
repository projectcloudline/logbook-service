// Package gemini provides a Gemini API client interface for Lambda functions.
package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"
)

// Client defines operations for interacting with Gemini models.
type Client interface {
	GenerateContent(ctx context.Context, model string, parts []Part, config *GenerateConfig) (string, error)
	EmbedContent(ctx context.Context, model string, text string) ([]float32, error)
}

// Part represents a content part for Gemini requests.
type Part struct {
	Text     string
	Data     []byte
	MIMEType string
}

// GenerateConfig holds configuration for content generation.
type GenerateConfig struct {
	Temperature      *float32
	ResponseMIMEType string
}

type geminiClient struct {
	client *genai.Client
}

// New creates a Gemini Client using the provided API key.
func New(ctx context.Context, apiKey string) (Client, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  apiKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("create genai client: %w", err)
	}
	return &geminiClient{client: client}, nil
}

func (c *geminiClient) GenerateContent(ctx context.Context, model string, parts []Part, config *GenerateConfig) (string, error) {
	var genaiParts []*genai.Part
	for _, p := range parts {
		if p.Text != "" {
			genaiParts = append(genaiParts, genai.NewPartFromText(p.Text))
		} else if p.Data != nil {
			genaiParts = append(genaiParts, genai.NewPartFromBytes(p.Data, p.MIMEType))
		}
	}

	var genConfig *genai.GenerateContentConfig
	if config != nil {
		genConfig = &genai.GenerateContentConfig{}
		if config.Temperature != nil {
			genConfig.Temperature = genai.Ptr(float32(*config.Temperature))
		}
		if config.ResponseMIMEType != "" {
			genConfig.ResponseMIMEType = config.ResponseMIMEType
		}
	}

	resp, err := c.client.Models.GenerateContent(ctx, model, []*genai.Content{
		genai.NewContentFromParts(genaiParts, "user"),
	}, genConfig)
	if err != nil {
		return "", fmt.Errorf("generate content: %w", err)
	}

	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0].Content == nil || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", nil
	}

	text := resp.Candidates[0].Content.Parts[0].Text
	return text, nil
}

func (c *geminiClient) EmbedContent(ctx context.Context, model string, text string) ([]float32, error) {
	resp, err := c.client.Models.EmbedContent(ctx, model, []*genai.Content{
		genai.NewContentFromText(text, "user"),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("embed content: %w", err)
	}

	if resp == nil || len(resp.Embeddings) == 0 || len(resp.Embeddings[0].Values) == 0 {
		return nil, fmt.Errorf("empty embedding response")
	}

	return resp.Embeddings[0].Values, nil
}
