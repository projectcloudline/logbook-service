// Package models provides API response helpers and pagination utilities.
package models

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
)

const (
	DefaultPageLimit = 25
	MaxPageLimit     = 100
)

// APIResponse builds a standard API Gateway Lambda proxy response with CORS headers.
func APIResponse(statusCode int, body any) (events.APIGatewayProxyResponse, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Headers:    corsHeaders(),
			Body:       fmt.Sprintf(`{"error":"json marshal: %s"}`, err.Error()),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers:    corsHeaders(),
		Body:       string(b),
	}, nil
}

func corsHeaders() map[string]string {
	return map[string]string{
		"Content-Type":                "application/json",
		"Access-Control-Allow-Origin": "*",
	}
}

// Pagination holds pagination metadata for list responses.
type Pagination struct {
	Page       int `json:"page"`
	Limit      int `json:"limit"`
	Total      int `json:"total"`
	TotalPages int `json:"totalPages"`
}

// NewPagination creates a Pagination from total count, page, and limit.
func NewPagination(total, page, limit int) Pagination {
	totalPages := 1
	if limit > 0 {
		totalPages = int(math.Ceil(float64(total) / float64(limit)))
		if totalPages < 1 {
			totalPages = 1
		}
	}
	return Pagination{
		Page:       page,
		Limit:      limit,
		Total:      total,
		TotalPages: totalPages,
	}
}

// QueryParams holds parsed pagination and filter parameters from a request.
type QueryParams struct {
	Params map[string]string
	Page   int
	Limit  int
	Offset int
}

// ParseQueryParams extracts pagination parameters from an API Gateway event.
func ParseQueryParams(event events.APIGatewayProxyRequest) QueryParams {
	params := event.QueryStringParameters
	if params == nil {
		params = map[string]string{}
	}

	limit := DefaultPageLimit
	if v, ok := params["limit"]; ok {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	page := 1
	if v, ok := params["page"]; ok {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 1 {
			page = parsed
		}
	}

	offset := (page - 1) * limit

	return QueryParams{
		Params: params,
		Page:   page,
		Limit:  limit,
		Offset: offset,
	}
}
