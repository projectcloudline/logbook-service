package models

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestAPIResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       any
		wantStatus int
		wantCORS   bool
	}{
		{
			name:       "200 with map body",
			statusCode: 200,
			body:       map[string]string{"message": "ok"},
			wantStatus: 200,
			wantCORS:   true,
		},
		{
			name:       "400 with error",
			statusCode: 400,
			body:       map[string]string{"error": "bad request"},
			wantStatus: 400,
			wantCORS:   true,
		},
		{
			name:       "404 not found",
			statusCode: 404,
			body:       map[string]string{"error": "not found"},
			wantStatus: 404,
			wantCORS:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := APIResponse(tt.statusCode, tt.body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantCORS {
				if resp.Headers["Access-Control-Allow-Origin"] != "*" {
					t.Error("missing CORS header")
				}
				if resp.Headers["Content-Type"] != "application/json" {
					t.Error("missing Content-Type header")
				}
			}

			// Verify body is valid JSON
			var parsed map[string]any
			if err := json.Unmarshal([]byte(resp.Body), &parsed); err != nil {
				t.Errorf("body is not valid JSON: %v", err)
			}
		})
	}
}

func TestNewPagination(t *testing.T) {
	tests := []struct {
		name       string
		total      int
		page       int
		limit      int
		wantPages  int
	}{
		{"zero items", 0, 1, 25, 1},
		{"one page", 10, 1, 25, 1},
		{"exact pages", 50, 1, 25, 2},
		{"partial last page", 51, 1, 25, 3},
		{"large result", 250, 3, 25, 10},
		{"limit 1", 5, 1, 1, 5},
		{"zero limit", 10, 1, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPagination(tt.total, tt.page, tt.limit)
			if p.TotalPages != tt.wantPages {
				t.Errorf("totalPages = %d, want %d", p.TotalPages, tt.wantPages)
			}
			if p.Page != tt.page {
				t.Errorf("page = %d, want %d", p.Page, tt.page)
			}
			if p.Limit != tt.limit {
				t.Errorf("limit = %d, want %d", p.Limit, tt.limit)
			}
			if p.Total != tt.total {
				t.Errorf("total = %d, want %d", p.Total, tt.total)
			}
		})
	}
}

func TestParseQueryParams(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]string
		wantPage   int
		wantLimit  int
		wantOffset int
	}{
		{
			name:       "defaults",
			params:     nil,
			wantPage:   1,
			wantLimit:  25,
			wantOffset: 0,
		},
		{
			name:       "custom page and limit",
			params:     map[string]string{"page": "3", "limit": "10"},
			wantPage:   3,
			wantLimit:  10,
			wantOffset: 20,
		},
		{
			name:       "limit exceeds max",
			params:     map[string]string{"limit": "500"},
			wantPage:   1,
			wantLimit:  100,
			wantOffset: 0,
		},
		{
			name:       "negative page defaults to 1",
			params:     map[string]string{"page": "-1"},
			wantPage:   1,
			wantLimit:  25,
			wantOffset: 0,
		},
		{
			name:       "invalid limit defaults",
			params:     map[string]string{"limit": "abc"},
			wantPage:   1,
			wantLimit:  25,
			wantOffset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := events.APIGatewayProxyRequest{
				QueryStringParameters: tt.params,
			}
			qp := ParseQueryParams(event)
			if qp.Page != tt.wantPage {
				t.Errorf("page = %d, want %d", qp.Page, tt.wantPage)
			}
			if qp.Limit != tt.wantLimit {
				t.Errorf("limit = %d, want %d", qp.Limit, tt.wantLimit)
			}
			if qp.Offset != tt.wantOffset {
				t.Errorf("offset = %d, want %d", qp.Offset, tt.wantOffset)
			}
		})
	}
}
