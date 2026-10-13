// Package httpjson writes JSON responses in one consistent format.
package httpjson

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Write sends v as a JSON response with the given status code.
func Write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent, so logging is all we can do.
		slog.Error("failed to write JSON response", "error", err)
	}
}

// ErrorBody is the shape of every error response.
type ErrorBody struct {
	Error   string `json:"error"`   // stable, machine-readable code
	Message string `json:"message"` // human-readable explanation
	// RetryAfterSeconds is set on 429 responses.
	RetryAfterSeconds int `json:"retry_after_seconds,omitempty"`
}

// Error sends an ErrorBody.
func Error(w http.ResponseWriter, status int, code, message string) {
	Write(w, status, ErrorBody{Error: code, Message: message})
}
