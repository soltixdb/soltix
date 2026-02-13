package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/queue"
)

// MockQueuePublisher is a mock implementation of queue.Publisher
type MockQueuePublisher struct {
	published   []PublishedMessage
	shouldError bool
	errorMsg    string
}

type PublishedMessage struct {
	Subject string
	Data    []byte
}

func NewMockQueuePublisher() *MockQueuePublisher {
	return &MockQueuePublisher{
		published: make([]PublishedMessage, 0),
	}
}

func (m *MockQueuePublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if m.shouldError {
		return fiber.NewError(fiber.StatusInternalServerError, m.errorMsg)
	}
	m.published = append(m.published, PublishedMessage{
		Subject: subject,
		Data:    data,
	})
	return nil
}

func (m *MockQueuePublisher) PublishBatch(ctx context.Context, messages []queue.BatchMessage) (int, error) {
	if m.shouldError {
		return 0, fiber.NewError(fiber.StatusInternalServerError, m.errorMsg)
	}
	for _, msg := range messages {
		m.published = append(m.published, PublishedMessage{
			Subject: msg.Subject,
			Data:    msg.Data,
		})
	}
	return len(messages), nil
}

func (m *MockQueuePublisher) Close() error {
	return nil
}

func TestHandler_TriggerFlush(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*MockQueuePublisher)
		expectedStatus int
		expectSuccess  bool
	}{
		{
			name: "successful_flush_trigger",
			setupMock: func(m *MockQueuePublisher) {
				// Normal operation
			},
			expectedStatus: fiber.StatusOK,
			expectSuccess:  true,
		},
		{
			name: "publish_error",
			setupMock: func(m *MockQueuePublisher) {
				m.shouldError = true
				m.errorMsg = "failed to publish"
			},
			expectedStatus: fiber.StatusInternalServerError,
			expectSuccess:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger := logging.NewDevelopment()
			mockQueue := NewMockQueuePublisher()
			if tt.setupMock != nil {
				tt.setupMock(mockQueue)
			}

			handler := &Handler{
				logger:         logger,
				queuePublisher: mockQueue,
			}

			app := fiber.New()
			app.Post("/admin/flush", handler.TriggerFlush)

			req := httptest.NewRequest("POST", "/admin/flush", nil)

			// Test
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to perform request: %v", err)
			}

			// Assertions
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			bodyBytes, _ := io.ReadAll(resp.Body)
			var response map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &response); err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			if success, ok := response["success"].(bool); ok {
				if success != tt.expectSuccess {
					t.Errorf("Expected success=%v, got %v", tt.expectSuccess, success)
				}
			}

			if tt.expectSuccess && len(mockQueue.published) == 0 {
				t.Error("Expected message to be published but nothing was published")
			}

			if tt.expectSuccess && len(mockQueue.published) > 0 {
				if mockQueue.published[0].Subject != "soltix.admin.flush.trigger" {
					t.Errorf("Expected subject 'soltix.admin.flush.trigger', got '%s'", mockQueue.published[0].Subject)
				}

				var msg map[string]interface{}
				if err := json.Unmarshal(mockQueue.published[0].Data, &msg); err != nil {
					t.Fatalf("Failed to unmarshal published message: %v", err)
				}

				if msg["action"] != "flush" {
					t.Errorf("Expected action 'flush', got '%v'", msg["action"])
				}
			}
		})
	}
}
