package services

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceError_Error(t *testing.T) {
	err := &ServiceError{
		Code:    "TEST_ERROR",
		Message: "Test error message",
	}

	if err.Error() != "Test error message" {
		t.Errorf("Expected 'Test error message', got '%s'", err.Error())
	}
}

func TestNewServiceError(t *testing.T) {
	err := NewServiceError("ERROR_CODE", "Error message")

	if err.Code != "ERROR_CODE" {
		t.Errorf("Expected code 'ERROR_CODE', got '%s'", err.Code)
	}
	if err.Message != "Error message" {
		t.Errorf("Expected message 'Error message', got '%s'", err.Message)
	}
	if err.Details != nil {
		t.Errorf("Expected nil details, got %v", err.Details)
	}
}

func TestNewServiceErrorWithDetails(t *testing.T) {
	details := map[string]interface{}{
		"field":  "test_field",
		"reason": "validation failed",
	}

	err := NewServiceErrorWithDetails("VALIDATION_ERROR", "Validation failed", details)

	if err.Code != "VALIDATION_ERROR" {
		t.Errorf("Expected code 'VALIDATION_ERROR', got '%s'", err.Code)
	}
	if err.Message != "Validation failed" {
		t.Errorf("Expected message 'Validation failed', got '%s'", err.Message)
	}
	if err.Details == nil {
		t.Fatal("Expected non-nil details")
	}
	if err.Details["field"] != "test_field" {
		t.Errorf("Expected field 'test_field', got '%v'", err.Details["field"])
	}
	if err.Details["reason"] != "validation failed" {
		t.Errorf("Expected reason 'validation failed', got '%v'", err.Details["reason"])
	}
}

func TestServiceError_ImplementsError(t *testing.T) {
	var _ error = &ServiceError{}
}

func TestServiceError_ErrorWithEmptyMessage(t *testing.T) {
	err := &ServiceError{
		Code:    "ERROR_CODE",
		Message: "",
	}

	if err.Error() != "" {
		t.Errorf("Expected empty string, got '%s'", err.Error())
	}
}

func TestServiceError_ErrorWithLongMessage(t *testing.T) {
	longMessage := strings.Repeat("a", 1000)
	err := &ServiceError{
		Code:    "ERROR_CODE",
		Message: longMessage,
	}

	if err.Error() != longMessage {
		t.Errorf("Expected long message, got different string")
	}
}

func TestServiceError_ErrorWithSpecialCharacters(t *testing.T) {
	specialMessage := "Error with special chars: !@#$%^&*()_+-=[]{}|;:',.<>?/\n\t\r"
	err := &ServiceError{
		Code:    "SPECIAL_ERROR",
		Message: specialMessage,
	}

	if err.Error() != specialMessage {
		t.Errorf("Expected special message, got '%s'", err.Error())
	}
}

func TestServiceError_ErrorWithUnicode(t *testing.T) {
	unicodeMessage := "Error with unicode: 你好世界 🚀 ñ é ü"
	err := &ServiceError{
		Code:    "UNICODE_ERROR",
		Message: unicodeMessage,
	}

	if err.Error() != unicodeMessage {
		t.Errorf("Expected unicode message, got '%s'", err.Error())
	}
}

func TestNewServiceError_EmptyCode(t *testing.T) {
	err := NewServiceError("", "Error message")

	if err.Code != "" {
		t.Errorf("Expected empty code, got '%s'", err.Code)
	}
	if err.Message != "Error message" {
		t.Errorf("Expected message 'Error message', got '%s'", err.Message)
	}
}

func TestNewServiceError_EmptyMessage(t *testing.T) {
	err := NewServiceError("ERROR_CODE", "")

	if err.Code != "ERROR_CODE" {
		t.Errorf("Expected code 'ERROR_CODE', got '%s'", err.Code)
	}
	if err.Message != "" {
		t.Errorf("Expected empty message, got '%s'", err.Message)
	}
}

func TestNewServiceError_BothEmpty(t *testing.T) {
	err := NewServiceError("", "")

	if err.Code != "" {
		t.Errorf("Expected empty code, got '%s'", err.Code)
	}
	if err.Message != "" {
		t.Errorf("Expected empty message, got '%s'", err.Message)
	}
	if err.Details != nil {
		t.Errorf("Expected nil details, got %v", err.Details)
	}
}

func TestNewServiceErrorWithDetails_EmptyDetails(t *testing.T) {
	details := map[string]interface{}{}

	err := NewServiceErrorWithDetails("ERROR", "Message", details)

	if err.Details == nil {
		t.Fatal("Expected non-nil details")
	}
	if len(err.Details) != 0 {
		t.Errorf("Expected empty details map, got %d items", len(err.Details))
	}
}

func TestNewServiceErrorWithDetails_NilDetails(t *testing.T) {
	err := NewServiceErrorWithDetails("ERROR", "Message", nil)

	if err.Code != "ERROR" {
		t.Errorf("Expected code 'ERROR', got '%s'", err.Code)
	}
	if err.Message != "Message" {
		t.Errorf("Expected message 'Message', got '%s'", err.Message)
	}
	if err.Details != nil {
		t.Errorf("Expected nil details, got %v", err.Details)
	}
}

func TestNewServiceErrorWithDetails_ComplexDetails(t *testing.T) {
	details := map[string]interface{}{
		"string": "value",
		"number": 42,
		"float":  3.14,
		"bool":   true,
		"array":  []int{1, 2, 3},
		"map":    map[string]string{"key": "val"},
		"nil":    nil,
	}

	err := NewServiceErrorWithDetails("COMPLEX_ERROR", "Complex error", details)

	if err.Details == nil {
		t.Fatal("Expected non-nil details")
	}
	if len(err.Details) != 7 {
		t.Errorf("Expected 7 detail items, got %d", len(err.Details))
	}
	if err.Details["string"] != "value" {
		t.Errorf("Expected string 'value', got %v", err.Details["string"])
	}
	if err.Details["number"] != 42 {
		t.Errorf("Expected number 42, got %v", err.Details["number"])
	}
	if err.Details["bool"] != true {
		t.Errorf("Expected bool true, got %v", err.Details["bool"])
	}
}

func TestServiceError_JSONMarshal(t *testing.T) {
	err := &ServiceError{
		Code:    "TEST_ERROR",
		Message: "Test message",
		Details: map[string]interface{}{
			"field": "value",
		},
	}

	jsonBytes, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatalf("Failed to marshal ServiceError: %v", marshalErr)
	}

	var unmarshaled ServiceError
	if unmarshalErr := json.Unmarshal(jsonBytes, &unmarshaled); unmarshalErr != nil {
		t.Fatalf("Failed to unmarshal ServiceError: %v", unmarshalErr)
	}

	if unmarshaled.Code != err.Code {
		t.Errorf("Expected code '%s', got '%s'", err.Code, unmarshaled.Code)
	}
	if unmarshaled.Message != err.Message {
		t.Errorf("Expected message '%s', got '%s'", err.Message, unmarshaled.Message)
	}
}

func TestServiceError_JSONMarshalOmitsEmptyDetails(t *testing.T) {
	err := &ServiceError{
		Code:    "TEST_ERROR",
		Message: "Test message",
		Details: nil,
	}

	jsonBytes, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatalf("Failed to marshal ServiceError: %v", marshalErr)
	}

	jsonString := string(jsonBytes)
	if strings.Contains(jsonString, "details") {
		t.Error("Expected 'details' field to be omitted in JSON")
	}
}

func TestServiceError_AsErrorInterface(t *testing.T) {
	err := NewServiceError("TEST", "Test error")

	var genericError error = err
	if genericError.Error() != "Test error" {
		t.Errorf("Expected 'Test error', got '%s'", genericError.Error())
	}
}

func TestServiceError_MultipleInstances(t *testing.T) {
	err1 := NewServiceError("ERROR1", "First error")
	err2 := NewServiceError("ERROR2", "Second error")

	if err1.Code == err2.Code {
		t.Error("Expected different error codes")
	}
	if err1.Message == err2.Message {
		t.Error("Expected different error messages")
	}
}

func TestServiceError_CodeCaseSensitivity(t *testing.T) {
	err1 := NewServiceError("ERROR_CODE", "Message")
	err2 := NewServiceError("error_code", "Message")

	if err1.Code == err2.Code {
		t.Log("Warning: Error codes are case-sensitive")
	}
	// Just verify they're stored as provided
	if err1.Code != "ERROR_CODE" {
		t.Errorf("Expected 'ERROR_CODE', got '%s'", err1.Code)
	}
	if err2.Code != "error_code" {
		t.Errorf("Expected 'error_code', got '%s'", err2.Code)
	}
}

func TestServiceError_DetailsImmutability(t *testing.T) {
	originalDetails := map[string]interface{}{
		"field": "value",
	}

	err := NewServiceErrorWithDetails("ERROR", "Message", originalDetails)

	// Modify original map
	originalDetails["field"] = "modified"

	// Check if error's details are affected (they will be in Go)
	if err.Details["field"] == "modified" {
		t.Log("Note: Details map is not deep-copied, shares reference with original")
	}
}
