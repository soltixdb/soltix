// Package services provides the business logic layer between handlers and coordinators.
// Services encapsulate complex business logic, data transformation, and orchestration.
package services

// ServiceError represents a service layer error
type ServiceError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

func (e *ServiceError) Error() string {
	return e.Message
}

// NewServiceError creates a new ServiceError
func NewServiceError(code, message string) *ServiceError {
	return &ServiceError{
		Code:    code,
		Message: message,
	}
}

// NewServiceErrorWithDetails creates a new ServiceError with details
func NewServiceErrorWithDetails(code, message string, details map[string]interface{}) *ServiceError {
	return &ServiceError{
		Code:    code,
		Message: message,
		Details: details,
	}
}
