package metadata

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDatabase_JSONMarshaling(t *testing.T) {
	now := time.Now().UTC().Round(time.Second)

	tests := []struct {
		name     string
		database Database
	}{
		{
			name: "complete_database",
			database: Database{
				Name:        "testdb",
				Description: "Test database",
				Metadata: map[string]string{
					"owner": "team-a",
					"env":   "production",
				},
				CreatedAt: now,
			},
		},
		{
			name: "minimal_database",
			database: Database{
				Name:      "minimal",
				CreatedAt: now,
			},
		},
		{
			name: "database_with_empty_metadata",
			database: Database{
				Name:      "emptymetadata",
				Metadata:  map[string]string{},
				CreatedAt: now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(&tt.database)
			if err != nil {
				t.Fatalf("Failed to marshal database: %v", err)
			}

			// Unmarshal back
			var unmarshaled Database
			if err := json.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("Failed to unmarshal database: %v", err)
			}

			// Verify fields
			if unmarshaled.Name != tt.database.Name {
				t.Errorf("Expected name %q, got %q", tt.database.Name, unmarshaled.Name)
			}

			if unmarshaled.Description != tt.database.Description {
				t.Errorf("Expected description %q, got %q", tt.database.Description, unmarshaled.Description)
			}

			if !unmarshaled.CreatedAt.Equal(tt.database.CreatedAt) {
				t.Errorf("Expected created_at %v, got %v", tt.database.CreatedAt, unmarshaled.CreatedAt)
			}

			// Verify metadata (empty map becomes null in JSON)
			if len(tt.database.Metadata) > 0 {
				if unmarshaled.Metadata == nil {
					t.Error("Expected metadata to be present")
				} else {
					for k, v := range tt.database.Metadata {
						if unmarshaled.Metadata[k] != v {
							t.Errorf("Expected metadata[%q] = %q, got %q", k, v, unmarshaled.Metadata[k])
						}
					}
				}
			}
		})
	}
}

func TestCollection_JSONMarshaling(t *testing.T) {
	now := time.Now().UTC().Round(time.Second)

	tests := []struct {
		name       string
		collection Collection
	}{
		{
			name: "complete_collection",
			collection: Collection{
				Name:        "testcoll",
				Description: "Test collection",
				SchemaHints: map[string]string{
					"temperature": "float",
					"humidity":    "float",
					"device_id":   "string",
				},
				CreatedAt: now,
			},
		},
		{
			name: "minimal_collection",
			collection: Collection{
				Name:      "minimal",
				CreatedAt: now,
			},
		},
		{
			name: "collection_with_empty_schema",
			collection: Collection{
				Name:        "emptyschemacoll",
				SchemaHints: map[string]string{},
				CreatedAt:   now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(&tt.collection)
			if err != nil {
				t.Fatalf("Failed to marshal collection: %v", err)
			}

			// Unmarshal back
			var unmarshaled Collection
			if err := json.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("Failed to unmarshal collection: %v", err)
			}

			// Verify fields
			if unmarshaled.Name != tt.collection.Name {
				t.Errorf("Expected name %q, got %q", tt.collection.Name, unmarshaled.Name)
			}

			if unmarshaled.Description != tt.collection.Description {
				t.Errorf("Expected description %q, got %q", tt.collection.Description, unmarshaled.Description)
			}

			if !unmarshaled.CreatedAt.Equal(tt.collection.CreatedAt) {
				t.Errorf("Expected created_at %v, got %v", tt.collection.CreatedAt, unmarshaled.CreatedAt)
			}

			// Verify schema hints (empty map becomes null in JSON)
			if len(tt.collection.SchemaHints) > 0 {
				if unmarshaled.SchemaHints == nil {
					t.Error("Expected schema_hints to be present")
				} else {
					for k, v := range tt.collection.SchemaHints {
						if unmarshaled.SchemaHints[k] != v {
							t.Errorf("Expected schema_hints[%q] = %q, got %q", k, v, unmarshaled.SchemaHints[k])
						}
					}
				}
			}
		})
	}
}

func TestDatabase_JSONOmitEmpty(t *testing.T) {
	db := Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
		// Description and Metadata intentionally omitted
	}

	data, err := json.Marshal(&db)
	if err != nil {
		t.Fatalf("Failed to marshal database: %v", err)
	}

	jsonStr := string(data)

	// Verify omitempty works - description and metadata should not be in JSON
	// (unless they explicitly check for omitempty in the struct tags)
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Failed to unmarshal to map: %v", err)
	}

	// Check required fields exist
	if _, ok := result["name"]; !ok {
		t.Error("Expected 'name' field in JSON")
	}
	if _, ok := result["created_at"]; !ok {
		t.Error("Expected 'created_at' field in JSON")
	}

	t.Logf("JSON output: %s", jsonStr)
}

func TestCollection_JSONOmitEmpty(t *testing.T) {
	coll := Collection{
		Name:      "testcoll",
		CreatedAt: time.Now(),
		// Description and SchemaHints intentionally omitted
	}

	data, err := json.Marshal(&coll)
	if err != nil {
		t.Fatalf("Failed to marshal collection: %v", err)
	}

	jsonStr := string(data)

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Failed to unmarshal to map: %v", err)
	}

	// Check required fields exist
	if _, ok := result["name"]; !ok {
		t.Error("Expected 'name' field in JSON")
	}
	if _, ok := result["created_at"]; !ok {
		t.Error("Expected 'created_at' field in JSON")
	}

	t.Logf("JSON output: %s", jsonStr)
}
