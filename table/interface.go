// Package table defines a generic, transport-independent interface for
// backends that expose graph data as named collections of rows.
package table

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrCollectionNotFound means a requested table or collection does not exist.
	ErrCollectionNotFound = errors.New("table collection not found")
	// ErrRowNotFound means a requested row ID does not exist.
	ErrRowNotFound = errors.New("table row not found")
	// ErrUnsupported means an operation is not provided by this table backend.
	ErrUnsupported = errors.New("table operation not supported")
	// ErrInvalidRequest means table metadata or an operation argument is invalid.
	ErrInvalidRequest = errors.New("invalid table request")
	// ErrMalformedRow means a row returned by a backend could not be validated.
	ErrMalformedRow = errors.New("malformed table row")
)

// FieldType describes the JSON-compatible value type of a table field.
type FieldType string

const (
	FieldTypeUnknown FieldType = ""
	FieldTypeString  FieldType = "string"
	FieldTypeInteger FieldType = "integer"
	FieldTypeNumber  FieldType = "number"
	FieldTypeBoolean FieldType = "boolean"
	FieldTypeObject  FieldType = "object"
	FieldTypeArray   FieldType = "array"
	FieldTypeNull    FieldType = "null"
	FieldTypeDate    FieldType = "date"
	FieldTypeBinary  FieldType = "binary"
)

// Field describes one field exposed by a collection.
type Field struct {
	Name    string    `json:"name"`
	Type    FieldType `json:"type,omitempty"`
	Indexed bool      `json:"indexed,omitempty"`
}

// Collection describes a named table and its primary key and fields.
type Collection struct {
	Name       string  `json:"name"`
	PrimaryKey string  `json:"primaryKey,omitempty"`
	Fields     []Field `json:"fields,omitempty"`
}

// Capabilities describes operations available from a TableDriver.
type Capabilities struct {
	Readable    bool `json:"readable"`
	Writable    bool `json:"writable,omitempty"`
	BatchLookup bool `json:"batchLookup,omitempty"`
	BatchWrite  bool `json:"batchWrite,omitempty"`
}

// Row is one record returned by a table backend.
type Row struct {
	ID   string                 `json:"id"`
	Data map[string]interface{} `json:"data"`
}

// ScanOptions controls collection scans.
type ScanOptions struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// RowResult carries one streamed row or a terminal per-row/backend error.
// A nil Row with nil Err is invalid and should be reported by ValidateRow.
type RowResult struct {
	Row *Row  `json:"row,omitempty"`
	Err error `json:"-"`
}

// RowStream is a streaming collection of table rows and errors.
type RowStream <-chan RowResult

// TableDriver is the transport-independent interface for a table backend.
// Graph adapters, including the GRIPPER resource-node adapter, consume this
// interface rather than owning or defining it.
type TableDriver interface {
	Capabilities() Capabilities
	ListCollections(ctx context.Context) ([]Collection, error)
	GetCollection(ctx context.Context, name string) (Collection, error)
	Scan(ctx context.Context, collection string, options ScanOptions) (RowStream, error)
	LookupByID(ctx context.Context, collection, id string) (*Row, error)
	LookupByField(ctx context.Context, collection, field string, value interface{}) (RowStream, error)
}

// BatchTableDriver is an optional batched lookup extension.
type BatchTableDriver interface {
	TableDriver
	LookupByIDs(ctx context.Context, collection string, ids []string) (RowStream, error)
}

// WriteTableDriver is an optional mutation extension.
type WriteTableDriver interface {
	TableDriver
	PutRow(ctx context.Context, collection string, row *Row) error
	DeleteRow(ctx context.Context, collection, id string) error
}

// BatchWriteTableDriver is an optional bulk-mutation extension.
type BatchWriteTableDriver interface {
	WriteTableDriver
	PutRows(ctx context.Context, collection string, rows []*Row) error
	DeleteRows(ctx context.Context, collection string, ids []string) error
}

// CloseTableDriver is implemented by table drivers that own resources which
// should be closed with the table GraphDB.
type CloseTableDriver interface {
	Close() error
}

// ValidateCollection verifies enough metadata for graph mapping code to use it.
func ValidateCollection(collection Collection) error {
	if collection.Name == "" {
		return fmt.Errorf("%w: collection name is empty", ErrInvalidRequest)
	}
	seen := make(map[string]bool, len(collection.Fields))
	for _, field := range collection.Fields {
		if field.Name == "" {
			return fmt.Errorf("%w: collection %q has an unnamed field", ErrInvalidRequest, collection.Name)
		}
		if seen[field.Name] {
			return fmt.Errorf("%w: collection %q repeats field %q", ErrInvalidRequest, collection.Name, field.Name)
		}
		seen[field.Name] = true
	}
	if collection.PrimaryKey != "" && !seen[collection.PrimaryKey] {
		return fmt.Errorf("%w: collection %q primary key %q is not a field", ErrInvalidRequest, collection.Name, collection.PrimaryKey)
	}
	return nil
}

// ValidateScanOptions verifies scan paging and result limits.
func ValidateScanOptions(options ScanOptions) error {
	if options.Limit < 0 {
		return fmt.Errorf("%w: scan limit cannot be negative", ErrInvalidRequest)
	}
	return nil
}

// ValidateRow verifies that a table row has an ID and data object.
func ValidateRow(row *Row) error {
	if row == nil {
		return fmt.Errorf("%w: row is nil", ErrMalformedRow)
	}
	if row.ID == "" {
		return fmt.Errorf("%w: row ID is empty", ErrMalformedRow)
	}
	if row.Data == nil {
		return fmt.Errorf("%w: row %q has nil data", ErrMalformedRow, row.ID)
	}
	return nil
}
