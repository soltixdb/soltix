package storage

import (
	"encoding/binary"
	"fmt"
	"time"

	pb "github.com/soltixdb/soltix/proto/storage/v1"
	"google.golang.org/protobuf/proto"
)

// DeviceBlock represents a compressed block of data for a single device
type DeviceBlock struct {
	DeviceID   string
	EntryCount uint32

	// Delta-encoded timestamps
	BaseTime int64    // First timestamp (Unix nano)
	Deltas   []uint32 // Delta from previous timestamp (milliseconds)

	// Field data (will be JSON encoded, then compressed)
	Fields []map[string]interface{}
}

// NewDeviceBlock creates a new device block from data points
func NewDeviceBlock(deviceID string, points []*DataPoint) (*DeviceBlock, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("empty data points")
	}

	// Sort by time (should already be sorted, but ensure)
	// points should be pre-sorted by caller

	block := &DeviceBlock{
		DeviceID:   deviceID,
		EntryCount: uint32(len(points)),
		BaseTime:   points[0].Time.UnixNano(),
		Deltas:     make([]uint32, len(points)-1),
		Fields:     make([]map[string]interface{}, len(points)),
	}

	// Delta encode timestamps
	prevTime := points[0].Time.UnixNano()
	for i := 1; i < len(points); i++ {
		currentTime := points[i].Time.UnixNano()
		deltaNano := currentTime - prevTime

		// Convert to milliseconds
		deltaMs := uint32(deltaNano / int64(time.Millisecond))
		block.Deltas[i-1] = deltaMs

		prevTime = currentTime
	}

	// Copy field data and add system metadata
	for i, point := range points {
		// Create new map with user fields + system metadata
		fieldsWithMeta := make(map[string]interface{}, len(point.Fields)+1)

		// Copy user fields
		for k, v := range point.Fields {
			fieldsWithMeta[k] = v
		}

		// Add InsertedAt as system metadata (prefix with _ to avoid conflicts)
		fieldsWithMeta["_inserted_at"] = point.InsertedAt.UnixNano()

		block.Fields[i] = fieldsWithMeta
	}

	return block, nil
}

// Encode encodes the block to binary format
func (b *DeviceBlock) Encode() ([]byte, error) {
	// Estimate size
	estimatedSize := 8 + 4 + len(b.DeviceID) + 4 + 8 + len(b.Deltas)*4
	buf := make([]byte, 0, estimatedSize)

	// Device ID length + Device ID
	deviceIDBytes := []byte(b.DeviceID)
	deviceIDLen := uint32(len(deviceIDBytes))
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, deviceIDLen)
	buf = append(buf, lenBuf...)
	buf = append(buf, deviceIDBytes...)

	// Entry count
	binary.LittleEndian.PutUint32(lenBuf, b.EntryCount)
	buf = append(buf, lenBuf...)

	// Base timestamp
	timeBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBuf, uint64(b.BaseTime))
	buf = append(buf, timeBuf...)

	// Deltas
	deltasBuf := make([]byte, len(b.Deltas)*4)
	for i, delta := range b.Deltas {
		binary.LittleEndian.PutUint32(deltasBuf[i*4:], delta)
	}
	buf = append(buf, deltasBuf...)

	// Fields (JSON encode)
	// Note: In production, consider more efficient encoding like MessagePack
	// For now, we'll return the metadata, fields will be encoded separately

	return buf, nil
}

// DecodeDeviceBlock decodes a device block from binary
func DecodeDeviceBlock(data []byte) (*DeviceBlock, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too short")
	}

	offset := 0

	// Device ID
	deviceIDLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(deviceIDLen) > len(data) {
		return nil, fmt.Errorf("invalid device ID length")
	}

	deviceID := string(data[offset : offset+int(deviceIDLen)])
	offset += int(deviceIDLen)

	// Entry count
	if offset+4 > len(data) {
		return nil, fmt.Errorf("data too short for entry count")
	}
	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Base timestamp
	if offset+8 > len(data) {
		return nil, fmt.Errorf("data too short for base time")
	}
	baseTime := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Deltas
	deltaCount := int(entryCount) - 1
	if deltaCount < 0 {
		deltaCount = 0
	}

	deltasSize := deltaCount * 4
	if offset+deltasSize > len(data) {
		return nil, fmt.Errorf("data too short for deltas")
	}

	deltas := make([]uint32, deltaCount)
	for i := 0; i < deltaCount; i++ {
		deltas[i] = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}

	return &DeviceBlock{
		DeviceID:   deviceID,
		EntryCount: entryCount,
		BaseTime:   baseTime,
		Deltas:     deltas,
		Fields:     nil, // Fields decoded separately
	}, nil
}

// GetTimestamps reconstructs timestamps from base + deltas
func (b *DeviceBlock) GetTimestamps() []time.Time {
	timestamps := make([]time.Time, b.EntryCount)
	timestamps[0] = time.Unix(0, b.BaseTime)

	currentTime := b.BaseTime
	for i := 0; i < len(b.Deltas); i++ {
		currentTime += int64(b.Deltas[i]) * int64(time.Millisecond)
		timestamps[i+1] = time.Unix(0, currentTime)
	}

	return timestamps
}

// CompressionStats returns statistics about delta encoding efficiency
func (b *DeviceBlock) CompressionStats() map[string]interface{} {
	// Original size: entryCount * 8 bytes (int64 timestamps)
	originalSize := int(b.EntryCount) * 8

	// Delta size: 8 (base) + (entryCount-1) * 4 (uint32 deltas)
	deltaSize := 8 + (int(b.EntryCount)-1)*4

	ratio := float64(originalSize) / float64(deltaSize)

	return map[string]interface{}{
		"original_bytes": originalSize,
		"delta_bytes":    deltaSize,
		"compression":    fmt.Sprintf("%.2fx", ratio),
	}
}

// toProtoFieldValue converts interface{} to pb.FieldValue
func toProtoFieldValue(v interface{}) *pb.FieldValue {
	fv := &pb.FieldValue{}
	switch val := v.(type) {
	case float64:
		fv.Value = &pb.FieldValue_NumberValue{NumberValue: val}
	case string:
		fv.Value = &pb.FieldValue_StringValue{StringValue: val}
	case bool:
		fv.Value = &pb.FieldValue_BoolValue{BoolValue: val}
	case int:
		fv.Value = &pb.FieldValue_IntValue{IntValue: int64(val)}
	case int64:
		fv.Value = &pb.FieldValue_IntValue{IntValue: val}
	default:
		// For unknown types, convert to string
		fv.Value = &pb.FieldValue_StringValue{StringValue: fmt.Sprintf("%v", val)}
	}
	return fv
}

// fromProtoFieldValue converts pb.FieldValue to interface{}
func fromProtoFieldValue(fv *pb.FieldValue) interface{} {
	switch v := fv.Value.(type) {
	case *pb.FieldValue_NumberValue:
		return v.NumberValue
	case *pb.FieldValue_StringValue:
		return v.StringValue
	case *pb.FieldValue_BoolValue:
		return v.BoolValue
	case *pb.FieldValue_IntValue:
		return v.IntValue
	default:
		return nil
	}
}

// EncodeFieldsProto encodes fields array to protobuf bytes
func EncodeFieldsProto(fields []map[string]interface{}) ([]byte, error) {
	pbFields := make([]*pb.FieldsEntry, len(fields))

	for i, fieldMap := range fields {
		pbFieldMap := make(map[string]*pb.FieldValue)
		for k, v := range fieldMap {
			pbFieldMap[k] = toProtoFieldValue(v)
		}
		pbFields[i] = &pb.FieldsEntry{Fields: pbFieldMap}
	}

	// Create a container message for the fields array
	pbBlock := &pb.DeviceBlock{Fields: pbFields}

	return proto.Marshal(pbBlock)
}

// DecodeFieldsProto decodes protobuf bytes to fields array
func DecodeFieldsProto(data []byte) ([]map[string]interface{}, error) {
	pbBlock := &pb.DeviceBlock{}
	if err := proto.Unmarshal(data, pbBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal fields: %w", err)
	}

	fields := make([]map[string]interface{}, len(pbBlock.Fields))
	for i, pbEntry := range pbBlock.Fields {
		fieldMap := make(map[string]interface{})
		for k, pbValue := range pbEntry.Fields {
			fieldMap[k] = fromProtoFieldValue(pbValue)
		}
		fields[i] = fieldMap
	}

	return fields, nil
}
