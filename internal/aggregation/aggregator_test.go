package aggregation

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestNewAggregatedField(t *testing.T) {
	field := NewAggregatedField(10.0)

	if field.Count != 1 {
		t.Errorf("Expected Count=1, got %d", field.Count)
	}
	if field.Sum != 10.0 {
		t.Errorf("Expected Sum=10.0, got %f", field.Sum)
	}
	if field.Avg != 10.0 {
		t.Errorf("Expected Avg=10.0, got %f", field.Avg)
	}
	if field.Min != 10.0 {
		t.Errorf("Expected Min=10.0, got %f", field.Min)
	}
	if field.Max != 10.0 {
		t.Errorf("Expected Max=10.0, got %f", field.Max)
	}
	if field.SumSquares != 100.0 {
		t.Errorf("Expected SumSquares=100.0, got %f", field.SumSquares)
	}
	if field.MinTime != 0 {
		t.Errorf("Expected MinTime=0, got %d", field.MinTime)
	}
	if field.MaxTime != 0 {
		t.Errorf("Expected MaxTime=0, got %d", field.MaxTime)
	}
}

func TestNewAggregatedFieldWithTime(t *testing.T) {
	ts := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	field := NewAggregatedFieldWithTime(10.0, ts)

	if field.MinTime != ts.UnixNano() {
		t.Errorf("Expected MinTime=%d, got %d", ts.UnixNano(), field.MinTime)
	}
	if field.MaxTime != ts.UnixNano() {
		t.Errorf("Expected MaxTime=%d, got %d", ts.UnixNano(), field.MaxTime)
	}
}

func TestAggregatedField_AddValue(t *testing.T) {
	field := NewAggregatedField(10.0)

	field.AddValue(20.0)
	field.AddValue(30.0)

	if field.Count != 3 {
		t.Errorf("Expected Count=3, got %d", field.Count)
	}
	if field.Sum != 60.0 {
		t.Errorf("Expected Sum=60.0, got %f", field.Sum)
	}
	if field.Avg != 20.0 {
		t.Errorf("Expected Avg=20.0, got %f", field.Avg)
	}
	if field.Min != 10.0 {
		t.Errorf("Expected Min=10.0, got %f", field.Min)
	}
	if field.Max != 30.0 {
		t.Errorf("Expected Max=30.0, got %f", field.Max)
	}
}

func TestAggregatedField_AddValue_UpdatesMinMax(t *testing.T) {
	base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	field := NewAggregatedFieldWithTime(50.0, base)

	field.AddValueWithTime(10.0, base.Add(time.Minute))    // New min
	field.AddValueWithTime(100.0, base.Add(2*time.Minute)) // New max
	field.AddValueWithTime(30.0, base.Add(3*time.Minute))

	if field.Min != 10.0 {
		t.Errorf("Expected Min=10.0, got %f", field.Min)
	}
	if field.Max != 100.0 {
		t.Errorf("Expected Max=100.0, got %f", field.Max)
	}
	if field.Count != 4 {
		t.Errorf("Expected Count=4, got %d", field.Count)
	}
	if field.MinTime != base.Add(time.Minute).UnixNano() {
		t.Errorf("Expected MinTime=%d, got %d", base.Add(time.Minute).UnixNano(), field.MinTime)
	}
	if field.MaxTime != base.Add(2*time.Minute).UnixNano() {
		t.Errorf("Expected MaxTime=%d, got %d", base.Add(2*time.Minute).UnixNano(), field.MaxTime)
	}
}

func TestAggregatedField_Merge(t *testing.T) {
	base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	field1 := NewAggregatedFieldWithTime(10.0, base)
	field1.AddValueWithTime(20.0, base.Add(time.Minute))
	field1.AddValueWithTime(30.0, base.Add(2*time.Minute))
	// field1: count=3, sum=60, avg=20, min=10, max=30

	field2 := NewAggregatedFieldWithTime(5.0, base.Add(3*time.Minute))
	field2.AddValueWithTime(35.0, base.Add(4*time.Minute))
	// field2: count=2, sum=40, avg=20, min=5, max=35

	field1.Merge(field2)

	if field1.Count != 5 {
		t.Errorf("Expected Count=5, got %d", field1.Count)
	}
	if field1.Sum != 100.0 {
		t.Errorf("Expected Sum=100.0, got %f", field1.Sum)
	}
	if field1.Avg != 20.0 {
		t.Errorf("Expected Avg=20.0, got %f", field1.Avg)
	}
	if field1.Min != 5.0 {
		t.Errorf("Expected Min=5.0, got %f", field1.Min)
	}
	if field1.Max != 35.0 {
		t.Errorf("Expected Max=35.0, got %f", field1.Max)
	}
	if field1.MinTime != base.Add(3*time.Minute).UnixNano() {
		t.Errorf("Expected MinTime=%d, got %d", base.Add(3*time.Minute).UnixNano(), field1.MinTime)
	}
	if field1.MaxTime != base.Add(4*time.Minute).UnixNano() {
		t.Errorf("Expected MaxTime=%d, got %d", base.Add(4*time.Minute).UnixNano(), field1.MaxTime)
	}
}

func TestAggregatedField_Merge_SumSquares(t *testing.T) {
	field1 := NewAggregatedField(10.0) // SumSquares = 100
	field2 := NewAggregatedField(20.0) // SumSquares = 400

	field1.Merge(field2)

	expectedSumSquares := 100.0 + 400.0
	if field1.SumSquares != expectedSumSquares {
		t.Errorf("Expected SumSquares=%f, got %f", expectedSumSquares, field1.SumSquares)
	}
}

func TestAggregatedField_Variance(t *testing.T) {
	field := NewAggregatedField(10.0)
	field.AddValue(20.0)
	field.AddValue(30.0)
	// Values: [10, 20, 30], Avg = 20
	// Variance = ((10-20)² + (20-20)² + (30-20)²) / 3 = (100 + 0 + 100) / 3 = 66.67

	variance := field.Variance()
	expected := 66.66666666666667

	if math.Abs(variance-expected) > 0.0001 {
		t.Errorf("Expected Variance≈%f, got %f", expected, variance)
	}
}

func TestAggregatedField_Variance_SingleValue(t *testing.T) {
	field := NewAggregatedField(10.0)

	variance := field.Variance()
	if variance != 0.0 {
		t.Errorf("Expected Variance=0.0 for single value, got %f", variance)
	}
}

func TestAggregatedField_StdDev(t *testing.T) {
	field := NewAggregatedField(10.0)
	field.AddValue(20.0)
	field.AddValue(30.0)

	stdDev := field.StdDev()
	expectedVariance := 66.66666666666667
	expectedStdDev := math.Sqrt(expectedVariance)

	if math.Abs(stdDev-expectedStdDev) > 0.0001 {
		t.Errorf("Expected StdDev≈%f, got %f", expectedStdDev, stdDev)
	}
}

func TestAggregatedField_ToProto(t *testing.T) {
	field := &AggregatedField{
		Count:      10,
		Sum:        100.5,
		Avg:        10.05,
		Min:        5.0,
		Max:        15.0,
		SumSquares: 1200.0,
	}

	proto := field.ToProto()

	if proto.Count != 10 {
		t.Errorf("Expected Count=10, got %d", proto.Count)
	}
	if proto.Sum != 100.5 {
		t.Errorf("Expected Sum=100.5, got %f", proto.Sum)
	}
	if proto.Avg != 10.05 {
		t.Errorf("Expected Avg=10.05, got %f", proto.Avg)
	}
	if proto.Min != 5.0 {
		t.Errorf("Expected Min=5.0, got %f", proto.Min)
	}
	if proto.Max != 15.0 {
		t.Errorf("Expected Max=15.0, got %f", proto.Max)
	}
	if proto.SumSquares != 1200.0 {
		t.Errorf("Expected SumSquares=1200.0, got %f", proto.SumSquares)
	}
}

func TestAggregatedFieldFromProto(t *testing.T) {
	field := &AggregatedField{
		Count:      10,
		Sum:        100.5,
		Avg:        10.05,
		Min:        5.0,
		Max:        15.0,
		SumSquares: 1200.0,
	}

	proto := field.ToProto()
	restored := AggregatedFieldFromProto(proto)

	if restored.Count != field.Count {
		t.Errorf("Expected Count=%d, got %d", field.Count, restored.Count)
	}
	if restored.Sum != field.Sum {
		t.Errorf("Expected Sum=%f, got %f", field.Sum, restored.Sum)
	}
	if restored.Avg != field.Avg {
		t.Errorf("Expected Avg=%f, got %f", field.Avg, restored.Avg)
	}
	if restored.Min != field.Min {
		t.Errorf("Expected Min=%f, got %f", field.Min, restored.Min)
	}
	if restored.Max != field.Max {
		t.Errorf("Expected Max=%f, got %f", field.Max, restored.Max)
	}
	if restored.SumSquares != field.SumSquares {
		t.Errorf("Expected SumSquares=%f, got %f", field.SumSquares, restored.SumSquares)
	}
}

func TestAggregatedField_NegativeValues(t *testing.T) {
	field := NewAggregatedField(-10.0)
	field.AddValue(-5.0)
	field.AddValue(-15.0)

	if field.Count != 3 {
		t.Errorf("Expected Count=3, got %d", field.Count)
	}
	if field.Sum != -30.0 {
		t.Errorf("Expected Sum=-30.0, got %f", field.Sum)
	}
	if field.Avg != -10.0 {
		t.Errorf("Expected Avg=-10.0, got %f", field.Avg)
	}
	if field.Min != -15.0 {
		t.Errorf("Expected Min=-15.0, got %f", field.Min)
	}
	if field.Max != -5.0 {
		t.Errorf("Expected Max=-5.0, got %f", field.Max)
	}
}

func TestAggregatedField_ZeroValues(t *testing.T) {
	field := NewAggregatedField(0.0)
	field.AddValue(0.0)
	field.AddValue(0.0)

	if field.Count != 3 {
		t.Errorf("Expected Count=3, got %d", field.Count)
	}
	if field.Sum != 0.0 {
		t.Errorf("Expected Sum=0.0, got %f", field.Sum)
	}
	if field.Avg != 0.0 {
		t.Errorf("Expected Avg=0.0, got %f", field.Avg)
	}
	if field.Variance() != 0.0 {
		t.Errorf("Expected Variance=0.0, got %f", field.Variance())
	}
}

// =============================================================================
// decodeV6AggFooter — truncation boundary tests
// =============================================================================

func TestDecodeV6AggFooter_TooShort_Boundary(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	_, err := s.decodeV6AggFooter([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("Expected error for too-short footer")
	}
}

func TestDecodeV6AggFooter_TruncatedAtDeviceName(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	// Encode: deviceCount=1, then truncate before name length
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1) // 1 device
	// No more data — truncated at device name

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated footer at device name")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtDeviceNameData(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	buf := make([]byte, 0, 32)
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, 1) // 1 device
	buf = append(buf, intBuf...)
	// Name length = 10 but only provide 2 bytes of name data
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 10)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("ab")...) // Only 2 bytes, need 10

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated device name data")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtRowCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	buf := make([]byte, 0, 32)
	intBuf := make([]byte, 4)
	// 1 device named "d"
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	// Truncate before row count

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated row count")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtFieldCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	// 1 device named "d" with row count 5
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	// Truncate before field count

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field count")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtFieldName(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	// 1 device named "d" with row count 5
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	// Field count = 1
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	// Truncate before field name length

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field name")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtFieldNameData(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	buf := make([]byte, 0, 64)
	intBuf := make([]byte, 4)
	// 1 device named "d" with row count 5
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	nameLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(nameLenBuf, 1)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, 'd')
	binary.LittleEndian.PutUint32(intBuf, 5) // row count
	buf = append(buf, intBuf...)
	// Field count = 1
	binary.LittleEndian.PutUint32(intBuf, 1)
	buf = append(buf, intBuf...)
	// Field name length = 8, but only 2 bytes follow
	binary.LittleEndian.PutUint16(nameLenBuf, 8)
	buf = append(buf, nameLenBuf...)
	buf = append(buf, []byte("ab")...)

	_, err := s.decodeV6AggFooter(buf)
	if err == nil {
		t.Fatal("Expected error for truncated field name data")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtColumnCount(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	// Build a valid footer up to field names, then truncate before column count
	footer := &V6AggPartFooter{
		DeviceNames:    []string{"dev1"},
		FieldNames:     []string{"_time", "temp"},
		Columns:        []V6AggColumnEntry{},
		RowCountPerDev: []uint32{3},
	}
	full := s.encodeV6AggFooter(footer)

	// Find where column count starts — after device names + row counts + field names
	// Let's truncate 5 bytes from the end (less than the 4-byte column count)
	truncated := full[:len(full)-1]

	_, err := s.decodeV6AggFooter(truncated)
	if err == nil {
		t.Fatal("Expected error for truncated column count")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDecodeV6AggFooter_TruncatedAtColumnEntry(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	footer := &V6AggPartFooter{
		DeviceNames: []string{"dev1"},
		FieldNames:  []string{"_time", "temp"},
		Columns: []V6AggColumnEntry{
			{DeviceIdx: 0, FieldIdx: 0, MetricIdx: 0xFF, Offset: 64, Size: 100, RowCount: 3, ColumnType: 0},
		},
		RowCountPerDev: []uint32{3},
	}
	full := s.encodeV6AggFooter(footer)

	// Truncate mid-column-entry (remove last 10 bytes of the 26-byte entry)
	truncated := full[:len(full)-10]

	_, err := s.decodeV6AggFooter(truncated)
	if err == nil {
		t.Fatal("Expected error for truncated column entry")
	}
	t.Logf("Got expected error: %v", err)
}

// =============================================================================
// readV6PartFooter — permission denied and truncated file
// =============================================================================

func TestReadV6PartFooter_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping permission test as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Create a file then make it unreadable
	filePath := filepath.Join(tmpDir, "noperm.bin")
	if err := os.WriteFile(filePath, make([]byte, 200), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filePath, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(filePath, 0o644) }()

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Fatal("Expected error for permission-denied file")
	}
	t.Logf("Got expected error: %v", err)
}

func TestReadV6PartFooter_FileTooSmall(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	filePath := filepath.Join(tmpDir, "small.bin")
	if err := os.WriteFile(filePath, make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	} // Less than V6AggHeaderSize+12

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Fatal("Expected error for too-small file")
	}
}

func TestReadV6PartFooter_InvalidFooterOffsetInFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Create file with valid size but invalid footer offset pointing beyond file
	fileSize := V6AggHeaderSize + 24
	data := make([]byte, fileSize)

	// Set footer offset (last 8 bytes) to something invalid
	binary.LittleEndian.PutUint64(data[fileSize-8:], 999999) // Way beyond file

	// Set footer size (4 bytes before offset)
	binary.LittleEndian.PutUint32(data[fileSize-12:], 100)

	filePath := filepath.Join(tmpDir, "bad_offset.bin")
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := s.readV6PartFooter(filePath)
	if err == nil {
		t.Fatal("Expected error for invalid footer offset")
	}
}

// =============================================================================
// compactDGParts — edge cases
// =============================================================================

func TestCompactDGParts_SinglePart(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	parts := []PartInfo{{PartNum: 0, FileName: "part_0000.bin", RowCount: 10}}
	result, err := s.compactDGParts(tmpDir, parts, 1000, "1h", false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for single part")
	}
}

func TestCompactDGParts_CorruptPartFooter(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write corrupt file
	if err := os.WriteFile(filepath.Join(dgDir, "part_0000.bin"), []byte("corrupt"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dgDir, "part_0001.bin"), []byte("corrupt2"), 0o644); err != nil {
		t.Fatal(err)
	}

	parts := []PartInfo{
		{PartNum: 0, FileName: "part_0000.bin", RowCount: 5},
		{PartNum: 1, FileName: "part_0001.bin", RowCount: 5},
	}

	_, err := s.compactDGParts(dgDir, parts, 1000, "1h", false)
	if err == nil {
		t.Fatal("Expected error for corrupt part footer")
	}
}

func TestCompactDGParts_MissingPartFile(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Part files don't exist — readV6PartFooter returns nil, nil for non-existent
	parts := []PartInfo{
		{PartNum: 0, FileName: "part_0000.bin", RowCount: 5},
		{PartNum: 1, FileName: "part_0001.bin", RowCount: 5},
	}

	result, err := s.compactDGParts(dgDir, parts, 1000, "1h", false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// All parts missing → allData is nil → should return nil
	if result != nil {
		t.Error("Expected nil result when all part files missing")
	}
}

func TestCompactDGParts_DryRun(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write 3 real parts
	for i := 0; i < 3; i++ {
		data := NewColumnarData(2)
		data.AddRowAllMetrics(int64(i*1000+100), "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i * 10), Avg: float64(i), Min: 0, Max: float64(i * 2), Count: int64(i + 1)},
		})
		data.AddRowAllMetrics(int64(i*1000+200), "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i * 20), Avg: float64(i * 2), Min: 0, Max: float64(i * 4), Count: int64(i + 2)},
		})
		pw, err := s.writeV6PartFile(dgDir, i, AggregationHourly, data)
		if err != nil {
			t.Fatalf("writeV6PartFile failed: %v", err)
		}
		if pw != nil {
			if err := os.Rename(pw.tmpPath, pw.filePath); err != nil {
				t.Fatal(err)
			}
		}
	}

	parts := []PartInfo{
		{PartNum: 0, FileName: s.getPartFileName(0), RowCount: 2},
		{PartNum: 1, FileName: s.getPartFileName(1), RowCount: 2},
		{PartNum: 2, FileName: s.getPartFileName(2), RowCount: 2},
	}

	// Dry run with target rows = 10 → all 6 rows fit in 1 part
	result, err := s.compactDGParts(dgDir, parts, 10, "1h", true)
	if err != nil {
		t.Fatalf("DryRun compactDGParts failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result for dry run")
	}
	if result.PartsBefore != 3 {
		t.Errorf("PartsBefore = %d, want 3", result.PartsBefore)
	}
	if result.PartsAfter != 1 {
		t.Errorf("PartsAfter = %d, want 1", result.PartsAfter)
	}
}

func TestCompactDGParts_NoCompactionBenefit(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write 2 parts with 1 row each, target rows = 1 → 2 new parts == 2 old parts → no benefit
	for i := 0; i < 2; i++ {
		data := NewColumnarData(1)
		data.AddRowAllMetrics(int64(i*1000+100), "dev1", map[string]*AggregatedField{
			"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
		})
		pw, err := s.writeV6PartFile(dgDir, i, AggregationHourly, data)
		if err != nil {
			t.Fatalf("writeV6PartFile failed: %v", err)
		}
		if pw != nil {
			if err := os.Rename(pw.tmpPath, pw.filePath); err != nil {
				t.Fatal(err)
			}
		}
	}

	parts := []PartInfo{
		{PartNum: 0, FileName: s.getPartFileName(0), RowCount: 1},
		{PartNum: 1, FileName: s.getPartFileName(1), RowCount: 1},
	}

	// Target rows = 1 → each row gets its own part → 2 parts >= 2 → no benefit
	result, err := s.compactDGParts(dgDir, parts, 1, "1h", false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result when compaction has no benefit")
	}
}

func TestCompactDGParts_FullCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dg_0000")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write 3 parts, target rows high → compact to 1 part
	for i := 0; i < 3; i++ {
		data := NewColumnarData(1)
		data.AddRowAllMetrics(int64(i*1000+100), "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i), Avg: float64(i), Min: 0, Max: float64(i), Count: int64(i + 1)},
		})
		pw, err := s.writeV6PartFile(dgDir, i, AggregationHourly, data)
		if err != nil {
			t.Fatalf("writeV6PartFile failed: %v", err)
		}
		if pw != nil {
			if err := os.Rename(pw.tmpPath, pw.filePath); err != nil {
				t.Fatal(err)
			}
		}
	}

	parts := []PartInfo{
		{PartNum: 0, FileName: s.getPartFileName(0), RowCount: 1},
		{PartNum: 1, FileName: s.getPartFileName(1), RowCount: 1},
		{PartNum: 2, FileName: s.getPartFileName(2), RowCount: 1},
	}

	result, err := s.compactDGParts(dgDir, parts, 100, "1h", false)
	if err != nil {
		t.Fatalf("compactDGParts failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if result.PartsAfter != 1 {
		t.Errorf("PartsAfter = %d, want 1", result.PartsAfter)
	}
	if result.RowsMerged != 3 {
		t.Errorf("RowsMerged = %d, want 3", result.RowsMerged)
	}
	if len(result.FilesRemoved) != 3 {
		t.Errorf("FilesRemoved count = %d, want 3", len(result.FilesRemoved))
	}

	// Verify final file exists
	finalPath := filepath.Join(dgDir, s.getPartFileName(0))
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("Final compacted file not found: %v", err)
	}
}

// =============================================================================
// mergeV6ColumnarData — edge cases
// =============================================================================

func TestMergeV6ColumnarData_EmptyFieldName(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	// Create ColumnarData with an invalid empty-field key (shouldn't normally happen)
	data := &ColumnarData{
		Timestamps: []int64{1000},
		DeviceIDs:  []string{"dev1"},
		FloatColumns: map[string][]float64{
			"": {42.0}, // Empty key → v6ParseFloatKey returns "", "" → skip
		},
		IntColumns: map[string][]int64{
			"": {10}, // Empty key → v6ParseIntKey returns "" → skip
		},
	}

	pointsMap := make(map[string]*AggregatedPoint)
	s.mergeV6ColumnarData(data, nil, pointsMap)

	// The empty-field columns should be skipped, point is still created from timestamps
	if len(pointsMap) != 1 {
		t.Errorf("Expected 1 point, got %d", len(pointsMap))
	}
	for _, p := range pointsMap {
		if len(p.Fields) != 0 {
			t.Errorf("Expected 0 fields (empty keys skipped), got %d", len(p.Fields))
		}
	}
}

func TestMergeV6ColumnarData_ShortValueArray(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	// FloatColumns value array shorter than row count → i >= len(values) → skip
	data := NewColumnarData(3)
	data.Timestamps = []int64{1000, 2000, 3000}
	data.DeviceIDs = []string{"dev1", "dev2", "dev3"}
	// Only 1 value but 3 rows
	sumKey := v6FloatKey(MetricSum, "temp")
	data.FloatColumns[sumKey] = []float64{42.0}
	countKey := v6IntKey("temp")
	data.IntColumns[countKey] = []int64{5}

	pointsMap := make(map[string]*AggregatedPoint)
	s.mergeV6ColumnarData(data, nil, pointsMap)

	if len(pointsMap) != 3 {
		t.Errorf("Expected 3 points, got %d", len(pointsMap))
	}

	// Only the first point should have field data
	for key, p := range pointsMap {
		if key == "1000_dev1" {
			if p.Fields["temp"] == nil {
				t.Error("First point should have temp field")
			}
		} else {
			if p.Fields["temp"] != nil {
				t.Errorf("Point %s should not have temp field (short array)", key)
			}
		}
	}
}

func TestMergeV6ColumnarData_IntOnlyField(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	// Only int columns, no float columns for a field → exercises "if !ok" in int merge
	data := NewColumnarData(1)
	data.Timestamps = []int64{1000}
	data.DeviceIDs = []string{"dev1"}
	countKey := v6IntKey("count_only_field")
	data.IntColumns[countKey] = []int64{42}

	pointsMap := make(map[string]*AggregatedPoint)
	s.mergeV6ColumnarData(data, nil, pointsMap)

	if len(pointsMap) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(pointsMap))
	}
	for _, p := range pointsMap {
		f, ok := p.Fields["count_only_field"]
		if !ok {
			t.Fatal("Expected count_only_field in fields")
		}
		if f.Count != 42 {
			t.Errorf("Count = %d, want 42", f.Count)
		}
	}
}

// =============================================================================
// DeleteOldData — error paths
// =============================================================================

func TestDeleteOldData_CorruptMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Create partition with corrupt metadata
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics",
		time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC))
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partDir, MetadataFile), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Should not crash — just continue past corrupt metadata
	err := s.DeleteOldData(AggregationHourly, "testdb", "metrics",
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("DeleteOldData should not return error: %v", err)
	}
}

func TestDeleteOldData_RemoveAllFailure(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Write real data with old timestamp
	oldTime := time.Date(2022, 1, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     oldTime,
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
			},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, oldTime)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Make partition directory unremovable
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", oldTime)
	parentDir := filepath.Dir(partDir)
	if err = os.Chmod(parentDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(parentDir, 0o755) }()

	// Should not crash — logs error but continues
	err = s.DeleteOldData(AggregationHourly, "testdb", "metrics",
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("DeleteOldData should not return error: %v", err)
	}
}

// =============================================================================
// CompactAll — error paths
// =============================================================================

func TestCompactAll_ListPartitionsError(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// CompactAll on level that has no data directories
	results, err := s.CompactAll(AggregationHourly, "nonexistent_db", "nonexistent_col")
	if err != nil {
		t.Logf("CompactAll error (expected): %v", err)
	}
	if results != nil {
		t.Logf("Results: %+v", results)
	}
}

func TestCompactAll_DoCompactError(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Write data then corrupt the DG metadata to make doCompact fail
	testTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
			},
		},
	}
	// Write same point 3 times to get 3 parts
	for i := 0; i < 3; i++ {
		err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, testTime)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Corrupt one of the part files in the DG directory
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", testTime)
	dgDir := filepath.Join(partDir, "dg_0000")
	// Corrupt the first part file
	files, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	if len(files) > 0 {
		if err := os.WriteFile(files[0], []byte("corrupt"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// CompactAll should log error but not crash
	results, err := s.CompactAll(AggregationHourly, "testdb", "metrics")
	t.Logf("CompactAll results: %v, err: %v", results, err)
}

// =============================================================================
// readPartitionData — additional error paths
// =============================================================================

func TestReadPartitionData_NilDGMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Write data then delete DG metadata file
	testTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
			},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, testTime)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Delete the DG metadata to trigger nil metadata path
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", testTime)
	dgDir := filepath.Join(partDir, "dg_0000")
	_ = os.Remove(filepath.Join(dgDir, "_dg_metadata.json")) // may not exist, that's OK

	// Read should still work but skip this DG
	pointsMap := make(map[string]*AggregatedPoint)
	readErr := s.readPartitionData(partDir, math.MinInt64, math.MaxInt64, nil, nil, nil, pointsMap)
	t.Logf("readPartitionData result: rowCount=%d, err=%v", len(pointsMap), readErr)
}

func TestReadPartitionData_EmptyPartData(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	testTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
			},
		},
	}
	err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, testTime)
	if err != nil {
		t.Fatalf("WriteAggregatedPoints failed: %v", err)
	}

	// Corrupt part files to make readV6PartFileData fail
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", testTime)
	dgDir := filepath.Join(partDir, "dg_0000")
	files, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	for _, f := range files {
		// Write valid header but corrupt data
		data := make([]byte, V6AggHeaderSize+20)
		binary.LittleEndian.PutUint32(data[0:], AggPartMagic)
		binary.LittleEndian.PutUint32(data[4:], AggPartVersion)
		if err := os.WriteFile(f, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	pointsMap := make(map[string]*AggregatedPoint)
	readErr := s.readPartitionData(partDir, math.MinInt64, math.MaxInt64, nil, nil, nil, pointsMap)
	t.Logf("readPartitionData with corrupt data: result=%d, err=%v", len(pointsMap), readErr)
}

// =============================================================================
// CompactWithOptions — doCompact error propagation
// =============================================================================

func TestCompactWithOptions_DoCompactError(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	testTime := time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC)
	points := []*AggregatedPoint{
		{
			Time:     testTime,
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
			},
		},
	}
	// Write 3 times to create multiple parts
	for i := 0; i < 3; i++ {
		err := s.WriteAggregatedPoints(AggregationHourly, "testdb", "metrics", points, testTime)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Corrupt a part file
	partDir := s.getPartitionDir(AggregationHourly, "testdb", "metrics", testTime)
	dgDir := filepath.Join(partDir, "dg_0000")
	files, _ := filepath.Glob(filepath.Join(dgDir, "part_*.bin"))
	if len(files) > 0 {
		if err := os.WriteFile(files[0], []byte("corrupt"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	err := s.CompactWithOptions(partDir, DefaultCompactOptions())
	if err != nil {
		t.Logf("CompactWithOptions error (expected): %v", err)
	}
}

// =============================================================================
// writeV6PartFile — error paths via read-only dir
// =============================================================================

func TestWriteV6PartFile_MkdirAllFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	data := NewColumnarData(1)
	data.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
	})

	// Use a path under a non-writable directory
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.MkdirAll(readOnlyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(readOnlyDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(readOnlyDir, 0o755) }()

	partDir := filepath.Join(readOnlyDir, "nested", "parts")
	_, err := s.writeV6PartFile(partDir, 0, AggregationHourly, data)
	if err == nil {
		t.Fatal("Expected error when MkdirAll fails")
	}
	t.Logf("Got expected error: %v", err)
}

func TestWriteV6PartFile_EmptyData_ReturnsNil(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	data := NewColumnarData(0)
	result, err := s.writeV6PartFile(tmpDir, 0, AggregationHourly, data)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for empty data")
	}
}

// =============================================================================
// writeMetadata / writeDGMetadata — additional error paths
// =============================================================================

func TestWriteMetadata_WriteFileFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Make the metadata directory read-only
	metaDir := filepath.Join(tmpDir, "metadir")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write initial metadata
	meta := &PartitionMetadata{
		Version: 6,
		Level:   "1h",
	}
	err := s.writeMetadata(metaDir, meta)
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Make directory read-only, then try to write again
	if err = os.Chmod(metaDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(metaDir, 0o755) }()

	err = s.writeMetadata(metaDir, meta)
	if err == nil {
		t.Fatal("Expected error when directory is read-only")
	}
	t.Logf("Got expected error: %v", err)
}

func TestWriteDGMetadata_WriteFileFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	dgDir := filepath.Join(tmpDir, "dgdir")
	if err := os.MkdirAll(dgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dgMeta := &DGMetadata{
		DirName: "dg_0000",
	}

	// Write initial
	err := s.writeDGMetadata(dgDir, dgMeta)
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Make read-only
	if err = os.Chmod(dgDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dgDir, 0o755) }()

	err = s.writeDGMetadata(dgDir, dgMeta)
	if err == nil {
		t.Fatal("Expected error when directory is read-only")
	}
	t.Logf("Got expected error: %v", err)
}

// =============================================================================
// readV6PartFileData — error paths
// =============================================================================

func TestReadV6PartFileData_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping as root")
	}
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Write a valid part file, then make it unreadable
	data := NewColumnarData(1)
	data.AddRowAllMetrics(1000, "dev1", map[string]*AggregatedField{
		"temp": {Sum: 10, Avg: 5, Min: 1, Max: 9, Count: 2},
	})

	pw, err := s.writeV6PartFile(tmpDir, 0, AggregationHourly, data)
	if err != nil {
		t.Fatalf("writeV6PartFile failed: %v", err)
	}
	if err := os.Rename(pw.tmpPath, pw.filePath); err != nil {
		t.Fatal(err)
	}

	// Read footer first while we can
	footer, err := s.readV6PartFooter(pw.filePath)
	if err != nil {
		t.Fatalf("readV6PartFooter failed: %v", err)
	}

	// Make unreadable
	if err = os.Chmod(pw.filePath, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(pw.filePath, 0o644) }()

	_, err = s.readV6PartFileData(pw.filePath, footer, nil, nil, nil, math.MinInt64, math.MaxInt64)
	if err == nil {
		t.Fatal("Expected error for unreadable file")
	}
}

// =============================================================================
// sliceColumnarData — partial float/int coverage
// =============================================================================

func TestSliceColumnarData_WithIntColumns(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	data := NewColumnarData(4)
	data.Timestamps = []int64{100, 200, 300, 400}
	data.DeviceIDs = []string{"d1", "d2", "d3", "d4"}
	sumKey := v6FloatKey(MetricSum, "temp")
	data.FloatColumns[sumKey] = []float64{1, 2, 3, 4}
	countKey := v6IntKey("temp")
	data.IntColumns[countKey] = []int64{10, 20, 30, 40}

	result := s.sliceColumnarData(data, 1, 3)
	if result.RowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowCount())
	}
	if result.Timestamps[0] != 200 || result.Timestamps[1] != 300 {
		t.Error("Wrong timestamps")
	}
	if vals, ok := result.FloatColumns[sumKey]; ok {
		if len(vals) != 2 || vals[0] != 2 || vals[1] != 3 {
			t.Error("Wrong float values")
		}
	} else {
		t.Error("Missing float column in slice")
	}
	if vals, ok := result.IntColumns[countKey]; ok {
		if len(vals) != 2 || vals[0] != 20 || vals[1] != 30 {
			t.Error("Wrong int values")
		}
	} else {
		t.Error("Missing int column in slice")
	}
}

func TestSliceColumnarData_FloatColumnShorterThanEnd(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	data := NewColumnarData(4)
	data.Timestamps = []int64{100, 200, 300, 400}
	data.DeviceIDs = []string{"d1", "d2", "d3", "d4"}
	sumKey := v6FloatKey(MetricSum, "temp")
	data.FloatColumns[sumKey] = []float64{1, 2} // Only 2 values but 4 rows
	countKey := v6IntKey("temp")
	data.IntColumns[countKey] = []int64{10, 20} // Only 2 values

	// Slice from 0 to 4 — float/int arrays should be clamped
	result := s.sliceColumnarData(data, 0, 4)
	if result.RowCount() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.RowCount())
	}
	if vals, ok := result.FloatColumns[sumKey]; ok {
		if len(vals) != 2 {
			t.Errorf("Expected 2 float values (clamped), got %d", len(vals))
		}
	}
	if vals, ok := result.IntColumns[countKey]; ok {
		if len(vals) != 2 {
			t.Errorf("Expected 2 int values (clamped), got %d", len(vals))
		}
	}
}

// =============================================================================
// EncodeDecodeFooter round-trip with edge cases
// =============================================================================

func TestEncodeDecodeFooter_EmptyDevicesAndFields(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	footer := &V6AggPartFooter{
		DeviceNames:    []string{},
		FieldNames:     []string{},
		Columns:        []V6AggColumnEntry{},
		RowCountPerDev: []uint32{},
	}

	encoded := s.encodeV6AggFooter(footer)
	decoded, err := s.decodeV6AggFooter(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if len(decoded.DeviceNames) != 0 {
		t.Errorf("Expected 0 devices, got %d", len(decoded.DeviceNames))
	}
	if len(decoded.FieldNames) != 0 {
		t.Errorf("Expected 0 fields, got %d", len(decoded.FieldNames))
	}
	if len(decoded.Columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(decoded.Columns))
	}
}

func TestEncodeDecodeFooter_ManyDevicesAndColumns(t *testing.T) {
	logger := logging.NewDevelopment()
	s := NewStorage(t.TempDir(), logger)

	devices := make([]string, 20)
	rowCounts := make([]uint32, 20)
	for i := range devices {
		devices[i] = "device_" + string(rune('A'+i))
		rowCounts[i] = uint32(i + 1)
	}

	columns := make([]V6AggColumnEntry, 50)
	for i := range columns {
		columns[i] = V6AggColumnEntry{
			DeviceIdx:  uint32(i % 20),
			FieldIdx:   uint32(i % 5),
			MetricIdx:  uint8(i % 4),
			Offset:     int64(i * 100),
			Size:       uint32(i * 10),
			RowCount:   uint32(i + 1),
			ColumnType: uint8(i % 2),
		}
	}

	footer := &V6AggPartFooter{
		DeviceNames:    devices,
		FieldNames:     []string{"_time", "temp", "humidity", "pressure", "wind"},
		Columns:        columns,
		RowCountPerDev: rowCounts,
	}

	encoded := s.encodeV6AggFooter(footer)
	decoded, err := s.decodeV6AggFooter(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if len(decoded.DeviceNames) != 20 {
		t.Errorf("Expected 20 devices, got %d", len(decoded.DeviceNames))
	}
	if len(decoded.Columns) != 50 {
		t.Errorf("Expected 50 columns, got %d", len(decoded.Columns))
	}

	// Verify a few column entries
	for i, col := range decoded.Columns {
		if col.DeviceIdx != uint32(i%20) {
			t.Errorf("Column %d DeviceIdx = %d, want %d", i, col.DeviceIdx, i%20)
			break
		}
	}
}

// =============================================================================
// commitPendingWrites — success with actual files
// =============================================================================

func TestCommitPendingWrites_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	// Create multiple pending writes
	var pending []*pendingPartWrite
	for i := 0; i < 3; i++ {
		data := NewColumnarData(1)
		data.AddRowAllMetrics(int64(i*1000), "dev1", map[string]*AggregatedField{
			"temp": {Sum: float64(i), Avg: float64(i), Min: 0, Max: float64(i), Count: 1},
		})
		partDir := filepath.Join(tmpDir, "parts")
		pw, err := s.writeV6PartFile(partDir, i, AggregationHourly, data)
		if err != nil {
			t.Fatalf("writeV6PartFile %d failed: %v", i, err)
		}
		pending = append(pending, pw)
	}

	err := s.commitPendingWrites(pending)
	if err != nil {
		t.Fatalf("commitPendingWrites failed: %v", err)
	}

	// Verify all files exist at their final paths
	for _, pw := range pending {
		if _, err := os.Stat(pw.filePath); err != nil {
			t.Errorf("Final file not found: %s", pw.filePath)
		}
		// Verify tmp file is gone
		if _, err := os.Stat(pw.tmpPath); !os.IsNotExist(err) {
			t.Errorf("Tmp file should be gone: %s", pw.tmpPath)
		}
	}
}

// =============================================================================
// WriteAggregatedPoints — compat layer coverage
// =============================================================================

func TestWriteHourly_ViaCompatLayer(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	points := []*AggregatedPoint{
		{
			Time:     time.Date(2024, 5, 15, 10, 0, 0, 0, time.UTC),
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 100, Avg: 10, Min: 5, Max: 15, Count: 10},
			},
		},
	}

	err := s.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	// Read back to verify
	readPoints, err := s.ReadAggregatedPoints(AggregationHourly, "testdb", "metrics",
		ReadOptions{
			StartTime: time.Date(2024, 5, 15, 9, 0, 0, 0, time.UTC),
			EndTime:   time.Date(2024, 5, 15, 11, 0, 0, 0, time.UTC),
		})
	if err != nil {
		t.Fatalf("ReadAggregatedPoints failed: %v", err)
	}
	if len(readPoints) != 1 {
		t.Errorf("Expected 1 point, got %d", len(readPoints))
	}
}

func TestWriteDaily_ViaCompatLayer(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	points := []*AggregatedPoint{
		{
			Time:     time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 100, Avg: 10, Min: 5, Max: 15, Count: 10},
			},
		},
	}

	err := s.WriteDaily("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteDaily failed: %v", err)
	}
}

func TestWriteMonthly_ViaCompatLayer(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewDevelopment()
	s := NewStorage(tmpDir, logger)

	points := []*AggregatedPoint{
		{
			Time:     time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC),
			DeviceID: "dev1",
			Fields: map[string]*AggregatedField{
				"temp": {Sum: 100, Avg: 10, Min: 5, Max: 15, Count: 10},
			},
		},
	}

	err := s.WriteMonthly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteMonthly failed: %v", err)
	}
}
