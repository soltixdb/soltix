package aggregation

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkAggregatedFieldAddValue benchmarks adding values to aggregated field
func BenchmarkAggregatedFieldAddValue(b *testing.B) {
	af := NewAggregatedField(10.0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		af.AddValue(float64(i) * 0.1)
	}
}

// BenchmarkAggregatedFieldMerge benchmarks merging aggregated fields
func BenchmarkAggregatedFieldMerge(b *testing.B) {
	af1 := NewAggregatedField(10.0)
	for i := 0; i < 100; i++ {
		af1.AddValue(float64(i) * 0.1)
	}

	af2 := NewAggregatedField(20.0)
	for i := 0; i < 100; i++ {
		af2.AddValue(float64(i) * 0.2)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a copy to avoid modifying the original
		b.StopTimer()
		afCopy := &AggregatedField{
			Count:      af1.Count,
			Sum:        af1.Sum,
			Avg:        af1.Avg,
			Min:        af1.Min,
			Max:        af1.Max,
			SumSquares: af1.SumSquares,
		}
		b.StartTimer()

		afCopy.Merge(af2)
	}
}

// BenchmarkAggregateRawDataPoints benchmarks aggregating raw data points
func BenchmarkAggregateRawDataPoints(b *testing.B) {
	baseTime := time.Now()
	points := make([]*RawDataPoint, 1000)
	for i := 0; i < 1000; i++ {
		points[i] = &RawDataPoint{
			Time: baseTime.Add(time.Duration(i) * time.Second),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
				"humidity":    float64(i) * 0.2,
			},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAggregationLevels benchmarks different aggregation levels
func BenchmarkAggregationLevels(b *testing.B) {
	baseTime := time.Now()
	points := make([]*RawDataPoint, 1000)
	for i := 0; i < 1000; i++ {
		points[i] = &RawDataPoint{
			Time: baseTime.Add(time.Duration(i) * time.Minute),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.1,
			},
		}
	}

	levels := []AggregationLevel{AggregationHourly, AggregationDaily, AggregationMonthly, AggregationYearly}

	for _, level := range levels {
		b.Run(string(level), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := AggregateRawDataPoints("device-001", points, level)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAggregateAggregatedPoints benchmarks cascading aggregations
func BenchmarkAggregateAggregatedPoints(b *testing.B) {
	baseTime := time.Now()

	// Create hourly aggregated points
	points := make([]*AggregatedPoint, 24)
	for i := 0; i < 24; i++ {
		points[i] = &AggregatedPoint{
			Time:     baseTime.Add(time.Duration(i) * time.Hour),
			DeviceID: "device-001",
			Level:    AggregationHourly,
			Fields: map[string]*AggregatedField{
				"temperature": {
					Count: 60,
					Sum:   1200.0 + float64(i)*10,
					Avg:   20.0 + float64(i)*0.1,
					Min:   18.0,
					Max:   25.0,
				},
			},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := AggregateAggregatedPoints("device-001", points, AggregationDaily)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMultipleDevices benchmarks aggregating multiple devices
func BenchmarkMultipleDevices(b *testing.B) {
	baseTime := time.Now()
	deviceCount := 100
	pointsPerDevice := 100

	// Create points for all devices
	allDevicePoints := make(map[string][]*RawDataPoint)
	for d := 0; d < deviceCount; d++ {
		deviceID := fmt.Sprintf("device-%d", d)
		points := make([]*RawDataPoint, pointsPerDevice)
		for i := 0; i < pointsPerDevice; i++ {
			points[i] = &RawDataPoint{
				Time: baseTime.Add(time.Duration(i) * time.Second),
				Fields: map[string]interface{}{
					"temperature": float64(i) * 0.1,
				},
			}
		}
		allDevicePoints[deviceID] = points
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for deviceID, points := range allDevicePoints {
			_, err := AggregateRawDataPoints(deviceID, points, AggregationHourly)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkAggregationWithMultipleFields benchmarks aggregating multiple fields
func BenchmarkAggregationWithMultipleFields(b *testing.B) {
	baseTime := time.Now()
	points := make([]*RawDataPoint, 1000)
	fields := []string{"temperature", "humidity", "voltage", "power", "frequency"}

	for i := 0; i < 1000; i++ {
		fieldsData := make(map[string]interface{})
		for j, fieldName := range fields {
			fieldsData[fieldName] = float64(i)*0.1 + float64(j)
		}
		points[i] = &RawDataPoint{
			Time:   baseTime.Add(time.Duration(i) * time.Second),
			Fields: fieldsData,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLargeDataPointAggregation benchmarks aggregating large batches of data points
func BenchmarkLargeDataPointAggregation(b *testing.B) {
	baseTime := time.Now()
	pointCounts := []int{5000, 10000}

	for _, count := range pointCounts {
		b.Run(fmt.Sprintf("Points-%d", count), func(b *testing.B) {
			points := make([]*RawDataPoint, count)
			for i := 0; i < count; i++ {
				points[i] = &RawDataPoint{
					Time: baseTime.Add(time.Duration(i) * time.Second),
					Fields: map[string]interface{}{
						"temperature": float64(i) * 0.1,
						"humidity":    float64(i) * 0.2,
					},
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCascadingAggregations benchmarks cascading aggregations through multiple levels
func BenchmarkCascadingAggregations(b *testing.B) {
	baseTime := time.Now()

	// Level 1: Raw data points
	rawPoints := make([]*RawDataPoint, 3600)
	for i := 0; i < 3600; i++ {
		rawPoints[i] = &RawDataPoint{
			Time: baseTime.Add(time.Duration(i) * time.Second),
			Fields: map[string]interface{}{
				"temperature": float64(i) * 0.01,
			},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Aggregate to hourly
		hourly, err := AggregateRawDataPoints("device-001", rawPoints, AggregationHourly)
		if err != nil {
			b.Fatal(err)
		}

		// Aggregate to daily
		if hourly != nil {
			_, err = AggregateAggregatedPoints("device-001", []*AggregatedPoint{hourly}, AggregationDaily)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkAggregationParallel benchmarks parallel aggregation of multiple devices
func BenchmarkAggregationParallel(b *testing.B) {
	baseTime := time.Now()

	// Prepare data for 100 devices
	devicePoints := make(map[string][]*RawDataPoint)
	for d := 0; d < 100; d++ {
		deviceID := fmt.Sprintf("device-%d", d)
		points := make([]*RawDataPoint, 500)
		for i := 0; i < 500; i++ {
			points[i] = &RawDataPoint{
				Time: baseTime.Add(time.Duration(i) * time.Second),
				Fields: map[string]interface{}{
					"temperature": float64(i)*0.1 + float64(d),
					"humidity":    float64(i)*0.2 - float64(d),
				},
			}
		}
		devicePoints[deviceID] = points
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			deviceID := fmt.Sprintf("device-%d", counter%100)
			if points, exists := devicePoints[deviceID]; exists {
				_, _ = AggregateRawDataPoints(deviceID, points, AggregationHourly)
			}
			counter++
		}
	})
}

// BenchmarkAggregatedFieldOperations benchmarks various aggregated field operations
func BenchmarkAggregatedFieldOperations(b *testing.B) {
	b.Run("AddValue", func(b *testing.B) {
		af := NewAggregatedField(10.0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			af.AddValue(float64(i) * 0.1)
		}
	})

	b.Run("Merge-Small", func(b *testing.B) {
		af1 := NewAggregatedField(10.0)
		af2 := NewAggregatedField(20.0)
		for i := 0; i < 10; i++ {
			af1.AddValue(float64(i))
			af2.AddValue(float64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			afCopy := &AggregatedField{
				Count:      af1.Count,
				Sum:        af1.Sum,
				Avg:        af1.Avg,
				Min:        af1.Min,
				Max:        af1.Max,
				SumSquares: af1.SumSquares,
			}
			b.StartTimer()
			afCopy.Merge(af2)
		}
	})

	b.Run("Merge-Large", func(b *testing.B) {
		af1 := NewAggregatedField(0.0)
		af2 := NewAggregatedField(0.0)
		for i := 0; i < 1000; i++ {
			af1.AddValue(float64(i) * 0.1)
			af2.AddValue(float64(i) * 0.2)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			afCopy := &AggregatedField{
				Count:      af1.Count,
				Sum:        af1.Sum,
				Avg:        af1.Avg,
				Min:        af1.Min,
				Max:        af1.Max,
				SumSquares: af1.SumSquares,
			}
			b.StartTimer()
			afCopy.Merge(af2)
		}
	})
}

// BenchmarkPointDistribution benchmarks aggregation with different data distributions
func BenchmarkPointDistribution(b *testing.B) {
	distributions := map[string]func(int) float64{
		"linear":      func(i int) float64 { return float64(i) * 0.1 },
		"exponential": func(i int) float64 { return float64(i) * float64(i) * 0.0001 },
		"sinusoidal": func(i int) float64 {
			return 20.0 + 5.0*float64(i%100)/100.0 // Simulating oscillating data
		},
	}

	for name, distribution := range distributions {
		b.Run(name, func(b *testing.B) {
			baseTime := time.Now()
			points := make([]*RawDataPoint, 1000)

			for i := 0; i < 1000; i++ {
				points[i] = &RawDataPoint{
					Time: baseTime.Add(time.Duration(i) * time.Second),
					Fields: map[string]interface{}{
						"temperature": distribution(i),
					},
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := AggregateRawDataPoints("device-001", points, AggregationHourly)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
