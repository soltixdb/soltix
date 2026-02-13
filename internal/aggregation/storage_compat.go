package aggregation

import (
	"time"
)

// =============================================================================
// Legacy API compatibility layer
// These methods provide compatibility with the old Storage interface
// =============================================================================

// WriteHourly writes hourly aggregates (file per day)
// Compatible with old API: WriteHourly(database, collection string, points []*AggregatedPoint) error
func (s *Storage) WriteHourly(database, collection string, points []*AggregatedPoint) error {
	if len(points) == 0 {
		return nil
	}

	// Group by date in configured timezone
	byDate := make(map[time.Time][]*AggregatedPoint)
	for _, point := range points {
		ts := point.Time.In(s.timezone)
		dayStart := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, s.timezone)
		byDate[dayStart] = append(byDate[dayStart], point)
	}

	// Write each date separately
	for dayStart, datePoints := range byDate {
		if err := s.WriteAggregatedPoints(AggregationHourly, database, collection, datePoints, dayStart); err != nil {
			return err
		}
	}

	return nil
}

// WriteDaily writes daily aggregates (file per month)
// Compatible with old API
func (s *Storage) WriteDaily(database, collection string, points []*AggregatedPoint) error {
	if len(points) == 0 {
		return nil
	}

	// Group by month in configured timezone
	byMonth := make(map[time.Time][]*AggregatedPoint)
	for _, point := range points {
		ts := point.Time.In(s.timezone)
		monthStart := TruncateToMonth(ts)
		byMonth[monthStart] = append(byMonth[monthStart], point)
	}

	// Write each month
	for monthStart, monthPoints := range byMonth {
		if err := s.WriteAggregatedPoints(AggregationDaily, database, collection, monthPoints, monthStart); err != nil {
			return err
		}
	}

	return nil
}

// WriteMonthly writes monthly aggregates (file per year)
// Compatible with old API
func (s *Storage) WriteMonthly(database, collection string, points []*AggregatedPoint) error {
	if len(points) == 0 {
		return nil
	}

	// Group by year in configured timezone
	byYear := make(map[time.Time][]*AggregatedPoint)
	for _, point := range points {
		ts := point.Time.In(s.timezone)
		yearStart := TruncateToYear(ts)
		byYear[yearStart] = append(byYear[yearStart], point)
	}

	// Write each year
	for yearStart, yearPoints := range byYear {
		if err := s.WriteAggregatedPoints(AggregationMonthly, database, collection, yearPoints, yearStart); err != nil {
			return err
		}
	}

	return nil
}

// WriteYearly writes yearly aggregates (single file)
// Compatible with old API
func (s *Storage) WriteYearly(database, collection string, points []*AggregatedPoint) error {
	if len(points) == 0 {
		return nil
	}

	// Write all points together
	return s.WriteAggregatedPoints(AggregationYearly, database, collection, points, time.Now())
}

// ReadHourly reads hourly aggregates for a specific date
// date format: YYYYMMDD
func (s *Storage) ReadHourly(database, collection, date string) ([]*AggregatedPoint, error) {
	// Parse date
	t, err := time.ParseInLocation("20060102", date, s.timezone)
	if err != nil {
		return nil, err
	}

	return s.ReadAggregatedPoints(AggregationHourly, database, collection, ReadOptions{
		StartTime: t,
		EndTime:   t.Add(24 * time.Hour),
	})
}

// ReadHourlyForDay reads all hourly aggregates for a specific day
func (s *Storage) ReadHourlyForDay(database, collection string, day time.Time) ([]*AggregatedPoint, error) {
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, s.timezone)
	dayEnd := dayStart.Add(24 * time.Hour)

	return s.ReadAggregatedPoints(AggregationHourly, database, collection, ReadOptions{
		StartTime: dayStart,
		EndTime:   dayEnd,
	})
}

// ReadDailyForMonth reads all daily aggregates for a specific month
func (s *Storage) ReadDailyForMonth(database, collection string, month time.Time) ([]*AggregatedPoint, error) {
	monthStart := TruncateToMonth(month.In(s.timezone))
	monthEnd := monthStart.AddDate(0, 1, 0)

	return s.ReadAggregatedPoints(AggregationDaily, database, collection, ReadOptions{
		StartTime: monthStart,
		EndTime:   monthEnd,
	})
}

// ReadMonthlyForYear reads all monthly aggregates for a specific year
func (s *Storage) ReadMonthlyForYear(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
	yearStart := TruncateToYear(year.In(s.timezone))
	yearEnd := yearStart.AddDate(1, 0, 0)

	return s.ReadAggregatedPoints(AggregationMonthly, database, collection, ReadOptions{
		StartTime: yearStart,
		EndTime:   yearEnd,
	})
}

// ReadYearly reads yearly aggregates
func (s *Storage) ReadYearly(database, collection string) ([]*AggregatedPoint, error) {
	return s.ReadAggregatedPoints(AggregationYearly, database, collection, ReadOptions{
		StartTime: time.Date(1970, 1, 1, 0, 0, 0, 0, s.timezone),
		EndTime:   time.Date(2100, 1, 1, 0, 0, 0, 0, s.timezone),
	})
}
