package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

// ShardScanner scans local data directory to discover shards
type ShardScanner struct {
	dataDir string
	logger  *logging.Logger
}

// NewShardScanner creates a new shard scanner
func NewShardScanner(dataDir string, logger *logging.Logger) *ShardScanner {
	return &ShardScanner{
		dataDir: dataDir,
		logger:  logger,
	}
}

// ScanShards scans the data directory and returns list of shards
// Expected directory structure: /data/{database}/{collection}/shard-{id}/
func (s *ShardScanner) ScanShards() ([]models.ShardInfo, error) {
	var shards []models.ShardInfo

	// Check if data directory exists
	if _, err := os.Stat(s.dataDir); os.IsNotExist(err) {
		s.logger.Info("Data directory does not exist, creating", "data_dir", s.dataDir)
		if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}
		return shards, nil // Empty directory, no shards yet
	}

	// Walk through data directory
	err := filepath.Walk(s.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			s.logger.Warn("Error accessing path", "path", path, "error", err)
			return nil // Continue scanning
		}

		// Skip non-directories
		if !info.IsDir() {
			return nil
		}

		// Check if this is a shard directory (format: shard-{id})
		if strings.HasPrefix(info.Name(), "shard-") {
			shard, err := s.parseShardDirectory(path)
			if err != nil {
				s.logger.Warn("Failed to parse shard directory", "path", path, "error", err)
				return nil // Continue scanning
			}

			shards = append(shards, *shard)
			s.logger.Debug("Discovered shard",
				"database", shard.Database,
				"collection", shard.Collection,
				"shard_id", shard.ShardID,
				"size", shard.DataSize,
			)

		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan data directory: %w", err)
	}

	s.logger.Info("Shard scan completed", "count", len(shards))

	return shards, nil
}

// parseShardDirectory parses a shard directory path and returns ShardInfo
// Expected path: /data/{database}/{collection}/shard-{id}/
func (s *ShardScanner) parseShardDirectory(path string) (*models.ShardInfo, error) {
	// Get relative path from data directory
	relPath, err := filepath.Rel(s.dataDir, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get relative path: %w", err)
	}

	// Split path components
	parts := strings.Split(relPath, string(os.PathSeparator))
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid shard path structure: %s", relPath)
	}

	database := parts[0]
	collection := parts[1]
	shardName := parts[2]

	// Extract shard ID from "shard-{id}"
	shardIDStr := strings.TrimPrefix(shardName, "shard-")
	shardID, err := strconv.ParseUint(shardIDStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid shard ID: %s", shardIDStr)
	}

	// Calculate shard data size
	dataSize, err := s.calculateDirectorySize(path)
	if err != nil {
		s.logger.Warn("Failed to calculate directory size", "path", path, "error", err)
		dataSize = 0
	}

	// Determine role (for now, default to "primary", can be enhanced later)
	role := "primary"

	return &models.ShardInfo{
		ShardID:    uint32(shardID),
		Database:   database,
		Collection: collection,
		Role:       role,
		DataSize:   dataSize,
	}, nil
}

// calculateDirectorySize calculates total size of all files in a directory
func (s *ShardScanner) calculateDirectorySize(path string) (int64, error) {
	var totalSize int64

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize, err
}

// GetDiskCapacity returns disk capacity information for the data directory
func (s *ShardScanner) GetDiskCapacity() (*models.Capacity, error) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(s.dataDir, &stat)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk stats: %w", err)
	}

	// Calculate disk space (in bytes)
	diskTotal := stat.Blocks * uint64(stat.Bsize)
	diskAvailable := stat.Bavail * uint64(stat.Bsize)
	diskUsed := diskTotal - diskAvailable

	return &models.Capacity{
		DiskTotal:     int64(diskTotal),
		DiskUsed:      int64(diskUsed),
		DiskAvailable: int64(diskAvailable),
	}, nil
}

// GroupScanResult holds information about a discovered group on disk
type GroupScanResult struct {
	GroupID  int
	DataSize int64
}

// ScanGroups scans the data directory for group_XXXX directories
// and returns the list of group IDs found on disk.
// Expected directory structure: data/data/group_XXXX/
func (s *ShardScanner) ScanGroups() ([]GroupScanResult, error) {
	var results []GroupScanResult

	// Groups live under data/data/ subdirectory
	dataDir := filepath.Join(s.dataDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		s.logger.Debug("Data subdirectory does not exist", "dir", dataDir)
		return results, nil
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "group_") {
			continue
		}

		groupID, err := parseGroupDirName(entry.Name())
		if err != nil {
			s.logger.Warn("Invalid group directory name", "name", entry.Name(), "error", err)
			continue
		}

		groupDir := filepath.Join(dataDir, entry.Name())
		dataSize, err := s.calculateDirectorySize(groupDir)
		if err != nil {
			s.logger.Warn("Failed to calculate group size", "group_id", groupID, "error", err)
			dataSize = 0
		}

		results = append(results, GroupScanResult{
			GroupID:  groupID,
			DataSize: dataSize,
		})

		s.logger.Debug("Discovered group on disk",
			"group_id", groupID,
			"data_size", dataSize)
	}

	s.logger.Info("Group scan completed", "count", len(results))
	return results, nil
}

// parseGroupDirName extracts group ID from "group_XXXX" directory name
func parseGroupDirName(name string) (int, error) {
	var groupID int
	_, err := fmt.Sscanf(name, "group_%d", &groupID)
	if err != nil {
		return 0, fmt.Errorf("invalid group dir name %q: %w", name, err)
	}
	return groupID, nil
}
