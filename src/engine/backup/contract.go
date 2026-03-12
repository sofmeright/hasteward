package backup

import (
	"context"
	"time"

	"github.com/PrPlanIT/HASteward/src/output/model"
)

// Backer is the engine-specific hook contract for backup operations.
type Backer interface {
	Name() string
	// Backup performs the default backup for this engine.
	Backup(ctx context.Context) (*model.BackupResult, error)
	// BackupDump streams a SQL dump through restic backup.
	// Used by both normal backup and repair escrow.
	BackupDump(ctx context.Context, backupType, donor, stdinFilename string, jobTime time.Time, extraTags map[string]string) (*model.BackupResult, error)
}
