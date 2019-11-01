// +build integration

package migrate

import (
	"testing"
	"reflect"
)

func TestGlobalMigrateUp(t *testing.T) {
	defer cleanup(db)
	SetDatabase(db)

	if err := Up(-1); err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(globalMigrate.migratedVersions, []uint64{1, 2}) {
		t.Errorf("Unexpected migrated versions %v", globalMigrate.migratedVersions)
		return
	}
}

func TestGlobalMigrateDown(t *testing.T) {
	defer cleanup(db)
	SetDatabase(db)

	if err := Up(-1); err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	if !reflect.DeepEqual(globalMigrate.migratedVersions, []uint64{1, 2}) {
		t.Errorf("Unexpected migrated versions after migrate %v", globalMigrate.migratedVersions)
		return
	}

	if err := Down(-1); err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	if !reflect.DeepEqual(globalMigrate.migratedVersions, []uint64{}) {
		t.Errorf("Unexpected migrated versions after rollback %v", globalMigrate.migratedVersions)
		return
	}
}
