// Package migrate allows to perform versioned migrations in your MongoDB.
package migrate

import (
	"context"
	"time"
	"fmt"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type collectionSpecification struct {
	Name string `bson:"name"`
	Type string `bson:"type"`
}

type versionRecord struct {
	Version     uint64    `bson:"version"`
	Description string    `bson:"description,omitempty"`
	Timestamp   time.Time `bson:"timestamp"`
}

const defaultMigrationsCollection = "migrations"

// AllAvailable used in "Up" or "Down" methods to run all available migrations.
const AllAvailable = -1

// Migrate is type for performing migrations in provided database.
// Information about which migrations have been applied is being stored in a dedicated collection.
// Each migration applying ("up") adds new document to the collection.
// This document consists of migration version, migration description and timestamp.
// Each migration rollback ("down") removes a corresponding document from the collection.
//
// db                         - database connection
// migrations                 - list of all registered migrations
// migrationsCollection       - name of a collection where migrations are stored
// migratedVersions           - list of migration versions stored in the database
// isLoadedMigratedVersions   - whether loading of migratedVersions has already been done
//
type Migrate struct {
	db                       *mongo.Database
	migrations               []Migration
	migrationsCollection     string
	migratedVersions         []uint64
	isLoadedMigratedVersions bool
}

func NewMigrate(db *mongo.Database, migrations ...Migration) *Migrate {
	internalMigrations := make([]Migration, len(migrations))
	copy(internalMigrations, migrations)
	return &Migrate{
		db:                       db,
		migrations:               internalMigrations,
		migrationsCollection:     defaultMigrationsCollection,
		migratedVersions:         make([]uint64, 0),
		isLoadedMigratedVersions: false,
	}
}

// SetMigrationsCollection replaces name of collection for storing migration information.
// By default it is "migrations".
func (m *Migrate) SetMigrationsCollection(name string) {
	m.migrationsCollection = name
}

func (m *Migrate) isCollectionExist(name string) (isExist bool, err error) {
	collections, err := m.getCollections()
	if err != nil {
		return false, err
	}

	for _, c := range collections {
		if name == c.Name {
			return true, nil
		}
	}
	return false, nil
}

func (m *Migrate) createCollectionIfNotExist(name string) error {
	exist, err := m.isCollectionExist(name)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	ctx := context.Background()

	// create collection and unique index by version in a transaction
	err = m.db.Client().UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err := sessionContext.StartTransaction(); err != nil {
			return err
		}

		command := bson.D{bson.E{Key: "create", Value: name}}
		err = m.db.RunCommand(nil, command).Err()

		if err == nil {
			_, err = m.db.Collection(name).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys: bson.D{{"version", 1}},
				Options: options.Index().SetName(fmt.Sprintf("index-%s-on-version", name)),
			})
		}

		if err != nil {
			sessionContext.AbortTransaction(sessionContext)
			return err
		} else {
			sessionContext.CommitTransaction(sessionContext)
		}
		return nil
	})

	return err
}

func (m *Migrate) getCollections() (collections []collectionSpecification, err error) {
	filter := bson.D{bson.E{Key: "type", Value: "collection"}}
	options := options.ListCollections().SetNameOnly(true)

	cursor, err := m.db.ListCollections(context.Background(), filter, options)
	if err != nil {
		return nil, err
	}

	if cursor != nil {
		defer func(cursor *mongo.Cursor) {
			curErr := cursor.Close(context.TODO())
			if curErr != nil {
				if err != nil {
					err = errors.Wrapf(curErr, "migrate: get collection failed: %s", err.Error())
				} else {
					err = curErr
				}
			}
		}(cursor)
	}

	for cursor.Next(context.TODO()) {
		var collection collectionSpecification

		err := cursor.Decode(&collection)
		if err != nil {
			return nil, err
		}

		collections = append(collections, collection)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return
}

// loadMigratedVersions returns array of migration versions stored in the database.
func (m *Migrate) loadMigratedVersions() ([]uint64, error) {
	if err := m.createCollectionIfNotExist(m.migrationsCollection); err != nil {
		return nil, err
	}

	filter := bson.D{}
	sort := bson.D{bson.E{Key: "version", Value: 1}}
	projection := bson.D{{"version", 1}}
	options := options.Find().SetSort(sort).SetProjection(projection)

	cur, err := m.db.Collection(m.migrationsCollection).Find(context.TODO(), filter, options)
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())

	versionRecords := make([]versionRecord, 0)

	for cur.Next(context.Background()) {
		var versionRecord versionRecord

		if err := cur.Decode(&versionRecord); err != nil {
			return nil, err
		}

		versionRecords = append(versionRecords, versionRecord)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	versions := make([]uint64, len(versionRecords))
	for i, vr := range versionRecords {
		versions[i] = vr.Version
	}

	copy(m.migratedVersions, versions)
	m.isLoadedMigratedVersions = true

	return versions, nil
}

// addMigratedVersion adds version to migratedVersions prop of Migrate object.
func (m *Migrate) addMigratedVersion(version uint64) {
	m.migratedVersions = append(m.migratedVersions, version)
}

// removeMigratedVersion removes last version from migratedVersions prop of Migrate object.
func (m *Migrate) removeMigratedVersion(version uint64) {
	m.migratedVersions[len(m.migratedVersions)-1] = 0
	m.migratedVersions = m.migratedVersions[:len(m.migratedVersions)-1]
}

// insertVersion inserts version record to migrations collection
func (m *Migrate) insertVersion(version uint64, description string) error {
	rec := versionRecord{
		Version:     version,
		Timestamp:   time.Now().UTC(),
		Description: description,
	}

	_, err := m.db.Collection(m.migrationsCollection).InsertOne(context.TODO(), rec)

	return err
}

// deleteVersion deletes version record from migrations collection
func (m *Migrate) deleteVersion(version uint64) error {
	query := bson.M{ "version": version }
	_, err := m.db.Collection(m.migrationsCollection).DeleteOne(context.TODO(), query)

	return err
}

// isMigratedVersion returns true if migration with specified version is migrated and false otherwise
func (m *Migrate) isMigratedVersion(version uint64) bool {
	for _, v := range m.migratedVersions {
		if v == version {
			return true
		}
	}
	return false
}

// IsMigrated returns true if all migrations have been migrated, false otherwise.
func (m *Migrate) IsMigrated() (bool, error) {
	if !m.isLoadedMigratedVersions {
		if _, err := m.loadMigratedVersions(); err != nil {
			return false, err
		}
	}

	for _, migration := range m.migrations {
		if !m.isMigratedVersion(migration.Version) {
			return false, nil
		}
	}
	return true, nil
}

// Up performs "up" migrations to latest available version.
// If n<=0 all "up" migrations with newer versions will be performed.
// If n>0 only n migrations with newer version will be performed.
func (m *Migrate) Up(n int) error {
	if !m.isLoadedMigratedVersions {
		if _, err := m.loadMigratedVersions(); err != nil {
			return err
		}
	}
	if n <= 0 || n > len(m.migrations) {
		n = len(m.migrations)
	}
	migrationSort(m.migrations)

	for i, p := 0, 0; i < len(m.migrations) && p < n; i++ {
		migration := m.migrations[i]
		if m.isMigratedVersion(migration.Version) || migration.Up == nil {
			continue
		}
		p++
		if err := migration.Up(m.db); err != nil {
			return err
		}
		if err := m.insertVersion(migration.Version, migration.Description); err != nil {
			return err
		}
		m.addMigratedVersion(migration.Version)
	}
	return nil
}

// Down performs "down" migration to oldest available version.
// If n<=0 all "down" migrations with older version will be performed.
// If n>0 only n migrations with older version will be performed.
func (m *Migrate) Down(n int) error {
	if !m.isLoadedMigratedVersions {
		if _, err := m.loadMigratedVersions(); err != nil {
			return err
		}
	}
	if n <= 0 || n > len(m.migrations) {
		n = len(m.migrations)
	}
	migrationSort(m.migrations)

	for i, p := len(m.migrations)-1, 0; i >= 0 && p < n; i-- {
		migration := m.migrations[i]
		if !m.isMigratedVersion(migration.Version) || migration.Down == nil {
			continue
		}
		p++
		if err := migration.Down(m.db); err != nil {
			return err
		}

		if err := m.deleteVersion(migration.Version); err != nil {
			return err
		}
		m.removeMigratedVersion(migration.Version)
	}
	return nil
}
