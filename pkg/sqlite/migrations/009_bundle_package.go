package migrations

import (
	"context"
	"database/sql"
	"fmt"
)

const BundlePackageMigrationKey = 9

// Register this migration
func init() {
	registerMigration(BundlePackageMigrationKey, bundlePackageMigration)
}

var bundlePackageMigration = &Migration{
	Id:         BundlePackageMigrationKey,
	DisableFks: true,
	Up: func(ctx context.Context, tx *sql.Tx) error {
		fmt.Println("hmm?")
		/*
			foreignKeyOff := `PRAGMA foreign_keys = OFF`
			_, err := tx.ExecContext(ctx, foreignKeyOff)
			if err != nil {
				return err
			}
		*/

		createNewOperatorBundleTable := `
		CREATE TABLE operatorbundle_new (
			name TEXT PRIMARY KEY,
			package_name TEXT,
			csv	TEXT,
			bundle TEXT,
			bundlepath TEXT,
			skiprange TEXT,
			version	TEXT,
			replaces TEXT,
			skips TEXT,
			FOREIGN KEY(package_name) REFERENCES package(name) ON DELETE CASCADE
		);
		`
		_, err := tx.ExecContext(ctx, createNewOperatorBundleTable)
		if err != nil {
			return err
		}

		fmt.Println("1")

		insertOperatorBundle := `INSERT INTO operatorbundle_new(name, csv, bundle, bundlepath, skiprange, version, replaces, skips) SELECT name, csv, bundle, bundlepath, skiprange, version, replaces, skips FROM operatorbundle;`
		_, err = tx.ExecContext(ctx, insertOperatorBundle)
		if err != nil {
			return err
		}

		bundlePkgs, err := getBundlePackages(ctx, tx)
		if err != nil {
			return err
		}

		for bundle, pkgName := range bundlePkgs {
			err := setPackage(ctx, tx, pkgName, bundle)
			if err != nil {
				return err
			}
		}

		renameNewAndDropOld := `
		DROP TABLE operatorbundle;
		ALTER TABLE operatorbundle_new RENAME TO operatorbundle;
		`
		_, err = tx.ExecContext(ctx, renameNewAndDropOld)
		if err != nil {
			return err
		}

		addIndex := `
		CREATE UNIQUE INDEX pk ON operatorbundle(name, version, bundlepath);
		CREATE UNIQUE INDEX pkg ON operatorbundle(name, package_name);
		`
		_, err = tx.ExecContext(ctx, addIndex)
		if err != nil {
			return err
		}

		createNewPackageTable := `CREATE TABLE IF NOT EXISTS package_new (
			name TEXT PRIMARY KEY,
			default_channel TEXT
		);`
		_, err = tx.ExecContext(ctx, createNewPackageTable)
		if err != nil {
			return err
		}

		insertPackage := `INSERT INTO package_new(name, default_channel) SELECT name, default_channel FROM package;`
		_, err = tx.ExecContext(ctx, insertPackage)
		if err != nil {
			return err
		}

		renameNewAndDropOld = `
		DROP TABLE package;
		ALTER TABLE package_new RENAME TO package;
		`
		_, err = tx.ExecContext(ctx, renameNewAndDropOld)
		if err != nil {
			return err
		}

		createNewChannelTable := `
		CREATE TABLE channel_new (
			name	TEXT,
			package_name	TEXT,
			head_operatorbundle_name	TEXT,
			PRIMARY KEY(name, package_name),
			FOREIGN KEY(package_name) REFERENCES package(name) ON DELETE CASCADE,
			FOREIGN KEY(head_operatorbundle_name, package_name) REFERENCES operatorbundle(name, package_name) DEFERRABLE INITIALLY DEFERRED
		);`
		_, err = tx.ExecContext(ctx, createNewChannelTable)
		if err != nil {
			return err
		}

		insertChannel := `INSERT INTO channel_new(name, package_name, head_operatorbundle_name) SELECT name, package_name, head_operatorbundle_name FROM channel;`
		_, err = tx.ExecContext(ctx, insertChannel)
		if err != nil {
			return err
		}

		renameNewAndDropOld = `
		DROP TABLE channel;
		ALTER TABLE channel_new RENAME TO channel;
		`
		_, err = tx.ExecContext(ctx, renameNewAndDropOld)
		if err != nil {
			return err
		}

		createNewChannelEntryTable := `
		CREATE TABLE channel_entry_new (
			entry_id INTEGER PRIMARY KEY,
			channel_name TEXT,
			operatorbundle_name TEXT,
			replaces INTEGER,
			depth INTEGER,
			package_name TEXT,
			FOREIGN KEY(replaces) REFERENCES channel_entry(entry_id) DEFERRABLE INITIALLY DEFERRED, 
			FOREIGN KEY(channel_name, package_name) REFERENCES channel(name, package_name) ON DELETE CASCADE,
			FOREIGN KEY(operatorbundle_name, package_name) REFERENCES operatorbundle(name, package_name) DEFERRABLE INITIALLY DEFERRED
		);`
		_, err = tx.ExecContext(ctx, createNewChannelEntryTable)
		if err != nil {
			return err
		}

		fmt.Println("Got here?")

		insertChannelEntry := `INSERT INTO channel_entry_new(entry_id, channel_name, package_name, operatorbundle_name, replaces, depth) SELECT entry_id, channel_name, package_name, operatorbundle_name, replaces, depth FROM channel_entry`
		_, err = tx.ExecContext(ctx, insertChannelEntry)
		if err != nil {
			return err
		}

		fmt.Println("Here?")

		renameNewAndDropOld = `
		DROP TABLE channel_entry;
		ALTER TABLE channel_entry_new RENAME TO channel_entry;
		`
		_, err = tx.ExecContext(ctx, renameNewAndDropOld)
		if err != nil {
			return err
		}

		fmt.Println("did we make it?")
		return nil
	},
	Down: func(ctx context.Context, tx *sql.Tx) error {
		foreignKeyOff := `PRAGMA foreign_keys = 0`
		createTempBundleTable := `CREATE TABLE operatorbundle_backup (name TEXT, csv TEXT, bundle TEXT, bundlepath TEXT, skiprange TEXT, version TEXT, replaces TEXT, skips TEXT)`
		backupTargetBundleTable := `INSERT INTO operatorbundle_backup SELECT name, csv, bundle, bundlepath, skiprange, version, replaces, skips FROM operatorbundle`
		createTempEntryTable := `CREATE TABLE channel_entry_backup (
								entry_id INTEGER PRIMARY KEY,
								channel_name TEXT,
								package_name TEXT,
								operatorbundle_name TEXT,
								replaces INTEGER,
								depth INTEGER,
								FOREIGN KEY(replaces) REFERENCES channel_entry(entry_id) DEFERRABLE INITIALLY DEFERRED, 
								FOREIGN KEY(channel_name, package_name) REFERENCES channel(name, package_name) ON DELETE CASCADE
							);`
		backupTargetEntryTable := `INSERT INTO channel_entry_backup(entry_id, channel_name, package_name, operatorbundle_name, replaces, depth) SELECT entry_id, channel_name, package_name, operatorbundle_name, replaces, depth FROM channel_entry`
		dropTargetTable := `DROP TABLE operatorbundle`
		renameBackUpTable := `ALTER TABLE operatorbundle_backup RENAME TO operatorbundle;`
		foreignKeyOn := `PRAGMA foreign_keys = 1`
		_, err := tx.ExecContext(ctx, foreignKeyOff)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, createTempEntryTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, backupTargetEntryTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, createTempBundleTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, backupTargetBundleTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, dropTargetTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, renameBackUpTable)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, foreignKeyOn)
		return err
	},
}

func setPackage(ctx context.Context, tx *sql.Tx, pkgName, bundle string) error {
	updateSql := `UPDATE operatorbundle_new SET package_name = ? WHERE name = ?;`
	_, err := tx.ExecContext(ctx, updateSql, pkgName, bundle)
	return err
}

func getBundlePackages(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	bundlePkgMap := make(map[string]string, 0)
	selectEntryPackageQuery := `SELECT DISTINCT operatorbundle_name, package_name FROM channel_entry`
	rows, err := tx.QueryContext(ctx, selectEntryPackageQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var bundle, pkgName sql.NullString

		if err = rows.Scan(&bundle, &pkgName); err != nil {
			return nil, err
		}

		if bundle.Valid && pkgName.Valid {
			bundlePkgMap[bundle.String] = pkgName.String
		}
	}

	return bundlePkgMap, nil
}
