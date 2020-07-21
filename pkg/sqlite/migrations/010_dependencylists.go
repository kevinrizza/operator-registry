package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/operator-framework/operator-registry/pkg/registry"
)

const DependencyListsMigrationKey = 10

// Register this migration
func init() {
	registerMigration(DependencyListsMigrationKey, dependencyListMigration)
}

type dep struct {
	key   bundleKey
	value string
}

type typedDep struct {
	key     bundleKey
	value   string
	depType string
}

var dependencyListMigration = &Migration{
	Id: DependencyListsMigrationKey,
	Up: func(ctx context.Context, tx *sql.Tx) error {
		sql := `
		CREATE TABLE IF NOT EXISTS dependencies_new (
			value TEXT,
			operatorbundle_name TEXT,
			operatorbundle_version TEXT,
			operatorbundle_path TEXT,
			FOREIGN KEY(operatorbundle_name, operatorbundle_version, operatorbundle_path) REFERENCES operatorbundle(name, version, bundlepath) ON DELETE CASCADE
		);
		`
		_, err := tx.ExecContext(ctx, sql)
		if err != nil {
			return err
		}

		insertRequired := `INSERT INTO dependencies_new(value, operatorbundle_name, operatorbundle_version, operatorbundle_path) VALUES (?, ?, ?, ?)`

		dependencies, err := getDependencies(ctx, tx)
		if err != nil {
			return err
		}
		for _, dependency := range dependencies {
			_, err = tx.ExecContext(ctx, insertRequired, dependency.value, dependency.key.CsvName, dependency.key.Version, dependency.key.BundlePath)
			if err != nil {
				return err
			}
		}

		renameNewAndDropOld := `
		DROP TABLE dependencies;
		ALTER TABLE dependencies_new RENAME TO dependencies;
		`
		_, err = tx.ExecContext(ctx, renameNewAndDropOld)
		if err != nil {
			return err
		}

		return nil
	},
	Down: func(ctx context.Context, tx *sql.Tx) error {
		foreignKeyOff := `PRAGMA foreign_keys = 0`
		createTempTable := `
		CREATE TABLE IF NOT EXISTS dependencies_backup (
			type TEXT,
			value TEXT,
			operatorbundle_name TEXT,
			operatorbundle_version TEXT,
			operatorbundle_path TEXT,
			FOREIGN KEY(operatorbundle_name, operatorbundle_version, operatorbundle_path) REFERENCES operatorbundle(name, version, bundlepath) ON DELETE CASCADE
		);
		`

		_, err := tx.ExecContext(ctx, foreignKeyOff)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, createTempTable)
		if err != nil {
			return err
		}

		insertDeps := `INSERT INTO dependencies_backup(type, value, operatorbundle_name, operatorbundle_version, operatorbundle_path) VALUES (?, ?, ?, ?, ?)`
		oldFormatDependencies, err := getOldFormatDependencies(ctx, tx)
		if err != nil {
			return err
		}

		for _, dependency := range oldFormatDependencies {
			_, err = tx.ExecContext(ctx, insertDeps, dependency.depType, dependency.value, dependency.key.CsvName, dependency.key.Version, dependency.key.BundlePath)
			if err != nil {
				return err
			}
		}

		dropTargetTable := `DROP TABLE dependencies`
		renameBackUpTable := `ALTER TABLE dependencies_backup RENAME TO dependencies;`
		foreignKeyOn := `PRAGMA foreign_keys = 1`

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

func getDependencies(ctx context.Context, tx *sql.Tx) ([]dep, error) {
	deps := make([]dep, 0)

	dependencyQuery := `SELECT dependencies.type, dependencies.value, dependencies.operatorbundle_name, dependencies.operatorbundle_version, dependencies.operatorbundle_path
  FROM dependencies`

	dependencyRows, err := tx.QueryContext(ctx, dependencyQuery)
	if err != nil {
		return nil, err
	}
	for dependencyRows.Next() {
		var depType sql.NullString
		var value sql.NullString
		var name sql.NullString
		var version sql.NullString
		var path sql.NullString
		if err = dependencyRows.Scan(&depType, &value, &name, &version, &path); err != nil {
			return nil, err
		}
		if !depType.Valid || !value.Valid || !name.Valid {
			continue
		}

		key := bundleKey{
			BundlePath: path,
			Version:    version,
			CsvName:    name,
		}
		var val string

		if depType.String == registry.GVKType {
			depValue := registry.GVKDependency{}
			if err := json.Unmarshal([]byte(value.String), &depValue); err != nil {
				return nil, fmt.Errorf("Unable to unmarshal dependency value of olm.gvk dependency for %s %s %s", name.String, version.String, path.String)
			}

			val = fmt.Sprintf("%s: %s/%s/%s", registry.GVKType, depValue.Group, depValue.Version, depValue.Kind)
		}

		if depType.String == registry.PackageType {
			depValue := registry.PackageDependency{}
			if err := json.Unmarshal([]byte(value.String), depValue); err != nil {
				return nil, fmt.Errorf("Unable to unmarshal dependency value of olm.package dependency for %s %s %s", name.String, version.String, path.String)
			}

			val = fmt.Sprintf("%s: %s, %s", registry.PackageType, depValue.PackageName, depValue.Version)
		}

		deps = append(deps, dep{key: key, value: val})
	}

	return deps, nil
}

func getOldFormatDependencies(ctx context.Context, tx *sql.Tx) ([]typedDep, error) {
	typedDeps := make([]typedDep, 0)

	dependencyQuery := `SELECT dependencies.value, dependencies.operatorbundle_name, dependencies.operatorbundle_version, dependencies.operatorbundle_path
  FROM dependencies`

	dependencyRows, err := tx.QueryContext(ctx, dependencyQuery)
	if err != nil {
		return nil, err
	}
	for dependencyRows.Next() {
		var value sql.NullString
		var name sql.NullString
		var version sql.NullString
		var path sql.NullString
		if err = dependencyRows.Scan(&value, &name, &version, &path); err != nil {
			return nil, err
		}
		if !value.Valid || !name.Valid {
			continue
		}

		key := bundleKey{
			BundlePath: path,
			Version:    version,
			CsvName:    name,
		}

		depList := strings.Split(strings.Replace(value.String, " ", "", -1), ";")
		for _, dep := range depList {
			depElements := strings.Split(dep, ":")
			if len(depElements) != 2 {
				return nil, fmt.Errorf("unable to parse dependency: not able to determine dependency type from value")
			}

			switch depType := depElements[0]; depType {
			case registry.GVKType:
				gvkElements := strings.Split(depElements[1], "/")
				if len(gvkElements) != 3 {
					return nil, fmt.Errorf("unable to parse olm.gvk dependency: invalid gvk defined for dependency")
				}
				depValue := registry.GVKDependency{
					Group:   gvkElements[0],
					Version: gvkElements[1],
					Kind:    gvkElements[2],
				}
				val, err := json.Marshal(depValue)
				if err != nil {
					return nil, fmt.Errorf("unable to marshal olm.gvk dependency into string: %s", err.Error())
				}
				typedDeps = append(typedDeps, typedDep{key: key, value: string(val), depType: depType})
			case registry.PackageType:
				pkgElements := strings.Split(depElements[1], ",")
				if len(pkgElements) != 2 {
					return nil, fmt.Errorf("unable to parse olm.package dependency: invalid syntax for package version")
				}
				depValue := registry.PackageDependency{
					PackageName: pkgElements[0],
					Version:     pkgElements[1],
				}
				val, err := json.Marshal(depValue)
				if err != nil {
					return nil, fmt.Errorf("unable to marshal olm.package dependency into string: %s", err.Error())
				}
				typedDeps = append(typedDeps, typedDep{key: key, value: string(val), depType: depType})
			default:
				return nil, fmt.Errorf("invalid type %s defined for dependency", depType)
			}
		}
	}

	return typedDeps, nil
}
