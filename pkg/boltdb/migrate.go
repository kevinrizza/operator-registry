package boltdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/alicebob/sqlittle"
	"github.com/asdine/storm/v3"
	bolt "go.etcd.io/bbolt"
	"os"
)

type rowMigrator func(node storm.Node, errs []error) (sqlittle.RowCB, string, []string)

// TODO: thread a logger through

func EnsureBolt(file string, backupFile string) error {
	db, err := bolt.Open(file, 0600, nil)

	// file is already a boltdb, no migration from sqlite needed
	if err == nil {
		return db.Close()
	}

	// move current db to a backup location
	if err := os.Rename(file, backupFile); err != nil {
		return err
	}

	// make new storm db and migrate
	sqlDb, err := sqlittle.Open(backupFile)
	if err != nil {
		return err
	}

	// TODO: check if sqlite file is at latest migration

	bdb, err := storm.Open(file)
	if err != nil {
		return nil
	}

	if err := migrateSqliteToBolt(sqlDb, bdb); err != nil {
		return err
	}

	if err := bdb.Close(); err != nil {
		return err
	}
	return sqlDb.Close()
}

func migrateSqliteToBolt(sqlDb *sqlittle.DB, bdb *storm.DB) error {
	tx, err := bdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); !errors.Is(err, storm.ErrNotInTransaction) {
			fmt.Printf("error rolling back: %v\n", err)
		}
	}()

	migrations := []rowMigrator{migratePackageRow, migrateChannelRow, migrateBundleRow, migrateRelatedImageRow}
	for _, m := range migrations {
		if err := migrate(tx, sqlDb, m); err != nil {
			return err
		}
	}

	if err := migrateApiProviders(sqlDb, tx); err != nil {
		return err
	}

	if err := migrateApiRequirers(sqlDb, tx); err != nil {
		return err
	}

	if err := migrateChannelEntries(sqlDb, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func migrate(node storm.Node, sqlDb *sqlittle.DB, rowMigrator rowMigrator) error {
	// migrate package table
	migrateError := make([]error, 0)
	migrator, table, columns := rowMigrator(node, migrateError)
	if err := sqlDb.Select(table, migrator, columns...); err != nil {
		return err
	}
	// TODO: bubble up all errors for display
	if len(migrateError) > 0 {
		return migrateError[0]
	}
	return nil
}

func migratePackageRow(node storm.Node, errs []error) (sqlittle.RowCB, string, []string) {
	columns := []string{"name", "default_channel"}
	table := "package"

	return func(r sqlittle.Row) {
		var (
			name           string
			defaultChannel string
		)
		if err := r.Scan(&name, &defaultChannel); err != nil {
			errs = append(errs, err)
			return
		}
		pkg := Package{
			Name:           name,
			DefaultChannel: defaultChannel,
		}
		if err := node.Save(&pkg); err != nil {
			errs = append(errs, err)
			return
		}
	}, table, columns
}

func migrateChannelRow(node storm.Node, errs []error) (sqlittle.RowCB, string, []string) {
	columns := []string{"name", "package_name", "head_operatorbundle_name"}
	table := "channel"
	return func(r sqlittle.Row) {
		var (
			name           string
			pkgName        string
			headBundleName string
		)
		if err := r.Scan(&name, &pkgName, &headBundleName); err != nil {
			errs = append(errs, err)
			return
		}
		fmt.Printf("migrating channel %s %s %s\n", name, pkgName, headBundleName)
		ch := Channel{
			PackageChannel: PackageChannel{
				ChannelName: name,
				PackageName: pkgName,
			},
			HeadOperatorBundleName: headBundleName,
		}
		if err := node.Save(&ch); err != nil {
			errs = append(errs, err)
			return
		}
	}, table, columns
}

func migrateBundleRow(node storm.Node, errs []error) (sqlittle.RowCB, string, []string) {
	columns := []string{"name", "version", "bundle", "csv", "skiprange", "bundlepath"}
	table := "operatorbundle"
	return func(r sqlittle.Row) {
		var (
			name       string
			version    string
			bundle     []byte
			csv        []byte
			skiprange  string
			bundlepath string
		)
		err := r.Scan(&name, &version, &bundle, &csv, &skiprange, &bundlepath)
		if err != nil {
			errs = append(errs, err)
			return
		}

		// TODO: replaces
		// TODO: skips
		ob := OperatorBundle{
			Name:    name,
			Version: version,
			//Replaces:   ,
			SkipRange: skiprange,
			//Skips:      nil,
			CSV:        bytes.TrimSuffix(csv, []byte("\n")),
			Bundle:     bundle,
			BundlePath: bundlepath,
		}

		if len(bundle) == 0 && len(bundlepath) == 0 {
			err := fmt.Errorf("no bundle or image ref found for bundle %s", name)
			errs = append(errs, err)
			return
		}

		err = node.Save(&ob)
		return
	}, table, columns

}

func migrateRelatedImageRow(node storm.Node, errs []error) (sqlittle.RowCB, string, []string) {
	columns := []string{"image", "operatorbundle_name"}
	table := "related_image"

	return func(r sqlittle.Row) {
		var (
			image               string
			operatorbundle_name string
		)
		if err := r.Scan(&image, &operatorbundle_name); err != nil {
			errs = append(errs, err)
			return
		}
		relatedImg := RelatedImage{
			ImageUser: ImageUser{
				Image:              image,
				OperatorBundleName: operatorbundle_name,
			},
		}
		if err := node.Save(&relatedImg); err != nil {
			errs = append(errs, err)
			return
		}
	}, table, columns
}

func migrateChannelEntries(sqlDb *sqlittle.DB, node storm.Node) error {
	columns := []string{"entry_id", "channel_name", "package_name", "operatorbundle_name", "replaces"}
	table := "channel_entry"

	type unpack struct {
		ChannelEntry
		replaces int64
	}
	unpacks := make(map[int64]unpack, 0)
	var errs []error
	getChannelEntries := func(r sqlittle.Row) {
		var (
			entry_id            int64
			channel_name        string
			package_name        string
			operatorbundle_name string
			replaces            int64
		)
		if err := r.Scan(&entry_id, &channel_name, &package_name, &operatorbundle_name, &replaces); err != nil {
			errs = append(errs, err)
			return
		}
		unpacks[entry_id] = unpack{
			ChannelEntry: ChannelEntry{
				ChannelReplacement: ChannelReplacement{
					PackageChannel: PackageChannel{
						PackageName: package_name,
						ChannelName: channel_name,
					},
					BundleName: operatorbundle_name,
				},
			},
			replaces: replaces,
		}
	}
	if err := sqlDb.Select(table, getChannelEntries, columns...); err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs[0]
	}

	for _, unpack := range unpacks {
		if unpack.replaces != 0 { // nil
			if replacementEntry, ok := unpacks[unpack.replaces]; ok {
				unpack.ChannelEntry.Replaces = replacementEntry.BundleName
			} else {
				return fmt.Errorf("Unable to find replacement for channel entry")
			}
		}

		if err := node.Save(&unpack.ChannelEntry); err != nil {
			return err
		}
	}

	return nil
}

func migrateApiProviders(sqlDb *sqlittle.DB, node storm.Node) error {
	columns := []string{"group_name", "version", "kind", "channel_entry_id"}
	table := "api_provider"

	type unpack struct {
		Capability
		channel_entry_id    int64
		operatorbundle_name string
	}
	unpacks := make([]unpack, 0)
	var errs []error
	getProvider := func(r sqlittle.Row) {
		var (
			groupName        string
			version          string
			kind             string
			channel_entry_id int64
		)
		if err := r.Scan(&groupName, &version, &kind, &channel_entry_id); err != nil {
			errs = append(errs, err)
			return
		}
		unpacks = append(unpacks, unpack{
			Capability: Capability{
				Name: GvkCapability,
				Value: Api{
					Group:   groupName,
					Version: version,
					Kind:    kind,
				}.String(),
			},
			channel_entry_id: channel_entry_id,
		})
	}
	if err := sqlDb.Select(table, getProvider, columns...); err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// fill out the operatorbundle name
	for i, u := range unpacks {
		err := sqlDb.PKSelect("channel_entry", sqlittle.Key{u.channel_entry_id}, func(rows sqlittle.Row) {
			var err error
			u.operatorbundle_name, err = rows.ScanString()
			if err != nil {
				errs = append(errs, err)
			}
		}, "operatorbundle_name")
		if err != nil {
			return err
		}
		unpacks[i] = u
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// fill out the plural
	for i, u := range unpacks {
		err := sqlDb.Select("api", func(rows sqlittle.Row) {
			var (
				groupName string
				version   string
				kind      string
				plural    string
			)
			if err := rows.Scan(&groupName, &version, &kind, &plural); err != nil {
				errs = append(errs, err)
				return
			}
			capValue, err := ApiFromString(u.Value)
			if err != nil {
				errs = append(errs, err)
				return
			}
			if u.Name == GvkCapability && capValue.Group == groupName && capValue.Version == version && capValue.Kind == kind {
				capValue.Plural = plural
			}
			u.Value = capValue.String()
			unpacks[i] = u
		}, "group_name", "version", "kind", "plural")
		if err != nil {
			return err
		}
		unpacks[i] = u
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// connect provided apis to their owner operator bundles
	bundleCapabilityFilter := make(map[string]map[string]struct{})
	for _, u := range unpacks {
		// Filter out duplicates
		if capabilities, ok := bundleCapabilityFilter[u.operatorbundle_name]; ok {
			if _, ok := capabilities[u.Capability.Value]; ok {
				continue
			}
		} else { // initialize the set
			capabilitySet := make(map[string]struct{})
			bundleCapabilityFilter[u.operatorbundle_name] = capabilitySet
		}

		var ob OperatorBundle
		err := node.One("Name", u.operatorbundle_name, &ob)
		if err != nil {
			return err
		}
		if ob.Capabilities == nil {
			ob.Capabilities = make([]Capability, 0)
		}
		ob.Capabilities = append(ob.Capabilities, u.Capability)
		if err := node.Save(&ob); err != nil {
			return err
		}

		// add unique item to filter once it's added to the set
		bundleCapabilityFilter[u.operatorbundle_name][u.Capability.Value] = struct{}{}
	}

	return nil
}

func migrateApiRequirers(sqlDb *sqlittle.DB, node storm.Node) error {
	columns := []string{"group_name", "version", "kind", "channel_entry_id"}
	table := "api_requirer"

	type unpack struct {
		Requirement
		channel_entry_id    int64
		operatorbundle_name string
	}
	unpacks := make([]unpack, 0)
	var errs []error
	getRequirer := func(r sqlittle.Row) {
		var (
			groupName        string
			version          string
			kind             string
			channel_entry_id int64
		)
		if err := r.Scan(&groupName, &version, &kind, &channel_entry_id); err != nil {
			errs = append(errs, err)
			return
		}
		unpacks = append(unpacks, unpack{
			Requirement: Requirement{
				Name: GvkCapability,
				Selector: Api{
					Group:   groupName,
					Version: version,
					Kind:    kind,
				}.String(),
				Optional: false,
			},
			channel_entry_id: channel_entry_id,
		})
	}
	if err := sqlDb.Select(table, getRequirer, columns...); err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// fill out the operatorbundle name
	for i, u := range unpacks {
		err := sqlDb.PKSelect("channel_entry", sqlittle.Key{u.channel_entry_id}, func(rows sqlittle.Row) {
			var err error
			u.operatorbundle_name, err = rows.ScanString()
			if err != nil {
				errs = append(errs, err)
			}
		}, "operatorbundle_name")
		if err != nil {
			return err
		}
		unpacks[i] = u
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// fill out the plural
	for i, u := range unpacks {
		err := sqlDb.Select("api", func(rows sqlittle.Row) {
			var (
				groupName string
				version   string
				kind      string
				plural    string
			)
			if err := rows.Scan(&groupName, &version, &kind, &plural); err != nil {
				errs = append(errs, err)
				return
			}
			if u.Name == GvkCapability {
				reqSelector, err := ApiFromString(u.Selector)
				if err != nil {
					errs = append(errs, err)
					return
				}
				if reqSelector.Group == groupName && reqSelector.Version == version && reqSelector.Kind == kind {
					reqSelector.Plural = plural
				}
				u.Selector = reqSelector.String()
				unpacks[i] = u
			} else {
				err := fmt.Errorf("Unsupported requirement type: %s", u.Name)
				errs = append(errs, err)
				return
			}
		}, "group_name", "version", "kind", "plural")
		if err != nil {
			return err
		}
		unpacks[i] = u
	}
	if len(errs) > 0 {
		return errs[0]
	}

	// connect required apis to their owner operator bundles
	bundleRequirementFilter := make(map[string]map[string]struct{})
	for _, u := range unpacks {
		// Filter out duplicates
		if capabilities, ok := bundleRequirementFilter[u.operatorbundle_name]; ok {
			if _, ok := capabilities[u.Requirement.Selector]; ok {
				continue
			}
		} else { // initialize the set
			capabilitySet := make(map[string]struct{})
			bundleRequirementFilter[u.operatorbundle_name] = capabilitySet
		}

		var ob OperatorBundle
		err := node.One("Name", u.operatorbundle_name, &ob)
		if err != nil {
			return err
		}
		if ob.Requirements == nil {
			ob.Requirements = make([]Requirement, 0)
		}
		ob.Requirements = append(ob.Requirements, u.Requirement)
		if err := node.Save(&ob); err != nil {
			return err
		}

		// add unique item to filter once it's added to the set
		bundleRequirementFilter[u.operatorbundle_name][u.Requirement.Selector] = struct{}{}
	}

	return nil
}
