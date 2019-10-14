package registry

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/sqlite"
)

func newOpmRegistryAddCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "add",
		Short: "add operator bundle to operator registry DB",
		Long:  `add operator bundle to operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: add,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("database", "d", "bundles.db", "relative path to database file")
	rootCmd.Flags().StringP("download-folder", "f", "", "directory where downloaded operator bundle(s) will be stored to be processed further")
	// TODO: make bundle-image comma separated list
	rootCmd.Flags().StringP("bundle-image", "b", "", "link to bundle image")
	rootCmd.Flags().StringP("manifest-path", "p", "", "path to local manifest files")
	rootCmd.Flags().Bool("permissive", false, "allow registry load errors")

	return rootCmd
}

func add(cmd *cobra.Command, args []string) error {
	downloadPath, err := cmd.Flags().GetString("download-folder")
	if err != nil {
		return err
	}

	bundleImage, err := cmd.Flags().GetString("bundle-image")
	if err != nil {
		return err
	}

	manifestPath, err := cmd.Flags().GetString("manifest-path")
	if err != nil {
		return err
	}

	fromFilename, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}
	permissive, err := cmd.Flags().GetBool("permissive")
	if err != nil {
		return err
	}

	// By default use the passed in download directory that stores the files on a local dir
	manifestDirectory := downloadPath

	if manifestDirectory == "" {
		if manifestPath != "" {
			// If it's not set, check to see if the manifests are already passed in
			manifestDirectory = manifestPath
		} else {
			// Otherwise use a temp directory that is cleaned up afterwards
			
			manifestDirectory = "./downloaded-manifests"
		}
	}

	if bundleImage != "" {
		// Pull the image and get the manifests
		reader := containertools.NewBundleReader()

		err = reader.GetBundle(bundleImage, manifestDirectory)
		if err != nil {
			return err
		}
	}

	dbLoader, err := sqlite.NewSQLLiteLoader(fromFilename)
	if err != nil {
		return err
	}
	defer dbLoader.Close()

	loader := sqlite.NewSQLLoaderForDirectory(dbLoader, manifestDirectory)
	if err := loader.Populate(); err != nil {
		err = fmt.Errorf("error loading manifests from directory: %s", err)
		if !permissive {
			logrus.WithError(err).Fatal("permissive mode disabled")
			return err
		}
		logrus.WithError(err).Warn("permissive mode enabled")
	}

	return nil
}