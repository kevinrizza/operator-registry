package cli

import (
	"context"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/operator-framework/operator-registry/pkg/api"
	health "github.com/operator-framework/operator-registry/pkg/api/grpc_health_v1"
	"github.com/operator-framework/operator-registry/pkg/lib/log"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/operator-framework/operator-registry/pkg/server"
	"github.com/operator-framework/operator-registry/pkg/sqlite"
)

// NewOpmRegistryCmd returns the appregistry-server command
func NewOpmRegistryCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "registry",
		Short: "registry",
		Long:  `registry can build and/or serve operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},
	}

	rootCmd.AddCommand(newOpmRegistryServeCmd())
	rootCmd.AddCommand(newOpmRegistryAddCmd())
	rootCmd.AddCommand(newOpmRegistryRmCmd())

	return rootCmd
}

func newOpmRegistryServeCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "serve",
		Short: "serve",
		Long:  `serve operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: runOpmRegistryServeCmdFunc,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("database", "d", "bundles.db", "relative path to sqlite db")
	rootCmd.Flags().StringP("port", "p", "50051", "port number to serve on")
	rootCmd.Flags().StringP("termination-log", "t", "/dev/termination-log", "path to a container termination log file")

	return rootCmd

}

func newOpmRegistryAddCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "add",
		Short: "add",
		Long:  `add operator bundle to operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: runOpmRegistryAddCmdFunc,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("database", "d", "bundles.db", "relative path to database file")
	rootCmd.Flags().StringP("manifests", "m", "manifests", "relative path to directory of manifests")
	rootCmd.Flags().Bool("permissive", false, "allow registry load errors")

	return rootCmd
}

func newOpmRegistryRmCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "rm",
		Short: "Remove",
		Long:  `Remove operator from operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: runOpmRegistryRmCmdFunc,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("database", "d", "bundles.db", "relative path to database file")
	rootCmd.Flags().StringP("packages", "o", "", "comma separated list of package names to be deleted")
	rootCmd.Flags().Bool("permissive", false, "allow registry load errors")

	return rootCmd
}

func runOpmRegistryServeCmdFunc(cmd *cobra.Command, args []string) error {
	// Immediately set up termination log
	terminationLogPath, err := cmd.Flags().GetString("termination-log")
	if err != nil {
		return err
	}
	err = log.AddDefaultWriterHooks(terminationLogPath)
	if err != nil {
		logrus.WithError(err).Warn("unable to set termination log path")
	}
	dbName, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}

	port, err := cmd.Flags().GetString("port")
	if err != nil {
		return err
	}

	logger := logrus.WithFields(logrus.Fields{"database": dbName, "port": port})

	var store registry.Query
	store, err = sqlite.NewSQLLiteQuerier(dbName)
	if err != nil {
		logger.WithError(err).Warnf("failed to load db")
	}
	if store == nil {
		store = registry.NewEmptyQuerier()
	}

	// sanity check that the db is available
	tables, err := store.ListTables(context.TODO())
	if err != nil {
		logger.WithError(err).Warnf("couldn't list tables in db")
	}
	if len(tables) == 0 {
		logger.Warn("no tables found in db")
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatalf("failed to listen: %s", err)
	}
	s := grpc.NewServer()

	api.RegisterRegistryServer(s, server.NewRegistryServer(store))
	health.RegisterHealthServer(s, server.NewHealthServer())
	reflection.Register(s)
	logger.Info("serving registry")
	if err := s.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %s", err)
	}

	return nil
}

func runOpmRegistryAddCmdFunc(cmd *cobra.Command, args []string) error {
	fromFilename, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}
	manifestDir, err := cmd.Flags().GetString("manifests")
	if err != nil {
		return err
	}
	permissive, err := cmd.Flags().GetBool("permissive")
	if err != nil {
		return err
	}

	dbLoader, err := sqlite.NewSQLLiteLoader(fromFilename)
	if err != nil {
		return err
	}
	defer dbLoader.Close()

	loader := sqlite.NewSQLLoaderForDirectory(dbLoader, manifestDir)
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

func runOpmRegistryRmCmdFunc(cmd *cobra.Command, args []string) error {
	fromFilename, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}
	packages, err := cmd.Flags().GetString("packages")
	if err != nil {
		return err
	}
	permissive, err := cmd.Flags().GetBool("permissive")
	if err != nil {
		return err
	}

	dbLoader, err := sqlite.NewSQLLiteLoader(fromFilename)
	if err != nil {
		return err
	}
	defer dbLoader.Close()

	remover := sqlite.NewSQLRemoverForPackages(dbLoader, packages)
	if err := remover.Remove(); err != nil {
		err = fmt.Errorf("error deleting packages from database: %s", err)
		if !permissive {
			logrus.WithError(err).Fatal("permissive mode disabled")
			return err
		}
		logrus.WithError(err).Warn("permissive mode enabled")
	}

	return nil
}
