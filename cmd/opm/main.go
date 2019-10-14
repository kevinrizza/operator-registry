package main

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/operator-framework/operator-registry/cmd/opm/registry"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "opm",
		Short: "operator-registry",
		Long:  "Top level CLI for operator-registry",
	}

	rootCmd.AddCommand(registry.NewOpmRegistryCmd())

	if err := rootCmd.Execute(); err != nil {
		logrus.Panic(err.Error())
	}
}
