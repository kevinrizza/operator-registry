package main

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	cli "github.com/operator-framework/operator-registry/cmd/cli"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "opm",
		Short: "operator-registry",
		Long:  "Top level CLI for operator-registry",
	}

	rootCmd.AddCommand(cli.NewOpmRegistryCmd())

	if err := rootCmd.Execute(); err != nil {
		logrus.Panic(err.Error())
	}
}
