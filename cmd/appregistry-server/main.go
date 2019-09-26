package main

import (
	"github.com/sirupsen/logrus"

	cli "github.com/operator-framework/operator-registry/cmd/cli"
)

func main() {
	rootCmd := cli.NewAppregistryServerCmd()

	if err := rootCmd.Flags().MarkHidden("debug"); err != nil {
		logrus.Panic(err.Error())
	}

	if err := rootCmd.Execute(); err != nil {
		logrus.Panic(err.Error())
	}
}
