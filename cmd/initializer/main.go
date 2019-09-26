package main

import (
	cli "github.com/operator-framework/operator-registry/cmd/cli"
)

func main() {
	rootCmd := cli.NewInitializerCmd()

	if err := rootCmd.Flags().MarkHidden("debug"); err != nil {
		panic(err)
	}

	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
