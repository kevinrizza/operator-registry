package registry

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewOpmRegistryCmd returns the appregistry-server command
func NewOpmRegistryCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "registry",
		Short: "registry can build and/or serve operator registry DB",
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
	rootCmd.AddCommand(newOpmRegistryGetCmd())

	return rootCmd
}