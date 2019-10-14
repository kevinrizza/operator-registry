package registry

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/operator-framework/operator-registry/pkg/containertools"
)

func newOpmRegistryGetCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "get",
		Short: "get operator bundle manifests from a bundle image in an image registry",
		Long:  `get operator bundle manifests from a bundle image in an image registry`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: get,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("download-folder", "f", "bundle", "directory where downloaded operator bundle(s) will be stored to be processed further")
	rootCmd.Flags().StringP("bundle-image", "b", "", "link to bundle image")

	return rootCmd
}


func get(cmd *cobra.Command, args []string) error {
	downloadPath, err := cmd.Flags().GetString("download-folder")
	if err != nil {
		return err
	}

	bundleImage, err := cmd.Flags().GetString("bundle-image")
	if err != nil {
		return err
	}

	// Pull the image and get the manifests
	reader := containertools.NewBundleReader()

	err = reader.GetBundle(bundleImage, downloadPath)
	if err != nil {
		return err
	}

	return nil
}