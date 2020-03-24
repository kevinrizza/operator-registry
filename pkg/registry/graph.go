package registry

type Package struct {
	Name           string
	DefaultChannel string
	Channels       []Channel
}

type Channel struct {
	Name            string
	OperatorBundles []OperatorBundle
	Head            BundleRef
}

type OperatorBundle struct {
	BundlePath      string
	Version         string // semver string
	CsvName         string
	ReplacesBundles []OperatorBundle
	Replaces        []BundleRef
}

type BundleRef struct {
	BundlePath string
	Version    string //semver string
	CsvName    string
}

func (b *BundleRef) IsEmptyRef() bool {
	if b.BundlePath == "" && b.Version == "" && b.CsvName == "" {
		return true
	}
	return false
}
