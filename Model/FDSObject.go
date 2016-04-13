package Model

type FDSObject struct {
	ObjectName string
	BucketName string
	Metadata   FDSMetaData
	ObjectContent []byte
}
