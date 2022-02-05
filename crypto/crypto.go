package crypto

import (
	"hash/crc64"
)

var crc64Table *crc64.Table

func InitCRCHashTable() {
	crc64Table = crc64.MakeTable(crc64.ECMA)
}

func Checksum(bytes []byte) uint64 {
	return crc64.Checksum(bytes, crc64Table)
}
