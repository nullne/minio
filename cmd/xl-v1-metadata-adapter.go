package cmd

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/minio/minio/cmd/pb"
)

func (m xlMetaV1) MarshalBinary() ([]byte, error) {
	mm := pb.XlMetaV1{}
	switch m.Version {
	case xlMetaVersion:
		mm.Version = pb.XlMetaV1_V101
	case xlMetaVersion100:
		mm.Version = pb.XlMetaV1_V100

	}
	switch m.Format {
	case xlMetaFormat:
		mm.Format = pb.XlMetaV1_XL
	}

	mm.Stat = &pb.XlMetaV1_StatInfo{}
	mm.Stat.Size = m.Stat.Size
	mm.Stat.ModTime = m.Stat.ModTime.UnixNano()

	mm.Erasure = &pb.XlMetaV1_ErasureInfoType{}
	switch m.Erasure.Algorithm {
	case erasureAlgorithmKlauspost:
		mm.Erasure.Algorithm = pb.XlMetaV1_ErasureAlgorithmKlauspost
	}
	mm.Erasure.DataBlocks = int32(m.Erasure.DataBlocks)
	mm.Erasure.ParityBlocks = int32(m.Erasure.ParityBlocks)
	mm.Erasure.BlockSize = m.Erasure.BlockSize
	mm.Erasure.Index = int32(m.Erasure.Index)
	mm.Erasure.Distribution = make([]int32, len(m.Erasure.Distribution))
	for i, v := range m.Erasure.Distribution {
		mm.Erasure.Distribution[i] = int32(v)
	}
	mm.Erasure.Checksums = make([]*pb.XlMetaV1_ErasureInfoType_ChecksumInfoType, len(m.Erasure.Checksums))
	for i, v := range m.Erasure.Checksums {
		c := pb.XlMetaV1_ErasureInfoType_ChecksumInfoType{
			Name:      v.Name,
			Hash:      v.Hash,
			Algorithm: int32(v.Algorithm),
		}
		mm.Erasure.Checksums[i] = &c
	}

	mm.Minio = &pb.XlMetaV1_MinioType{}
	// ignore minio version for now because of the  size problem
	// mm.Minio.Release = m.Minio.Release

	mm.Meta = m.Meta

	mm.Parts = make([]*pb.ObjectPartInfoType, len(m.Parts))
	for i, v := range m.Parts {
		c := pb.ObjectPartInfoType{
			Number:     int32(v.Number),
			Name:       v.Name,
			Etag:       v.ETag,
			Size:       v.Size,
			ActualSize: v.ActualSize,
		}
		mm.Parts[i] = &c
	}
	return proto.Marshal(&mm)
}

func (m *xlMetaV1) UnmarshalBinary(bs []byte) error {
	mm := pb.XlMetaV1{}
	if err := proto.Unmarshal(bs, &mm); err != nil {
		return err
	}

	switch mm.Version {
	case pb.XlMetaV1_V101:
		m.Version = xlMetaVersion
	case pb.XlMetaV1_V100:
		m.Version = xlMetaVersion100

	}
	switch mm.Format {
	case pb.XlMetaV1_XL:
		m.Format = xlMetaFormat
	}

	m.Stat.Size = mm.Stat.Size

	m.Stat.ModTime = time.Unix(0, mm.Stat.ModTime)

	switch mm.Erasure.Algorithm {
	case pb.XlMetaV1_ErasureAlgorithmKlauspost:
		m.Erasure.Algorithm = erasureAlgorithmKlauspost
	}
	m.Erasure.DataBlocks = int(mm.Erasure.DataBlocks)
	m.Erasure.ParityBlocks = int(mm.Erasure.ParityBlocks)
	m.Erasure.BlockSize = mm.Erasure.BlockSize
	m.Erasure.Index = int(mm.Erasure.Index)
	m.Erasure.Distribution = make([]int, len(mm.Erasure.Distribution))
	for i, v := range mm.Erasure.Distribution {
		m.Erasure.Distribution[i] = int(v)
	}
	m.Erasure.Checksums = make([]ChecksumInfo, len(mm.Erasure.Checksums))
	for i, v := range mm.Erasure.Checksums {
		c := ChecksumInfo{
			Name:      v.Name,
			Hash:      v.Hash,
			Algorithm: BitrotAlgorithm(v.Algorithm),
		}
		m.Erasure.Checksums[i] = c
	}

	// mm.Minio = &pb.XlMetaV1_MinioType{}
	// ignore minio version for now because of the  size problem
	// mm.Minio.Release = m.Minio.Release

	m.Meta = mm.Meta

	m.Parts = make([]ObjectPartInfo, len(mm.Parts))
	for i, v := range mm.Parts {
		c := ObjectPartInfo{
			Number:     int(v.Number),
			Name:       v.Name,
			ETag:       v.Etag,
			Size:       v.Size,
			ActualSize: v.ActualSize,
		}
		m.Parts[i] = c
	}
	return nil
}
