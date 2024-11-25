// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
)

const (
	recordChecksumLen = 8
	recordVersionLen  = 1
	recordTypeLen     = 2

	errInvalidCRC = "invalid CRC checksum"
)

type Record struct {
	Version uint8
	Type    uint16
	Payload []byte
}

func (r *Record) Bytes() []byte {

	payloadLen := len(r.Payload)

	buff := make([]byte, recordVersionLen+recordTypeLen+payloadLen+recordChecksumLen)

	var pos int

	buff[pos] = r.Version
	pos++

	binary.BigEndian.PutUint16(buff[pos:], r.Type)
	pos += 2

	copy(buff[pos:], r.Payload)
	pos += len(r.Payload)

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := crc.Write(buff[:pos]); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}
	copy(buff[pos:pos+8], crc.Sum(nil))

	return buff
}

func (r *Record) FromBytes(buff []byte) error {
	if len(buff) == 0 {
		return fmt.Errorf("record empty")
	}

	if len(buff) < 8 {
		return fmt.Errorf("record too short, expected 8 bytes for CRC")
	}

	dataLen := len(buff) - 8

	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := crc.Write(buff[:dataLen]); err != nil {
		panic(fmt.Sprintf("CRC checksum failed: %v", err))
	}

	checksum := buff[dataLen:]

	if !bytes.Equal(checksum, crc.Sum(nil)) {
		return fmt.Errorf(errInvalidCRC)
	}

	lengthWithoutChecksumAndPayload := recordVersionLen + recordTypeLen
	if dataLen < lengthWithoutChecksumAndPayload {
		return fmt.Errorf("record too short, expected at least additional %d bytes", lengthWithoutChecksumAndPayload)
	}

	payloadLen := dataLen - lengthWithoutChecksumAndPayload

	if payloadLen == 0 {
		return fmt.Errorf("record too short, expected at least 1 byte for payload")
	}

	var pos int

	version := buff[0]
	pos++

	recType := binary.BigEndian.Uint16(buff[pos : pos+2])
	pos += 2

	payload := buff[pos : pos+payloadLen]

	r.Version = version
	r.Type = recType
	r.Payload = payload

	return nil
}
