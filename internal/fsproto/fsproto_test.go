package fsproto

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeRequestChecksumFile(t *testing.T) {
	p := "/fonts/lv_font_dots_40.bin"
	got := EncodeRequest(ChecksumFileOpcode, ChecksumFileRequest{
		PathLen: uint16(len(p)),
		Path:    p,
	})

	want := append([]byte{
		byte(ChecksumFileOpcode),
		0x00,
		byte(len(p)),
		0x00,
	}, []byte(p)...)

	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeRequest mismatch:\n got: %x\nwant: %x", got, want)
	}
}

func TestReadResponseChecksumFile(t *testing.T) {
	pkt := []byte{
		byte(ChecksumFileResp),
		0x01,
		0x00, 0x00,
		0x09, 0x00, 0x00, 0x00,
		0x26, 0x39, 0xf4, 0xcb,
	}

	var resp ChecksumFileResponse
	if err := ReadResponse(pkt, ChecksumFileResp, &resp); err != nil {
		t.Fatalf("ReadResponse unexpected error: %v", err)
	}
	if resp.FileSize != 9 {
		t.Fatalf("expected FileSize 9, got %d", resp.FileSize)
	}
	if resp.CRC32 != 0xCBF43926 {
		t.Fatalf("expected CRC32 0xCBF43926, got 0x%08X", resp.CRC32)
	}
}

func TestReadResponseChecksumFileError(t *testing.T) {
	pkt := []byte{
		byte(ChecksumFileResp),
		0xfe, // int8(-2) = LFS_ERR_NOENT
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	var resp ChecksumFileResponse
	err := ReadResponse(pkt, ChecksumFileResp, &resp)
	var fsErr Error
	if !errors.As(err, &fsErr) {
		t.Fatalf("expected fsproto.Error, got %T (%v)", err, err)
	}
	if fsErr.Code != -2 {
		t.Fatalf("expected Error code -2, got %d", fsErr.Code)
	}
}
