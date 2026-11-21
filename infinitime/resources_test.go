package infinitime

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseResourceLoadMode(t *testing.T) {
	tests := []struct {
		in   string
		want ResourceLoadMode
		err  bool
	}{
		{in: "", want: ResourceLoadModeAuto},
		{in: "auto", want: ResourceLoadModeAuto},
		{in: "delete+copy", want: ResourceLoadModeDeleteCopy},
		{in: "size-only", want: ResourceLoadModeSizeOnly},
		{in: "size+crc", want: ResourceLoadModeSizeCRC},
		{in: "bogus", err: true},
		{in: "SIZE+CRC", err: true},
	}

	for _, tc := range tests {
		got, err := ParseResourceLoadMode(tc.in)
		if tc.err {
			if err == nil {
				t.Fatalf("ParseResourceLoadMode(%q) unexpectedly succeeded with %q", tc.in, got)
			}
			if !errors.Is(err, ErrInvalidResourceMode) {
				t.Fatalf("ParseResourceLoadMode(%q) error = %v, want ErrInvalidResourceMode", tc.in, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseResourceLoadMode(%q) unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseResourceLoadMode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestStreamSizeAndCRC32(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantSize uint32
		wantCRC  uint32
	}{
		{
			name:     "empty",
			data:     nil,
			wantSize: 0,
			wantCRC:  0x00000000,
		},
		{
			name:     "123456789",
			data:     []byte("123456789"),
			wantSize: 9,
			wantCRC:  0xCBF43926,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			size, crc, err := streamSizeAndCRC32(bytes.NewReader(tc.data))
			if err != nil {
				t.Fatalf("streamSizeAndCRC32 unexpected error: %v", err)
			}
			if size != tc.wantSize {
				t.Fatalf("size = %d, want %d", size, tc.wantSize)
			}
			if crc != tc.wantCRC {
				t.Fatalf("crc = 0x%08X, want 0x%08X", crc, tc.wantCRC)
			}
		})
	}
}
