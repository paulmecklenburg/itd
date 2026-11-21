package rpc

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestLoadResourcesRequestRoundTrip(t *testing.T) {
	in := &LoadResourcesRequest{
		Path: "/tmp/res.zip",
		Mode: "size+crc",
	}

	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}

	var out LoadResourcesRequest
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}

	if out.GetPath() != in.Path || out.GetMode() != in.Mode {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", &out, in)
	}

	if got := out.ProtoReflect().Descriptor().FullName(); got != "rpc.LoadResourcesRequest" {
		t.Fatalf("descriptor FullName = %q, want rpc.LoadResourcesRequest", got)
	}
}

func TestResourceLoadProgressCheckAndSkipRoundTrip(t *testing.T) {
	for _, op := range []ResourceLoadProgress_Operation{
		ResourceLoadProgress_Check,
		ResourceLoadProgress_Skip,
	} {
		in := &ResourceLoadProgress{
			Name:      "lv_font_dots_40.bin",
			Total:     1234,
			Sent:      1234,
			Operation: op,
		}

		b, err := proto.Marshal(in)
		if err != nil {
			t.Fatalf("proto.Marshal(%v) failed: %v", op, err)
		}

		var out ResourceLoadProgress
		if err := proto.Unmarshal(b, &out); err != nil {
			t.Fatalf("proto.Unmarshal(%v) failed: %v", op, err)
		}

		if out.GetOperation() != op {
			t.Fatalf("Operation = %v, want %v", out.GetOperation(), op)
		}
	}
}
