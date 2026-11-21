package rpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/protobuf/proto"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
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

func TestConnectRequestAndITDServiceDescriptors(t *testing.T) {
	in := &ConnectRequest{Address: "AA:BB:CC:DD:EE:FF"}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}

	var out ConnectRequest
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if out.GetAddress() != in.Address {
		t.Fatalf("Address = %q, want %q", out.GetAddress(), in.Address)
	}
	if got := out.ProtoReflect().Descriptor().FullName(); got != "rpc.ConnectRequest" {
		t.Fatalf("descriptor FullName = %q, want rpc.ConnectRequest", got)
	}

	itdSvc := File_itd_proto.Services().ByName("ITD")
	if itdSvc == nil {
		t.Fatal("File_itd_proto.Services().ByName(ITD) is nil")
	}

	connMethod := itdSvc.Methods().ByName("Connect")
	if connMethod == nil {
		t.Fatal("ITD.Connect method descriptor is nil")
	}
	if got := connMethod.Input().FullName(); got != "rpc.ConnectRequest" {
		t.Fatalf("Connect input = %q, want rpc.ConnectRequest", got)
	}
	if got := connMethod.Output().FullName(); got != "rpc.StringResponse" {
		t.Fatalf("Connect output = %q, want rpc.StringResponse", got)
	}

	discMethod := itdSvc.Methods().ByName("Disconnect")
	if discMethod == nil {
		t.Fatal("ITD.Disconnect method descriptor is nil")
	}
	if got := discMethod.Input().FullName(); got != "rpc.Empty" {
		t.Fatalf("Disconnect input = %q, want rpc.Empty", got)
	}
	if got := discMethod.Output().FullName(); got != "rpc.StringResponse" {
		t.Fatalf("Disconnect output = %q, want rpc.StringResponse", got)
	}
}

type mockITDServer struct {
	DRPCITDUnimplementedServer
	lastConnectAddr string
	disconnected    bool
}

func (m *mockITDServer) Connect(_ context.Context, req *ConnectRequest) (*StringResponse, error) {
	m.lastConnectAddr = req.GetAddress()
	return &StringResponse{Value: req.GetAddress()}, nil
}

func (m *mockITDServer) Disconnect(_ context.Context, _ *Empty) (*StringResponse, error) {
	m.disconnected = true
	return &StringResponse{Value: "AA:BB:CC:DD:EE:FF"}, nil
}

func TestDRPCConnectAndDisconnectRoundTrip(t *testing.T) {
	mux := drpcmux.New()
	impl := &mockITDServer{}
	if err := DRPCRegisterITD(mux, impl); err != nil {
		t.Fatalf("DRPCRegisterITD failed: %v", err)
	}

	cConn, sConn := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := drpcserver.New(mux)
	srvDone := make(chan error, 1)
	go func() {
		srvDone <- srv.ServeOne(ctx, sConn)
	}()

	dconn := drpcconn.New(cConn)
	defer dconn.Close()

	client := NewDRPCITDClient(dconn)
	connResp, err := client.Connect(ctx, &ConnectRequest{Address: "11:22:33:44:55:66"})
	if err != nil {
		t.Fatalf("client.Connect failed: %v", err)
	}
	if connResp.GetValue() != "11:22:33:44:55:66" || impl.lastConnectAddr != "11:22:33:44:55:66" {
		t.Fatalf("unexpected Connect result: resp=%q recorded=%q", connResp.GetValue(), impl.lastConnectAddr)
	}

	discResp, err := client.Disconnect(ctx, &Empty{})
	if err != nil {
		t.Fatalf("client.Disconnect failed: %v", err)
	}
	if discResp.GetValue() != "AA:BB:CC:DD:EE:FF" || !impl.disconnected {
		t.Fatalf("unexpected Disconnect result: resp=%q disconnected=%v", discResp.GetValue(), impl.disconnected)
	}

	_ = dconn.Close()
	cancel()
	<-srvDone
}
