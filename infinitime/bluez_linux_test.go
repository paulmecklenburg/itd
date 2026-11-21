//go:build linux

package infinitime

import (
	"testing"

	"github.com/godbus/dbus/v5"
)

func TestIsInfiniTimeProps(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]dbus.Variant
		want  bool
	}{
		{
			name:  "empty props",
			props: nil,
			want:  false,
		},
		{
			name: "by Name",
			props: map[string]dbus.Variant{
				"Name": dbus.MakeVariant("InfiniTime"),
			},
			want: true,
		},
		{
			name: "by Alias",
			props: map[string]dbus.Variant{
				"Name":  dbus.MakeVariant("OtherName"),
				"Alias": dbus.MakeVariant("InfiniTime"),
			},
			want: true,
		},
		{
			name: "by InfiniTime music service UUID with custom name",
			props: map[string]dbus.Variant{
				"Name":  dbus.MakeVariant("PineTime Watch 2"),
				"Alias": dbus.MakeVariant("PineTime Watch 2"),
				"UUIDs": dbus.MakeVariant([]string{
					"00001800-0000-1000-8000-00805f9b34fb",
					"00000000-78FC-48FE-8E23-433B3A1942D0",
				}),
			},
			want: true,
		},
		{
			name: "by Adafruit BLE FS service UUID",
			props: map[string]dbus.Variant{
				"Alias": dbus.MakeVariant("Custom PineTime"),
				"UUIDs": dbus.MakeVariant([]string{
					"adaf0100-4669-6c65-5472-616e73666572",
				}),
			},
			want: true,
		},
		{
			name: "unrelated BLE device",
			props: map[string]dbus.Variant{
				"Name":  dbus.MakeVariant("Sony WH-1000XM5"),
				"Alias": dbus.MakeVariant("Sony WH-1000XM5"),
				"UUIDs": dbus.MakeVariant([]string{
					"0000180f-0000-1000-8000-00805f9b34fb",
				}),
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isInfiniTimeProps(tc.props); got != tc.want {
				t.Fatalf("isInfiniTimeProps() = %v, want %v", got, tc.want)
			}
		})
	}
}
