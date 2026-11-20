//go:build linux

package infinitime

import (
	"strings"

	"github.com/godbus/dbus/v5"
	"tinygo.org/x/bluetooth"
)

func bluezDevicePath(mac string) dbus.ObjectPath {
	return dbus.ObjectPath("/org/bluez/hci0/dev_" + strings.ReplaceAll(mac, ":", "_"))
}

func isBlueZDeviceConnected(mac string) bool {
	if mac == "" {
		return false
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return false
	}

	prop, err := bus.Object("org.bluez", bluezDevicePath(mac)).GetProperty("org.bluez.Device1.Connected")
	if err != nil {
		return false
	}

	connected, ok := prop.Value().(bool)
	return ok && connected
}

func shouldAttemptConnect(mac string, rssi int16) bool {
	return rssi != 0 || isBlueZDeviceConnected(mac)
}

func disconnectBlueZDevice(mac string) {
	if mac == "" {
		return
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return
	}

	_ = bus.Object("org.bluez", bluezDevicePath(mac)).Call("org.bluez.Device1.Disconnect", 0).Err
}

// stopBlueZNotifies clears any lingering BlueZ StartNotify sessions for the
// given characteristics so subsequent EnableNotifications calls do not fail
// with "Already notifying" across reconnects.
func stopBlueZNotifies(mac string, chars ...btChar) {
	if mac == "" || len(chars) == 0 {
		return
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return
	}

	var objects map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	err = bus.Object("org.bluez", dbus.ObjectPath("/")).Call("org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0).Store(&objects)
	if err != nil {
		return
	}

	targetServices := make(map[bluetooth.UUID]struct{}, len(chars))
	type charKey struct {
		service bluetooth.UUID
		char    bluetooth.UUID
	}
	targetChars := make(map[charKey]struct{}, len(chars))
	for _, ch := range chars {
		targetServices[ch.ServiceID] = struct{}{}
		targetChars[charKey{service: ch.ServiceID, char: ch.ID}] = struct{}{}
	}

	servicePrefix := string(bluezDevicePath(mac)) + "/service"
	servicePaths := make(map[string]bluetooth.UUID)
	for objPath, ifaces := range objects {
		pathStr := string(objPath)
		if !strings.HasPrefix(pathStr, servicePrefix) {
			continue
		}
		svcProps, ok := ifaces["org.bluez.GattService1"]
		if !ok {
			continue
		}
		uuidVar, ok := svcProps["UUID"]
		if !ok {
			continue
		}
		uuidStr, ok := uuidVar.Value().(string)
		if !ok {
			continue
		}
		svcUUID, err := bluetooth.ParseUUID(uuidStr)
		if err != nil {
			continue
		}
		if _, wanted := targetServices[svcUUID]; wanted {
			servicePaths[pathStr] = svcUUID
		}
	}

	if len(servicePaths) == 0 {
		return
	}

	for objPath, ifaces := range objects {
		charProps, ok := ifaces["org.bluez.GattCharacteristic1"]
		if !ok {
			continue
		}
		pathStr := string(objPath)
		var matchedSvc bluetooth.UUID
		var foundSvc bool
		for svcPath, svcUUID := range servicePaths {
			if strings.HasPrefix(pathStr, svcPath+"/char") {
				matchedSvc = svcUUID
				foundSvc = true
				break
			}
		}
		if !foundSvc {
			continue
		}

		uuidVar, ok := charProps["UUID"]
		if !ok {
			continue
		}
		uuidStr, ok := uuidVar.Value().(string)
		if !ok {
			continue
		}
		charUUID, err := bluetooth.ParseUUID(uuidStr)
		if err != nil {
			continue
		}

		if _, wanted := targetChars[charKey{service: matchedSvc, char: charUUID}]; wanted {
			_ = bus.Object("org.bluez", objPath).Call("org.bluez.GattCharacteristic1.StopNotify", 0).Err
		}
	}
}
