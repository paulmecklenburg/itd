//go:build linux

package infinitime

import (
	"sort"
	"strings"
	"sync"

	"github.com/godbus/dbus/v5"
	"tinygo.org/x/bluetooth"
)

var infiniTimeUUIDSet = map[string]struct{}{
	"00000000-78fc-48fe-8e23-433b3a1942d0": {}, // Music service
	"00010000-78fc-48fe-8e23-433b3a1942d0": {}, // Navigation service
	"00030000-78fc-48fe-8e23-433b3a1942d0": {}, // Motion service
	"00050000-78fc-48fe-8e23-433b3a1942d0": {}, // Weather service
	"adaf0100-4669-6c65-5472-616e73666572": {}, // Adafruit BLE FS service
}

func bluezDevicePrefix(adapterID string) string {
	return "/org/bluez/" + normAdapterID(adapterID) + "/dev_"
}

func bluezDevicePath(adapterID, mac string) dbus.ObjectPath {
	if norm, err := NormalizeMAC(mac); err == nil {
		mac = norm
	} else {
		mac = strings.ToUpper(mac)
	}
	return dbus.ObjectPath(bluezDevicePrefix(adapterID) + strings.ReplaceAll(mac, ":", "_"))
}

func macFromBlueZDevicePath(adapterID, objPath string) (string, bool) {
	prefix := bluezDevicePrefix(adapterID)
	if !strings.HasPrefix(objPath, prefix) {
		return "", false
	}
	rest := objPath[len(prefix):]
	if rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	norm, err := NormalizeMAC(strings.ReplaceAll(rest, "_", ":"))
	if err != nil {
		return "", false
	}
	return norm, true
}

func isInfiniTimeProps(props map[string]dbus.Variant) bool {
	if len(props) == 0 {
		return false
	}

	for _, key := range []string{"Name", "Alias"} {
		if v, ok := props[key]; ok {
			if s, ok := v.Value().(string); ok && s == "InfiniTime" {
				return true
			}
		}
	}

	if v, ok := props["UUIDs"]; ok {
		if uuids, ok := v.Value().([]string); ok {
			for _, u := range uuids {
				if _, match := infiniTimeUUIDSet[strings.ToLower(strings.TrimSpace(u))]; match {
					return true
				}
			}
		}
	}

	return false
}

func isBlueZDeviceConnected(adapterID, mac string) bool {
	if mac == "" {
		return false
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return false
	}

	prop, err := bus.Object("org.bluez", bluezDevicePath(adapterID, mac)).GetProperty("org.bluez.Device1.Connected")
	if err != nil {
		return false
	}

	connected, ok := prop.Value().(bool)
	return ok && connected
}

func isKnownBlueZInfiniTime(adapterID, mac string) bool {
	if mac == "" {
		return false
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return false
	}

	var props map[string]dbus.Variant
	err = bus.Object("org.bluez", bluezDevicePath(adapterID, mac)).Call("org.freedesktop.DBus.Properties.GetAll", 0, "org.bluez.Device1").Store(&props)
	if err != nil {
		return false
	}

	return isInfiniTimeProps(props)
}

func shouldAttemptConnect(adapterID, mac string, rssi int16) bool {
	return rssi != 0 || isBlueZDeviceConnected(adapterID, mac)
}

func disconnectBlueZDevice(adapterID, mac string) {
	if mac == "" {
		return
	}

	bus, err := dbus.SystemBus()
	if err != nil {
		return
	}

	_ = bus.Object("org.bluez", bluezDevicePath(adapterID, mac)).Call("org.bluez.Device1.Disconnect", 0).Err
}

func findConnectedBlueZInfiniTime(adapterID string, accept func(mac string) bool, avoidMAC string) (bluetooth.Address, bool) {
	bus, err := dbus.SystemBus()
	if err != nil {
		return bluetooth.Address{}, false
	}

	var objects map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	err = bus.Object("org.bluez", dbus.ObjectPath("/")).Call("org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0).Store(&objects)
	if err != nil {
		return bluetooth.Address{}, false
	}

	var candidates []string
	for objPath, ifaces := range objects {
		pathMAC, isDev := macFromBlueZDevicePath(adapterID, string(objPath))
		if !isDev {
			continue
		}
		devProps, ok := ifaces["org.bluez.Device1"]
		if !ok {
			continue
		}
		connVar, ok := devProps["Connected"]
		if !ok {
			continue
		}
		connected, ok := connVar.Value().(bool)
		if !ok || !connected {
			continue
		}
		if !isInfiniTimeProps(devProps) {
			continue
		}

		mac := pathMAC
		if addrVar, ok := devProps["Address"]; ok {
			if raw, ok := addrVar.Value().(string); ok {
				if norm, err := NormalizeMAC(raw); err == nil {
					mac = norm
				}
			}
		}

		if accept != nil && !accept(mac) {
			continue
		}
		candidates = append(candidates, mac)
	}

	if len(candidates) == 0 {
		return bluetooth.Address{}, false
	}

	sort.Strings(candidates)
	chosen := candidates[0]
	if avoidMAC != "" {
		for _, c := range candidates {
			if !strings.EqualFold(c, avoidMAC) {
				chosen = c
				break
			}
		}
	}

	addr, err := addressFromMAC(chosen)
	if err != nil {
		return bluetooth.Address{}, false
	}
	return addr, true
}

// watchBlueZConnections listens on a private D-Bus connection for BlueZ Device1
// connect/disconnect and service resolution events on the configured adapter.
func watchBlueZConnections(adapterID string, onEvent func(addr bluetooth.Address, mac string, connected bool, isInfiniTime bool)) func() {
	if onEvent == nil {
		return func() {}
	}

	conn, err := dbus.SystemBusPrivate()
	if err != nil {
		return func() {}
	}
	if err := conn.Auth(nil); err != nil {
		_ = conn.Close()
		return func() {}
	}
	if err := conn.Hello(); err != nil {
		_ = conn.Close()
		return func() {}
	}

	if err := conn.AddMatchSignal(
		dbus.WithMatchSender("org.bluez"),
		dbus.WithMatchInterface("org.freedesktop.DBus.Properties"),
		dbus.WithMatchMember("PropertiesChanged"),
	); err != nil {
		_ = conn.Close()
		return func() {}
	}

	if err := conn.AddMatchSignal(
		dbus.WithMatchSender("org.bluez"),
		dbus.WithMatchInterface("org.freedesktop.DBus.ObjectManager"),
		dbus.WithMatchMember("InterfacesAdded"),
	); err != nil {
		_ = conn.Close()
		return func() {}
	}

	sigCh := make(chan *dbus.Signal, 32)
	conn.Signal(sigCh)

	stopCh := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}

	go func() {
		defer conn.Close()
		defer conn.RemoveSignal(sigCh)

		for {
			select {
			case <-stopCh:
				return
			case sig, ok := <-sigCh:
				if !ok || sig == nil {
					return
				}

				switch sig.Name {
				case "org.freedesktop.DBus.ObjectManager.InterfacesAdded":
					if len(sig.Body) < 2 {
						continue
					}
					objPath, ok := sig.Body[0].(dbus.ObjectPath)
					if !ok {
						continue
					}
					pathMAC, isDev := macFromBlueZDevicePath(adapterID, string(objPath))
					if !isDev {
						continue
					}
					ifaces, ok := sig.Body[1].(map[string]map[string]dbus.Variant)
					if !ok {
						continue
					}
					devProps, ok := ifaces["org.bluez.Device1"]
					if !ok {
						continue
					}
					connVar, ok := devProps["Connected"]
					if !ok {
						continue
					}
					connected, ok := connVar.Value().(bool)
					if !ok || !connected || !isInfiniTimeProps(devProps) {
						continue
					}

					mac := pathMAC
					if addrVar, ok := devProps["Address"]; ok {
						if raw, ok := addrVar.Value().(string); ok {
							if norm, err := NormalizeMAC(raw); err == nil {
								mac = norm
							}
						}
					}
					if addr, err := addressFromMAC(mac); err == nil {
						onEvent(addr, mac, true, true)
					}

				case "org.freedesktop.DBus.Properties.PropertiesChanged":
					if len(sig.Body) < 2 {
						continue
					}
					pathMAC, isDev := macFromBlueZDevicePath(adapterID, string(sig.Path))
					if !isDev {
						continue
					}
					iface, ok := sig.Body[0].(string)
					if !ok || iface != "org.bluez.Device1" {
						continue
					}
					changes, ok := sig.Body[1].(map[string]dbus.Variant)
					if !ok {
						continue
					}

					connVar, hasConn := changes["Connected"]
					resVar, hasResolved := changes["ServicesResolved"]
					if !hasConn && !hasResolved {
						continue
					}

					var allProps map[string]dbus.Variant
					_ = conn.Object("org.bluez", sig.Path).Call("org.freedesktop.DBus.Properties.GetAll", 0, "org.bluez.Device1").Store(&allProps)

					mac := pathMAC
					if addrVar, ok := allProps["Address"]; ok {
						if raw, ok := addrVar.Value().(string); ok {
							if norm, err := NormalizeMAC(raw); err == nil {
								mac = norm
							}
						}
					}
					addr, err := addressFromMAC(mac)
					if err != nil {
						continue
					}
					isIT := isInfiniTimeProps(allProps)

					if hasConn {
						if connChanged, ok := connVar.Value().(bool); ok {
							if !connChanged {
								onEvent(addr, mac, false, isIT)
							} else if isIT {
								onEvent(addr, mac, true, true)
							}
						}
						continue
					}

					if resolved, ok := resVar.Value().(bool); ok && resolved && isIT {
						if curConnVar, ok := allProps["Connected"]; ok {
							if curConn, ok := curConnVar.Value().(bool); ok && curConn {
								onEvent(addr, mac, true, true)
							}
						}
					}
				}
			}
		}
	}()

	return stop
}

// stopBlueZNotifies clears any lingering BlueZ StartNotify sessions for the
// given characteristics so subsequent EnableNotifications calls do not fail
// with "Already notifying" across reconnects.
func stopBlueZNotifies(adapterID, mac string, chars ...btChar) {
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

	servicePrefix := string(bluezDevicePath(adapterID, mac)) + "/service"
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
