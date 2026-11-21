//go:build !linux

package infinitime

import "tinygo.org/x/bluetooth"

func isBlueZDeviceConnected(adapterID, mac string) bool {
	return true
}

func isKnownBlueZInfiniTime(adapterID, mac string) bool {
	return false
}

func shouldAttemptConnect(adapterID, mac string, rssi int16) bool {
	return true
}

func disconnectBlueZDevice(adapterID, mac string) {}

func findConnectedBlueZInfiniTime(adapterID string, accept func(mac string) bool, avoidMAC string) (bluetooth.Address, bool) {
	return bluetooth.Address{}, false
}

func watchBlueZConnections(adapterID string, onEvent func(addr bluetooth.Address, mac string, connected bool, isInfiniTime bool)) func() {
	return func() {}
}

func stopBlueZNotifies(adapterID, mac string, chars ...btChar) {}
