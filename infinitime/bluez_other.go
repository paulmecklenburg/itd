//go:build !linux

package infinitime

func isBlueZDeviceConnected(mac string) bool {
	return true
}

func shouldAttemptConnect(mac string, rssi int16) bool {
	return true
}

func disconnectBlueZDevice(mac string) {}

func stopBlueZNotifies(mac string, chars ...btChar) {}
