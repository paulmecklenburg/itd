package infinitime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"tinygo.org/x/bluetooth"
)

type Options struct {
	Allowlist    []string
	Blocklist    []string
	ScanInterval time.Duration

	OnDisconnect func(dev *Device)
	OnReconnect  func(dev *Device)
	OnConnect    func(dev *Device)
}

func closeChanIfOpen(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func reconnect(opts Options, adapter *bluetooth.Adapter, device *Device, mac string) {
	if device == nil {
		return
	}

	// Allow BlueZ to finish tearing down the previous connection before scanning.
	time.Sleep(1 * time.Second)

	for {
		var (
			found      bool
			targetAddr bluetooth.Address
			stopOnce   sync.Once
		)
		stopScan := func() {
			stopOnce.Do(func() {
				_ = adapter.StopScan()
			})
		}

		scanDone := make(chan struct{})
		go func() {
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				stopScan()
			case <-scanDone:
			}
		}()

		scanErr := adapter.Scan(func(a *bluetooth.Adapter, sr bluetooth.ScanResult) {
			if found || sr.Address.String() != mac {
				return
			}
			if !shouldAttemptConnect(mac, sr.RSSI) {
				return
			}

			targetAddr = sr.Address
			found = true
			stopScan()
		})
		close(scanDone)

		if scanErr != nil {
			// Ensure adapter.scanCancelChan is cleared so future Scan calls don't fail with errScanning.
			stopScan()
			time.Sleep(5 * time.Second)
			continue
		}

		if !found {
			time.Sleep(1 * time.Second)
			continue
		}

		device.connStateMtx.Lock()
		device.isConnected = true
		device.disconnectCh = make(chan struct{})
		device.connStateMtx.Unlock()

		type connectResult struct {
			dev bluetooth.Device
			err error
		}
		connectCh := make(chan connectResult, 1)
		go func() {
			dev, err := adapter.Connect(targetAddr, bluetooth.ConnectionParams{})
			connectCh <- connectResult{dev: dev, err: err}
		}()

		var (
			dev        bluetooth.Device
			connectErr error
		)
		connectTimer := time.NewTimer(35 * time.Second)
		select {
		case res := <-connectCh:
			connectTimer.Stop()
			dev, connectErr = res.dev, res.err
		case <-connectTimer.C:
			disconnectBlueZDevice(mac)
			connectErr = fmt.Errorf("connect timeout")
		}

		if connectErr != nil {
			device.connStateMtx.Lock()
			device.isConnected = false
			closeChanIfOpen(device.disconnectCh)
			device.connStateMtx.Unlock()
			time.Sleep(5 * time.Second)
			continue
		}

		// Wait for BlueZ ServicesResolved before accessing GATT characteristics.
		if _, err := dev.DiscoverServices(nil); err != nil {
			_ = dev.Disconnect()
			device.connStateMtx.Lock()
			device.isConnected = false
			closeChanIfOpen(device.disconnectCh)
			device.connStateMtx.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}

		wasFSNotify := device.fsNotifyEnabled.Swap(false)

		device.deviceMtx.Lock()
		device.device = dev
		device.charCache = make(map[btChar]*bluetooth.DeviceCharacteristic)
		device.deviceMtx.Unlock()

		var charsToReset []btChar
		if wasFSNotify {
			charsToReset = append(charsToReset, fsTransferChar)
		}
		device.notifierMtx.Lock()
		for char := range device.notifierMap {
			charsToReset = append(charsToReset, char)
		}
		device.notifierMtx.Unlock()
		stopBlueZNotifies(mac, charsToReset...)

		device.notifierMtx.Lock()
		for char, notifier := range device.notifierMap {
			c, err := device.getChar(char)
			if err != nil {
				continue
			}

			if err = device.enableNotifications(char, c, notifier.notify); err != nil {
				continue
			}

			notifier.setChar(c)
		}
		device.notifierMtx.Unlock()

		bluezConnected := isBlueZDeviceConnected(mac)
		device.connStateMtx.Lock()
		stillConnected := device.isConnected && bluezConnected
		if !stillConnected {
			device.isConnected = false
			closeChanIfOpen(device.disconnectCh)
			device.isReconnecting = true
			device.connStateMtx.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}
		device.isReconnecting = false
		device.connStateMtx.Unlock()

		if opts.OnReconnect != nil {
			opts.OnReconnect(device)
		}
		return
	}
}

func Connect(opts Options) (device *Device, err error) {
	adapter := bluetooth.DefaultAdapter

	if opts.ScanInterval == 0 {
		opts.ScanInterval = 2 * time.Minute
	}

	var (
		macMtx sync.Mutex
		mac    string
	)
	adapter.SetConnectHandler(func(dev bluetooth.Device, connected bool) {
		macMtx.Lock()
		curMac := mac
		curDevice := device
		macMtx.Unlock()

		if curDevice == nil || curMac == "" || dev.Address.String() != curMac {
			return
		}

		if connected {
			curDevice.connStateMtx.Lock()
			curDevice.isConnected = true
			curDevice.connStateMtx.Unlock()
			return
		}

		curDevice.connStateMtx.Lock()
		wasConnected := curDevice.isConnected
		curDevice.isConnected = false
		closeChanIfOpen(curDevice.disconnectCh)
		shouldReconnect := !curDevice.isReconnecting
		if shouldReconnect {
			curDevice.isReconnecting = true
		}
		curDevice.connStateMtx.Unlock()

		if wasConnected && opts.OnDisconnect != nil {
			go opts.OnDisconnect(curDevice)
		}
		if shouldReconnect {
			go reconnect(opts, adapter, curDevice, curMac)
		}
	})

	err = adapter.Enable()
	if err != nil {
		return nil, err
	}

	var (
		found      bool
		targetAddr bluetooth.Address
		stopOnce   sync.Once
	)
	stopScan := func() {
		stopOnce.Do(func() {
			_ = adapter.StopScan()
		})
	}

	err = adapter.Scan(func(a *bluetooth.Adapter, sr bluetooth.ScanResult) {
		if found || sr.LocalName() != "InfiniTime" {
			return
		}

		targetAddr = sr.Address
		found = true
		stopScan()
	})
	if err != nil {
		stopScan()
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("no InfiniTime device found")
	}

	targetMac := targetAddr.String()
	macMtx.Lock()
	mac = targetMac
	device = &Device{
		adapter:      adapter,
		mac:          targetMac,
		isConnected:  true,
		disconnectCh: make(chan struct{}),
		charCache:    map[btChar]*bluetooth.DeviceCharacteristic{},
		notifierMap:  map[btChar]notifier{},
		fsNotifyCh:   make(chan []byte, 256),
	}
	macMtx.Unlock()

	dev, err := adapter.Connect(targetAddr, bluetooth.ConnectionParams{})
	if err != nil {
		macMtx.Lock()
		mac = ""
		macMtx.Unlock()
		return nil, err
	}

	device.deviceMtx.Lock()
	device.device = dev
	device.deviceMtx.Unlock()

	if opts.OnConnect != nil {
		opts.OnConnect(device)
	}

	return device, nil
}

// Device represents an InfiniTime device
type Device struct {
	adapter *bluetooth.Adapter
	mac     string

	connStateMtx   sync.Mutex
	isConnected    bool
	isReconnecting bool
	disconnectCh   chan struct{}

	deviceMtx sync.Mutex
	device    bluetooth.Device
	updating  atomic.Bool

	charCache map[btChar]*bluetooth.DeviceCharacteristic

	fsNotifyMtx     sync.Mutex
	fsNotifyCh      chan []byte
	fsNotifyEnabled atomic.Bool

	notifierMtx sync.Mutex
	notifierMap map[btChar]notifier
}

// IsConnected reports whether the PineTime is currently connected.
func (d *Device) IsConnected() bool {
	d.connStateMtx.Lock()
	defer d.connStateMtx.Unlock()
	return d.isConnected
}

func (d *Device) getDisconnectCh() <-chan struct{} {
	d.connStateMtx.Lock()
	defer d.connStateMtx.Unlock()
	if !d.isConnected || d.disconnectCh == nil {
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh
	}
	return d.disconnectCh
}

func (d *Device) evictChar(ch btChar) {
	d.deviceMtx.Lock()
	delete(d.charCache, ch)
	d.deviceMtx.Unlock()
}

func (d *Device) enableNotifications(ch btChar, c *bluetooth.DeviceCharacteristic, cb func([]byte)) error {
	err := c.EnableNotifications(cb)
	if err == nil {
		return nil
	}

	// Clean up partial notification channel state left by tinygo/bluetooth on error,
	// then clear any stale BlueZ StartNotify session and retry once.
	_ = c.EnableNotifications(nil)
	stopBlueZNotifies(d.Address(), ch)
	time.Sleep(100 * time.Millisecond)

	err = c.EnableNotifications(cb)
	if err != nil {
		_ = c.EnableNotifications(nil)
		d.evictChar(ch)
		return err
	}

	return nil
}

func (d *Device) disableNotifications(ch btChar, c *bluetooth.DeviceCharacteristic) {
	if c != nil {
		_ = c.EnableNotifications(nil)
	}
	stopBlueZNotifies(d.Address(), ch)
}

// FS returns a handle for InifniTime's filesystem'
func (d *Device) FS() *FS {
	return &FS{
		dev: d,
	}
}

func (d *Device) getChar(c btChar) (*bluetooth.DeviceCharacteristic, error) {
	if d.updating.Load() {
		return nil, fmt.Errorf("device is currently updating")
	}

	if !d.IsConnected() {
		return nil, fmt.Errorf("device is not connected")
	}

	d.deviceMtx.Lock()
	defer d.deviceMtx.Unlock()

	if d.charCache == nil {
		d.charCache = make(map[btChar]*bluetooth.DeviceCharacteristic)
	}

	if char, ok := d.charCache[c]; ok && char != nil {
		return char, nil
	}

	services, err := d.device.DiscoverServices([]bluetooth.UUID{c.ServiceID})
	if err != nil || len(services) == 0 {
		return nil, fmt.Errorf("characteristic %s (%s) not found", c.ID, c.Name)
	}

	chars, err := services[0].DiscoverCharacteristics([]bluetooth.UUID{c.ID})
	if err != nil || len(chars) == 0 {
		return nil, fmt.Errorf("characteristic %s (%s) not found", c.ID, c.Name)
	}

	d.charCache[c] = chars[0]
	return chars[0], err
}
