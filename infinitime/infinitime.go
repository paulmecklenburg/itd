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

func reconnect(opts Options, adapter *bluetooth.Adapter, device *Device, mac string) {
	if device == nil {
		return
	}

	done := false
	for {
		var connectErr error
		err := adapter.Scan(func(a *bluetooth.Adapter, sr bluetooth.ScanResult) {
			if sr.Address.String() != mac {
				return
			}

			dev, err := a.Connect(sr.Address, bluetooth.ConnectionParams{})
			if err != nil {
				connectErr = err
				a.StopScan()
				return
			}
			a.StopScan()

			device.deviceMtx.Lock()
			device.device = dev
			device.charCache = make(map[btChar]*bluetooth.DeviceCharacteristic)
			device.fsNotifyEnabled.Store(false)
			device.deviceMtx.Unlock()

			device.notifierMtx.Lock()
			for char, notifier := range device.notifierMap {
				c, err := device.getChar(char)
				if err != nil {
					continue
				}

				err = c.EnableNotifications(nil)
				if err != nil {
					continue
				}

				err = c.EnableNotifications(notifier.notify)
				if err != nil {
					continue
				}
			}
			device.notifierMtx.Unlock()

			device.reconnecting.Store(false)
			done = true
		})

		if done {
			return
		}

		if err != nil || connectErr != nil {
			time.Sleep(5 * time.Second)
		} else {
			time.Sleep(opts.ScanInterval)
		}
	}
}

func Connect(opts Options) (device *Device, err error) {
	adapter := bluetooth.DefaultAdapter

	if opts.ScanInterval == 0 {
		opts.ScanInterval = 2 * time.Minute
	}

	var mac string
	adapter.SetConnectHandler(func(dev bluetooth.Device, connected bool) {
		if mac == "" || dev.Address.String() != mac {
			return
		}

		if connected {
			if opts.OnReconnect != nil {
				opts.OnReconnect(device)
			}
		} else {
			if opts.OnDisconnect != nil {
				opts.OnDisconnect(device)
			}
			if device.reconnecting.CompareAndSwap(false, true) {
				go reconnect(opts, adapter, device, mac)
			}
		}
	})

	err = adapter.Enable()
	if err != nil {
		return nil, err
	}

	var scanErr error
	err = adapter.Scan(func(a *bluetooth.Adapter, sr bluetooth.ScanResult) {
		if sr.LocalName() != "InfiniTime" {
			return
		}

		dev, err := a.Connect(sr.Address, bluetooth.ConnectionParams{})
		if err != nil {
			scanErr = err
			adapter.StopScan()
			return
		}
		mac = dev.Address.String()

		device = &Device{
			adapter:     a,
			device:      dev,
			charCache:   map[btChar]*bluetooth.DeviceCharacteristic{},
			notifierMap: map[btChar]notifier{},
			fsNotifyCh:  make(chan []byte, 256),
		}
		if opts.OnConnect != nil {
			opts.OnConnect(device)
		}
		adapter.StopScan()
	})
	if err != nil {
		return nil, err
	}

	if scanErr != nil {
		return nil, scanErr
	}

	return device, nil
}

// Device represents an InfiniTime device
type Device struct {
	adapter *bluetooth.Adapter

	deviceMtx sync.Mutex
	device    bluetooth.Device
	updating  atomic.Bool
	reconnecting atomic.Bool

	charCache map[btChar]*bluetooth.DeviceCharacteristic

	fsNotifyCh      chan []byte
	fsNotifyEnabled atomic.Bool

	notifierMtx sync.Mutex
	notifierMap map[btChar]notifier
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
