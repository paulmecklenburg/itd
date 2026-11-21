package infinitime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tinygo.org/x/bluetooth"
)

const prevMACGracePeriod = 8 * time.Second

// Options configures InfiniTime connection, device filtering, and lifecycle callbacks.
type Options struct {
	Adapter      string
	Allowlist    []string
	Blocklist    []string
	ScanInterval time.Duration

	AutoReconnect func() bool
	AllowFunc     func(mac string) bool

	OnDisconnect func(dev *Device)
	OnReconnect  func(dev *Device)
	OnConnect    func(dev *Device)
}

func normAdapterID(adapterID string) string {
	if adapterID == "" {
		return "hci0"
	}
	return adapterID
}

// NormalizeMAC validates and normalizes a 6-byte Bluetooth MAC address into
// uppercase colon-separated form (e.g. "AA:BB:CC:DD:EE:FF").
func NormalizeMAC(s string) (string, error) {
	trimmed := strings.TrimSpace(s)
	hw, err := net.ParseMAC(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid MAC address %q: %w", s, err)
	}
	if len(hw) != 6 {
		return "", fmt.Errorf("invalid BLE MAC address %q: expected 6 bytes, got %d", s, len(hw))
	}
	return strings.ToUpper(hw.String()), nil
}

func addressFromMAC(mac string) (bluetooth.Address, error) {
	norm, err := NormalizeMAC(mac)
	if err != nil {
		return bluetooth.Address{}, err
	}
	parsed, err := bluetooth.ParseMAC(norm)
	if err != nil {
		return bluetooth.Address{}, err
	}
	return bluetooth.Address{
		MACAddress: bluetooth.MACAddress{MAC: parsed},
	}, nil
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

func matchesMACList(list []string, mac string) bool {
	normTarget, err := NormalizeMAC(mac)
	if err != nil {
		normTarget = strings.ToUpper(strings.TrimSpace(mac))
	}
	for _, entry := range list {
		normEntry, err := NormalizeMAC(entry)
		if err != nil {
			normEntry = strings.ToUpper(strings.TrimSpace(entry))
		}
		if normEntry != "" && strings.EqualFold(normEntry, normTarget) {
			return true
		}
	}
	return false
}

func safeScan(ctx context.Context, scanMtx *sync.Mutex, adapter *bluetooth.Adapter, timeout time.Duration, cb func(*bluetooth.Adapter, bluetooth.ScanResult) bool) (err error) {
	scanMtx.Lock()
	defer scanMtx.Unlock()

	var (
		found    bool
		stopOnce sync.Once
	)
	stopScan := func() {
		stopOnce.Do(func() {
			_ = adapter.StopScan()
		})
	}

	scanDone := make(chan struct{})
	defer close(scanDone)

	go func() {
		var timerCh <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timerCh = timer.C
		}
		select {
		case <-timerCh:
			stopScan()
		case <-ctx.Done():
			stopScan()
		case <-scanDone:
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			stopScan()
			err = fmt.Errorf("bluetooth scan panic: %v", r)
		}
	}()

	scanErr := adapter.Scan(func(a *bluetooth.Adapter, sr bluetooth.ScanResult) {
		if found {
			return
		}
		if cb(a, sr) {
			found = true
			stopScan()
		}
	})
	if scanErr != nil {
		stopScan()
		return scanErr
	}
	return nil
}

// Connect discovers and connects to an InfiniTime watch, configuring automatic
// reconnection and in-place BlueZ device switching.
func Connect(opts Options) (*Device, error) {
	adapter := bluetooth.DefaultAdapter
	adapterID := normAdapterID(opts.Adapter)

	if opts.ScanInterval == 0 {
		opts.ScanInterval = 2 * time.Minute
	}

	device := &Device{
		opts:         opts,
		adapter:      adapter,
		adapterID:    adapterID,
		disconnectCh: make(chan struct{}),
		charCache:    map[btChar]*bluetooth.DeviceCharacteristic{},
		notifierMap:  map[btChar]notifier{},
		fsNotifyCh:   make(chan []byte, 256),
	}

	adapter.SetConnectHandler(func(dev bluetooth.Device, connected bool) {
		device.handleConnectionChange(dev.Address, dev.Address.String(), connected, true, false)
	})

	if err := adapter.Enable(); err != nil {
		return nil, err
	}

	device.stopBlueZWatch = watchBlueZConnections(adapterID, func(addr bluetooth.Address, mac string, connected bool, isInfiniTime bool) {
		device.handleConnectionChange(addr, mac, connected, isInfiniTime, true)
	})

	for {
		var targetAddr bluetooth.Address
		var found bool

		if addr, ok := findConnectedBlueZInfiniTime(adapterID, device.isAllowed, ""); ok {
			targetAddr = addr
			found = true
		}

		if !found {
			scanErr := safeScan(context.Background(), &device.scanMtx, adapter, 30*time.Second, func(a *bluetooth.Adapter, sr bluetooth.ScanResult) bool {
				srMAC := sr.Address.String()
				if norm, err := NormalizeMAC(srMAC); err == nil {
					srMAC = norm
				}
				if !device.isInfiniTimeScanResult(sr) || !device.isAllowed(srMAC) {
					return false
				}
				if !shouldAttemptConnect(adapterID, srMAC, sr.RSSI) {
					return false
				}
				targetAddr = sr.Address
				found = true
				return true
			})
			if scanErr != nil {
				errStr := scanErr.Error()
				if !strings.Contains(errStr, "bluetooth scan panic") && !strings.Contains(errStr, "InProgress") {
					device.Close()
					return nil, scanErr
				}
				time.Sleep(1 * time.Second)
				continue
			}
		}

		if !found {
			time.Sleep(1 * time.Second)
			continue
		}

		if err := device.connectAndInit(context.Background(), targetAddr, false); err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		device.started.Store(true)

		// Catch any disconnect that happened just before started became true.
		curMAC := device.Address()
		if curMAC != "" && !isBlueZDeviceConnected(adapterID, curMAC) {
			if addr, err := addressFromMAC(curMAC); err == nil {
				device.handleConnectionChange(addr, curMAC, false, true, false)
			}
		}

		if opts.OnConnect != nil {
			opts.OnConnect(device)
		}
		return device, nil
	}
}

// Device represents an InfiniTime device.
type Device struct {
	opts      Options
	adapter   *bluetooth.Adapter
	adapterID string

	// scanMtx serializes adapter.Scan calls so concurrent callers cannot race.
	scanMtx sync.Mutex

	// opMtx serializes connect, switch, and disconnect transitions.
	// Never acquire opMtx while holding any other Device or watcher mutex.
	opMtx sync.Mutex

	// connStateMtx is a leaf mutex protecting connection state metadata.
	// Never perform BLE/D-Bus I/O or acquire another mutex while holding it.
	connStateMtx        sync.Mutex
	mac                 string
	isConnected         bool
	connecting          bool
	manualConnecting    bool
	manualDisconnect    bool
	manualTargetMAC     string
	pendingExternalMAC  string
	lastDisconnectedMAC string
	lastDisconnectTime  time.Time
	ignoreExternalMAC   string
	ignoreExternalUntil time.Time
	dfuPinMAC           string
	dfuPinUntil         time.Time
	disconnectCh        chan struct{}
	reconnectCancel     context.CancelFunc
	reconnectDone       chan struct{}
	reconnecting        bool

	deviceMtx sync.Mutex
	device    bluetooth.Device
	updating  atomic.Bool

	charCache map[btChar]*bluetooth.DeviceCharacteristic

	fsNotifyMtx     sync.Mutex
	fsNotifyCh      chan []byte
	fsNotifyEnabled atomic.Bool
	fsCRCSupport    atomic.Int32

	notifierMtx sync.Mutex
	notifierMap map[btChar]notifier

	started        atomic.Bool
	stopBlueZWatch func()
}

// Close stops background reconnect and BlueZ connection monitoring.
func (d *Device) Close() {
	d.cancelReconnect()
	if d.stopBlueZWatch != nil {
		d.stopBlueZWatch()
	}
}

// IsConnected reports whether the PineTime is currently connected.
func (d *Device) IsConnected() bool {
	d.connStateMtx.Lock()
	defer d.connStateMtx.Unlock()
	return d.isConnected
}

// ConnectTo connects to an InfiniTime device, switching away from the currently
// connected watch if needed. If address is empty, it connects to any allowed watch.
func (d *Device) ConnectTo(ctx context.Context, address string) (string, error) {
	var targetMAC string
	if strings.TrimSpace(address) != "" {
		norm, err := NormalizeMAC(address)
		if err != nil {
			return "", err
		}
		if d.isBlocked(norm) {
			return "", fmt.Errorf("device %s is blocklisted", norm)
		}
		targetMAC = norm
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
	}

	d.connStateMtx.Lock()
	d.manualConnecting = true
	d.manualTargetMAC = targetMAC
	avoidMAC := d.lastDisconnectedMAC
	prevDisconnectAt := d.lastDisconnectTime
	d.connStateMtx.Unlock()

	switchedOrFailed := true
	defer func() {
		d.connStateMtx.Lock()
		d.manualConnecting = false
		d.manualTargetMAC = ""
		shouldRestart := switchedOrFailed && !d.isConnected && !d.manualDisconnect && d.shouldAutoReconnect()
		d.connStateMtx.Unlock()
		if shouldRestart {
			d.startReconnectLoop(false)
		}
	}()

	d.cancelReconnect()

	curMAC := d.Address()
	if d.IsConnected() && curMAC != "" && isBlueZDeviceConnected(d.adapterID, curMAC) {
		if targetMAC == "" || strings.EqualFold(curMAC, targetMAC) {
			d.connStateMtx.Lock()
			d.manualDisconnect = false
			d.connStateMtx.Unlock()
			switchedOrFailed = false
			return curMAC, nil
		}
	}

	d.connStateMtx.Lock()
	d.manualDisconnect = false
	d.connStateMtx.Unlock()

	firstPass := true
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		var targetAddr bluetooth.Address
		var found bool

		if targetMAC != "" {
			if isBlueZDeviceConnected(d.adapterID, targetMAC) {
				if addr, err := addressFromMAC(targetMAC); err == nil {
					targetAddr = addr
					found = true
				}
			}
		} else {
			avoid := ""
			if firstPass {
				avoid = avoidMAC
			}
			if addr, ok := findConnectedBlueZInfiniTime(d.adapterID, d.isAllowed, avoid); ok {
				targetAddr = addr
				found = true
			}
		}

		if !found {
			scanErr := safeScan(ctx, &d.scanMtx, d.adapter, 15*time.Second, func(a *bluetooth.Adapter, sr bluetooth.ScanResult) bool {
				srMAC := sr.Address.String()
				if norm, err := NormalizeMAC(srMAC); err == nil {
					srMAC = norm
				}
				isIT := true
				if targetMAC == "" {
					isIT = d.isInfiniTimeScanResult(sr)
				}
				if !d.shouldAcceptCandidate(srMAC, isIT, targetMAC, "", avoidMAC, prevDisconnectAt, avoidMAC != "", time.Now()) {
					return false
				}
				if !shouldAttemptConnect(d.adapterID, srMAC, sr.RSSI) {
					return false
				}
				targetAddr = sr.Address
				found = true
				return true
			})
			if scanErr != nil && ctx.Err() != nil {
				return "", ctx.Err()
			}
		}

		firstPass = false

		if found {
			if err := d.connectAndInit(ctx, targetAddr, true); err == nil {
				switchedOrFailed = false
				return d.Address(), nil
			} else if ctx.Err() != nil {
				return "", err
			}
		}

		retryTimer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			retryTimer.Stop()
			return "", ctx.Err()
		case <-retryTimer.C:
		}
	}
}

// Disconnect disconnects the currently connected InfiniTime watch and pauses
// automatic reconnection until ConnectTo or a manual BlueZ connection occurs.
func (d *Device) Disconnect() (string, error) {
	d.connStateMtx.Lock()
	d.manualDisconnect = true
	d.connStateMtx.Unlock()

	d.cancelReconnect()

	d.opMtx.Lock()
	defer d.opMtx.Unlock()

	d.connStateMtx.Lock()
	prevMac := d.mac
	wasConnected := d.isConnected
	d.isConnected = false
	if prevMac != "" {
		d.lastDisconnectedMAC = prevMac
		d.lastDisconnectTime = time.Now()
		d.ignoreExternalMAC = prevMac
		d.ignoreExternalUntil = time.Now().Add(2 * time.Second)
	}
	oldDiscCh := d.disconnectCh
	d.disconnectCh = nil
	d.connStateMtx.Unlock()

	closeChanIfOpen(oldDiscCh)

	if prevMac == "" {
		return "", nil
	}

	bluezWasConnected := isBlueZDeviceConnected(d.adapterID, prevMac)
	d.clearActiveNotifications(prevMac)
	disconnectBlueZDevice(d.adapterID, prevMac)

	if !wasConnected && !bluezWasConnected {
		return "", nil
	}
	return prevMac, nil
}

func (d *Device) shouldAutoReconnect() bool {
	if d.opts.AutoReconnect != nil {
		return d.opts.AutoReconnect()
	}
	return true
}

func (d *Device) isBlocked(mac string) bool {
	return matchesMACList(d.opts.Blocklist, mac)
}

func (d *Device) isAllowed(mac string) bool {
	if d.isBlocked(mac) {
		return false
	}
	if len(d.opts.Allowlist) > 0 && !matchesMACList(d.opts.Allowlist, mac) {
		return false
	}
	if d.opts.AllowFunc != nil && !d.opts.AllowFunc(mac) {
		return false
	}
	return true
}

func (d *Device) pinDFU(mac string, dur time.Duration) {
	if norm, err := NormalizeMAC(mac); err == nil {
		mac = norm
	}
	d.connStateMtx.Lock()
	d.dfuPinMAC = mac
	d.dfuPinUntil = time.Now().Add(dur)
	d.connStateMtx.Unlock()
}

func (d *Device) clearDFUPin() {
	d.connStateMtx.Lock()
	d.dfuPinMAC = ""
	d.dfuPinUntil = time.Time{}
	d.connStateMtx.Unlock()
}

func (d *Device) activeDFUPin() string {
	d.connStateMtx.Lock()
	defer d.connStateMtx.Unlock()
	if d.dfuPinMAC != "" && time.Now().Before(d.dfuPinUntil) {
		return d.dfuPinMAC
	}
	return ""
}

func (d *Device) shouldAcceptCandidate(
	srMAC string,
	isIT bool,
	targetMAC string,
	dfuPinMAC string,
	prevMAC string,
	prevDisconnectAt time.Time,
	applyPrevGrace bool,
	now time.Time,
) bool {
	if targetMAC != "" {
		return !d.isBlocked(srMAC) && strings.EqualFold(srMAC, targetMAC)
	}
	if dfuPinMAC != "" {
		return strings.EqualFold(srMAC, dfuPinMAC)
	}
	if !isIT || !d.isAllowed(srMAC) {
		return false
	}
	if applyPrevGrace && prevMAC != "" && strings.EqualFold(srMAC, prevMAC) && !prevDisconnectAt.IsZero() {
		if now.Sub(prevDisconnectAt) < prevMACGracePeriod {
			return false
		}
	}
	return true
}

func (d *Device) isInfiniTimeScanResult(sr bluetooth.ScanResult) bool {
	if sr.AdvertisementPayload != nil {
		if sr.LocalName() == "InfiniTime" {
			return true
		}
		for _, svcUUID := range []bluetooth.UUID{
			musicServiceUUID,
			navigationServiceUUID,
			motionServiceUUID,
			weatherServiceUUID,
			fsTransferChar.ServiceID,
		} {
			if sr.HasServiceUUID(svcUUID) {
				return true
			}
		}
	}
	return isKnownBlueZInfiniTime(d.adapterID, sr.Address.String())
}

func (d *Device) clearActiveNotifications(mac string) {
	var charsToReset []btChar
	if d.fsNotifyEnabled.Swap(false) {
		charsToReset = append(charsToReset, fsTransferChar)
	}

	var oldFSChar *bluetooth.DeviceCharacteristic
	d.deviceMtx.Lock()
	if c, ok := d.charCache[fsTransferChar]; ok && c != nil {
		oldFSChar = c
		delete(d.charCache, fsTransferChar)
	}
	d.deviceMtx.Unlock()
	if oldFSChar != nil {
		_ = oldFSChar.EnableNotifications(nil)
	}

	type charNotifier struct {
		ch btChar
		n  notifier
	}
	d.notifierMtx.Lock()
	items := make([]charNotifier, 0, len(d.notifierMap))
	for ch, n := range d.notifierMap {
		charsToReset = append(charsToReset, ch)
		items = append(items, charNotifier{ch: ch, n: n})
	}
	d.notifierMtx.Unlock()

	for _, item := range items {
		item.n.setChar(nil)
	}

	if mac != "" {
		stopBlueZNotifies(d.adapterID, mac, charsToReset...)
	}
}

func (d *Device) connectAndInit(ctx context.Context, targetAddr bluetooth.Address, fireReconnect bool) error {
	targetMac := targetAddr.String()
	if norm, err := NormalizeMAC(targetMac); err == nil {
		targetMac = norm
	}

	d.opMtx.Lock()
	opLocked := true
	defer func() {
		if opLocked {
			d.opMtx.Unlock()
		}
	}()

	d.connStateMtx.Lock()
	oldMac := d.mac
	d.connecting = true
	switching := oldMac != "" && !strings.EqualFold(oldMac, targetMac)
	if switching {
		d.isConnected = false
		oldDiscCh := d.disconnectCh
		d.disconnectCh = nil
		d.lastDisconnectedMAC = oldMac
		d.lastDisconnectTime = time.Now()
		d.ignoreExternalMAC = oldMac
		d.ignoreExternalUntil = time.Now().Add(2 * time.Second)
		d.connStateMtx.Unlock()
		closeChanIfOpen(oldDiscCh)
		d.clearActiveNotifications(oldMac)
		disconnectBlueZDevice(d.adapterID, oldMac)
	} else {
		d.connStateMtx.Unlock()
	}

	clearConnecting := true
	defer func() {
		if !clearConnecting {
			return
		}
		// Safety net for early error paths; cleared atomically on success below.
		d.connStateMtx.Lock()
		d.connecting = false
		d.connStateMtx.Unlock()
	}()

	d.connStateMtx.Lock()
	oldDiscCh := d.disconnectCh
	d.mac = targetMac
	d.isConnected = true
	d.disconnectCh = make(chan struct{})
	d.connStateMtx.Unlock()
	closeChanIfOpen(oldDiscCh)

	type connectResult struct {
		dev bluetooth.Device
		err error
	}
	connectCh := make(chan connectResult, 1)
	go func() {
		dev, err := d.adapter.Connect(targetAddr, bluetooth.ConnectionParams{})
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
		disconnectBlueZDevice(d.adapterID, targetMac)
		connectErr = fmt.Errorf("connect timeout")
	case <-ctx.Done():
		connectTimer.Stop()
		disconnectBlueZDevice(d.adapterID, targetMac)
		connectErr = ctx.Err()
	}

	if connectErr != nil {
		d.connStateMtx.Lock()
		d.isConnected = false
		closeChanIfOpen(d.disconnectCh)
		d.connStateMtx.Unlock()
		return connectErr
	}

	// Wait for BlueZ ServicesResolved before accessing GATT characteristics.
	if _, err := dev.DiscoverServices(nil); err != nil {
		_ = dev.Disconnect()
		d.connStateMtx.Lock()
		d.isConnected = false
		closeChanIfOpen(d.disconnectCh)
		d.connStateMtx.Unlock()
		return err
	}

	if ctx.Err() != nil {
		_ = dev.Disconnect()
		d.connStateMtx.Lock()
		d.isConnected = false
		closeChanIfOpen(d.disconnectCh)
		d.connStateMtx.Unlock()
		return ctx.Err()
	}

	wasFSNotify := d.fsNotifyEnabled.Swap(false)
	d.fsCRCSupport.Store(0)
	for len(d.fsNotifyCh) > 0 {
		<-d.fsNotifyCh
	}

	d.deviceMtx.Lock()
	if oldFSChar, ok := d.charCache[fsTransferChar]; ok && oldFSChar != nil {
		_ = oldFSChar.EnableNotifications(nil)
	}
	d.device = dev
	d.charCache = make(map[btChar]*bluetooth.DeviceCharacteristic)
	d.deviceMtx.Unlock()

	type charNotifier struct {
		ch btChar
		n  notifier
	}
	var charsToReset []btChar
	if wasFSNotify {
		charsToReset = append(charsToReset, fsTransferChar)
	}
	d.notifierMtx.Lock()
	items := make([]charNotifier, 0, len(d.notifierMap))
	for ch, n := range d.notifierMap {
		charsToReset = append(charsToReset, ch)
		items = append(items, charNotifier{ch: ch, n: n})
	}
	d.notifierMtx.Unlock()

	for _, item := range items {
		item.n.setChar(nil)
	}
	stopBlueZNotifies(d.adapterID, targetMac, charsToReset...)

	for _, item := range items {
		d.notifierMtx.Lock()
		stillRegistered := d.notifierMap[item.ch] == item.n
		d.notifierMtx.Unlock()
		if !stillRegistered {
			continue
		}

		c, err := d.getChar(item.ch)
		if err != nil {
			continue
		}
		if err := d.enableNotifications(item.ch, c, item.n.notify); err != nil {
			continue
		}

		var old *bluetooth.DeviceCharacteristic
		d.notifierMtx.Lock()
		stillRegistered = d.notifierMap[item.ch] == item.n
		if stillRegistered {
			old = item.n.swapChar(c)
		}
		d.notifierMtx.Unlock()

		if stillRegistered {
			if old != nil && old != c {
				_ = old.EnableNotifications(nil)
			}
		} else {
			d.disableNotifications(item.ch, c)
		}
	}

	bluezConnected := isBlueZDeviceConnected(d.adapterID, targetMac)
	d.connStateMtx.Lock()
	stillConnected := d.isConnected && bluezConnected && ctx.Err() == nil
	if !stillConnected {
		d.isConnected = false
		closeChanIfOpen(d.disconnectCh)
		d.connStateMtx.Unlock()
		if bluezConnected && ctx.Err() != nil {
			disconnectBlueZDevice(d.adapterID, targetMac)
		}
		return errors.New("device disconnected during initialization")
	}
	// Clear connecting atomically with verified connected state so no
	// disconnect signal in between is skipped.
	d.connecting = false
	clearConnecting = false
	d.manualDisconnect = false
	d.pendingExternalMAC = ""
	d.dfuPinMAC = ""
	d.dfuPinUntil = time.Time{}
	d.connStateMtx.Unlock()

	d.opMtx.Unlock()
	opLocked = false

	if fireReconnect && d.opts.OnReconnect != nil {
		d.opts.OnReconnect(d)
	}
	return nil
}

func (d *Device) startReconnectLoop(applyPrevGrace bool) {
	if !d.shouldAutoReconnect() {
		return
	}

	d.connStateMtx.Lock()
	if d.isConnected || d.reconnecting || d.manualDisconnect || d.manualConnecting || d.manualTargetMAC != "" || d.connecting || d.pendingExternalMAC != "" {
		d.connStateMtx.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	d.reconnecting = true
	d.reconnectCancel = cancel
	d.reconnectDone = done
	d.connStateMtx.Unlock()

	go func() {
		defer close(done)
		defer func() {
			cancel()
			d.connStateMtx.Lock()
			if d.reconnectDone == done {
				d.reconnecting = false
				d.reconnectCancel = nil
				d.reconnectDone = nil
			}
			d.connStateMtx.Unlock()
		}()
		d.reconnectLoop(ctx, applyPrevGrace)
	}()
}

func (d *Device) cancelReconnect() {
	d.connStateMtx.Lock()
	cancel := d.reconnectCancel
	done := d.reconnectDone
	d.connStateMtx.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (d *Device) reconnectLoop(ctx context.Context, applyPrevGrace bool) {
	// Allow BlueZ to finish tearing down the previous connection before scanning.
	initTimer := time.NewTimer(1 * time.Second)
	select {
	case <-initTimer.C:
	case <-ctx.Done():
		initTimer.Stop()
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		d.connStateMtx.Lock()
		stopLoop := d.manualDisconnect || !d.shouldAutoReconnect()
		prevMAC := d.lastDisconnectedMAC
		prevDisconnectAt := d.lastDisconnectTime
		ignoreMAC := d.ignoreExternalMAC
		ignoreUntil := d.ignoreExternalUntil
		d.connStateMtx.Unlock()

		if stopLoop {
			return
		}

		dfuPinMAC := d.activeDFUPin()

		var (
			found      bool
			targetAddr bluetooth.Address
		)

		// Prefer an already-connected InfiniTime watch in BlueZ before active scanning.
		if addr, ok := findConnectedBlueZInfiniTime(d.adapterID, func(mac string) bool {
			if ignoreMAC != "" && strings.EqualFold(mac, ignoreMAC) && time.Now().Before(ignoreUntil) {
				return false
			}
			return d.shouldAcceptCandidate(mac, true, "", dfuPinMAC, prevMAC, prevDisconnectAt, false, time.Now())
		}, prevMAC); ok {
			targetAddr = addr
			found = true
		}

		if !found {
			scanErr := safeScan(ctx, &d.scanMtx, d.adapter, 30*time.Second, func(a *bluetooth.Adapter, sr bluetooth.ScanResult) bool {
				srMAC := sr.Address.String()
				if norm, err := NormalizeMAC(srMAC); err == nil {
					srMAC = norm
				}
				isIT := d.isInfiniTimeScanResult(sr)
				if !d.shouldAcceptCandidate(srMAC, isIT, "", dfuPinMAC, prevMAC, prevDisconnectAt, applyPrevGrace, time.Now()) {
					return false
				}
				if !shouldAttemptConnect(d.adapterID, srMAC, sr.RSSI) {
					return false
				}
				targetAddr = sr.Address
				found = true
				return true
			})
			if ctx.Err() != nil {
				return
			}
			if scanErr != nil {
				waitTimer := time.NewTimer(5 * time.Second)
				select {
				case <-waitTimer.C:
					continue
				case <-ctx.Done():
					waitTimer.Stop()
					return
				}
			}
		}

		if !found {
			waitTimer := time.NewTimer(1 * time.Second)
			select {
			case <-waitTimer.C:
				continue
			case <-ctx.Done():
				waitTimer.Stop()
				return
			}
		}

		if err := d.connectAndInit(ctx, targetAddr, true); err != nil {
			if ctx.Err() != nil {
				return
			}
			waitTimer := time.NewTimer(2 * time.Second)
			select {
			case <-waitTimer.C:
				continue
			case <-ctx.Done():
				waitTimer.Stop()
				return
			}
		}
		return
	}
}

func (d *Device) handleConnectionChange(addr bluetooth.Address, mac string, connected bool, isInfiniTime bool, fromWatch bool) {
	if norm, err := NormalizeMAC(mac); err == nil {
		mac = norm
	}

	if !connected {
		d.connStateMtx.Lock()
		curMac := d.mac
		if curMac == "" || !strings.EqualFold(mac, curMac) {
			d.connStateMtx.Unlock()
			return
		}
		if fromWatch && !d.started.Load() {
			d.connStateMtx.Unlock()
			return
		}
		wasConnected := d.isConnected
		if !wasConnected {
			d.connStateMtx.Unlock()
			return
		}
		d.isConnected = false
		now := time.Now()
		dfuPinned := d.dfuPinMAC != "" && now.Before(d.dfuPinUntil)
		if !dfuPinned || !strings.EqualFold(d.dfuPinMAC, curMac) {
			d.lastDisconnectedMAC = curMac
			d.lastDisconnectTime = now
		}
		closeChanIfOpen(d.disconnectCh)
		shouldReconnect := !d.manualDisconnect &&
			!d.connecting &&
			!d.manualConnecting &&
			d.manualTargetMAC == "" &&
			d.pendingExternalMAC == "" &&
			!d.reconnecting &&
			d.shouldAutoReconnect()
		applyPrevGrace := !dfuPinned
		d.connStateMtx.Unlock()

		if d.opts.OnDisconnect != nil {
			go d.opts.OnDisconnect(d)
		}
		if shouldReconnect {
			d.startReconnectLoop(applyPrevGrace)
		}
		return
	}

	if !fromWatch {
		d.connStateMtx.Lock()
		if d.mac != "" && strings.EqualFold(mac, d.mac) && d.connecting {
			d.isConnected = true
		}
		d.connStateMtx.Unlock()
		return
	}

	if !d.started.Load() || !isInfiniTime {
		return
	}
	if !d.isAllowed(mac) {
		return
	}

	now := time.Now()
	d.connStateMtx.Lock()
	if d.connecting || d.manualConnecting {
		d.connStateMtx.Unlock()
		return
	}
	if d.isConnected && strings.EqualFold(d.mac, mac) {
		d.connStateMtx.Unlock()
		return
	}
	if d.pendingExternalMAC != "" && strings.EqualFold(d.pendingExternalMAC, mac) {
		d.connStateMtx.Unlock()
		return
	}
	if d.ignoreExternalMAC != "" && strings.EqualFold(d.ignoreExternalMAC, mac) && now.Before(d.ignoreExternalUntil) {
		d.connStateMtx.Unlock()
		return
	}
	if d.dfuPinMAC != "" && now.Before(d.dfuPinUntil) && !strings.EqualFold(d.dfuPinMAC, mac) {
		d.connStateMtx.Unlock()
		return
	}
	d.pendingExternalMAC = mac
	d.connStateMtx.Unlock()

	go d.handleExternalConnect(addr, mac)
}

func (d *Device) handleExternalConnect(addr bluetooth.Address, mac string) {
	defer func() {
		d.connStateMtx.Lock()
		if strings.EqualFold(d.pendingExternalMAC, mac) {
			d.pendingExternalMAC = ""
		}
		d.connStateMtx.Unlock()
	}()

	d.cancelReconnect()

	d.connStateMtx.Lock()
	alreadySame := d.isConnected && strings.EqualFold(d.mac, mac)
	manualConflict := d.manualConnecting && (d.manualTargetMAC == "" || !strings.EqualFold(d.manualTargetMAC, mac))
	d.connStateMtx.Unlock()
	if alreadySame || manualConflict {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := d.connectAndInit(ctx, addr, true); err != nil {
		d.connStateMtx.Lock()
		shouldRestart := !d.isConnected && !d.manualDisconnect && !d.manualConnecting && d.manualTargetMAC == "" && d.shouldAutoReconnect()
		d.connStateMtx.Unlock()
		if shouldRestart {
			d.startReconnectLoop(false)
		}
	}
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
	stopBlueZNotifies(d.adapterID, d.Address(), ch)
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
	stopBlueZNotifies(d.adapterID, d.Address(), ch)
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
