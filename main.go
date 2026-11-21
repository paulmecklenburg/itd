/*
 *	itd uses bluetooth low energy to communicate with InfiniTime devices
 *	Copyright (C) 2021 Arsen Musayelyan
 *
 *	This program is free software: you can redistribute it and/or modify
 *	it under the terms of the GNU General Public License as published by
 *	the Free Software Foundation, either version 3 of the License, or
 *	(at your option) any later version.
 *
 *	This program is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *	GNU General Public License for more details.
 *
 *	You should have received a copy of the GNU General Public License
 *	along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gen2brain/dlgs"
	"github.com/knadh/koanf"
	"github.com/mattn/go-isatty"
	"go.elara.ws/itd/infinitime"
	"go.elara.ws/logger"
	"go.elara.ws/logger/log"
)

var k = koanf.New(".")

var (
	firmwareUpdating = false
	// The FS must be updated when the watch is reconnected
	updateFS = false
)

func main() {
	showVer := flag.Bool("version", false, "Show version number and exit")
	flag.Parse()
	// If version requested, print and exit
	if *showVer {
		fmt.Println(version)
		return
	}

	level, err := logger.ParseLogLevel(k.String("logging.level"))
	if err != nil {
		level = logger.LogLevelInfo
	}
	log.Logger.SetLevel(level)

	adapterCfg := strings.TrimSpace(k.String("bluetooth.adapter"))
	if adapterCfg != "" && adapterCfg != "hci0" {
		log.Warn("Configured bluetooth.adapter is not supported by tinygo/bluetooth; falling back to hci0").
			Str("configured", adapterCfg).
			Send()
	}

	// Create infinitime options struct
	opts := infinitime.Options{
		Adapter: "hci0",
		AutoReconnect: func() bool {
			return k.Bool("conn.reconnect")
		},
		AllowFunc: func(mac string) bool {
			if !k.Bool("conn.whitelist.enabled") {
				return true
			}
			normTarget, err := infinitime.NormalizeMAC(mac)
			if err != nil {
				normTarget = strings.ToUpper(strings.TrimSpace(mac))
			}
			for _, allowed := range k.Strings("conn.whitelist.devices") {
				normAllowed, err := infinitime.NormalizeMAC(allowed)
				if err != nil {
					normAllowed = strings.ToUpper(strings.TrimSpace(allowed))
				}
				if normAllowed != "" && strings.EqualFold(normAllowed, normTarget) {
					return true
				}
			}
			return false
		},
		OnDisconnect: func(dev *infinitime.Device) {
			log.Warn("Disconnected from InfiniTime, waiting to reconnect").Str("address", dev.Address()).Send()
		},
		OnReconnect: func(dev *infinitime.Device) {
			log.Info("Reconnected to InfiniTime").Str("address", dev.Address()).Send()

			if k.Bool("on.reconnect.setTime") {
				// Set time to current time
				if err := dev.SetTime(time.Now()); err != nil {
					log.Error("Error setting current time on reconnected InfiniTime").Err(err).Send()
				}
			}

			// If config specifies to notify on reconnect
			if k.Bool("on.reconnect.notify") {
				// Send notification to InfiniTime
				if err := dev.Notify("itd", "Successfully reconnected"); err != nil {
					log.Error("Error sending reconnect notification to InfiniTime").Err(err).Send()
				}
			}

			// FS must be updated on reconnect
			updateFS = true
			// Resend weather on reconnect
			select {
			case sendWeatherCh <- struct{}{}:
			default:
			}
		},
	}

	ctx := context.Background()

	// Connect to InfiniTime with default options
	dev, err := infinitime.Connect(opts)
	if err != nil {
		log.Fatal("Error connecting to InfiniTime").Err(err).Send()
	}
	defer dev.Close()

	// Get firmware version
	ver, err := dev.Version()
	if err != nil {
		log.Error("Error getting firmware version").Err(err).Send()
	}

	// Log connection
	log.Info("Connected to InfiniTime").Str("version", ver).Send()

	// If config specifies to notify on connect
	if k.Bool("on.connect.notify") {
		// Send notification to InfiniTime
		err = dev.Notify("itd", "Successfully connected")
		if err != nil {
			log.Error("Error sending notification to InfiniTime").Err(err).Send()
		}
	}

	// Set time to current time
	err = dev.SetTime(time.Now())
	if err != nil {
		log.Error("Error setting current time on connected InfiniTime").Err(err).Send()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		sig := <-sigCh
		log.Warn("Signal received, shutting down").Stringer("signal", sig).Send()
		cancel()
	}()

	wg := WaitGroup{&sync.WaitGroup{}}

	// Initialize music controls
	err = initMusicCtrl(ctx, wg, dev)
	if err != nil {
		log.Error("Error initializing music control").Err(err).Send()
	}

	// Start control socket
	err = initCallNotifs(ctx, wg, dev)
	if err != nil {
		log.Error("Error initializing call notifications").Err(err).Send()
	}

	// Initialize notification relay
	err = initNotifRelay(ctx, wg, dev)
	if err != nil {
		log.Error("Error initializing notification relay").Err(err).Send()
	}

	// Initializa weather
	err = initWeather(ctx, wg, dev)
	if err != nil {
		log.Error("Error initializing weather").Err(err).Send()
	}

	// Initialize metrics collection
	err = initMetrics(ctx, wg, dev)
	if err != nil {
		log.Error("Error intializing metrics collection").Err(err).Send()
	}

	// Initialize puremaps integration
	err = initPureMaps(ctx, wg, dev)
	if err != nil {
		log.Error("Error intializing puremaps integration").Err(err).Send()
	}

	// Start fuse socket
	if k.Bool("fuse.enabled") {
		err = startFUSE(ctx, wg, dev)
		if err != nil {
			log.Error("Error starting fuse socket").Err(err).Send()
		}
	}

	// Start control socket
	err = startSocket(ctx, wg, dev)
	if err != nil {
		log.Error("Error starting socket").Err(err).Send()
	}

	wg.Wait()
}

type x struct {
	n int
	*sync.WaitGroup
}

func (xy *x) Add(i int) {
	xy.n += i
	xy.WaitGroup.Add(i)
	fmt.Println("add: counter:", xy.n)
}

func (xy *x) Done() {
	xy.n -= 1
	xy.WaitGroup.Done()
	fmt.Println("done: counter:", xy.n)
}

func onReqPasskey() (uint32, error) {
	var out uint32
	if isatty.IsTerminal(os.Stdin.Fd()) {
		fmt.Print("Passkey: ")
		_, err := fmt.Scanln(&out)
		if err != nil {
			return 0, err
		}
	} else {
		passkey, ok, err := dlgs.Entry("Pairing", "Enter the passkey displayed on your watch.", "")
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		passkeyInt, err := strconv.Atoi(passkey)
		return uint32(passkeyInt), err
	}
	return out, nil
}
