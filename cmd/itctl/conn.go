package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

func connectDevice(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return cli.Exit("Command connect takes at most one address argument", 1)
	}

	addr, err := client.Connect(c.Context, c.Args().First())
	if err != nil {
		return err
	}

	fmt.Printf("Connected to %s\n", addr)
	return nil
}

func disconnectDevice(c *cli.Context) error {
	if c.Args().Len() > 0 {
		return cli.Exit("Command disconnect takes no arguments", 1)
	}

	addr, err := client.Disconnect(c.Context)
	if err != nil {
		return err
	}

	if addr == "" {
		fmt.Println("Already disconnected")
	} else {
		fmt.Printf("Disconnected from %s\n", addr)
	}
	return nil
}
