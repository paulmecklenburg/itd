package api

import (
	"context"

	"go.elara.ws/itd/internal/rpc"
)

// Connect connects itd to an InfiniTime watch, optionally targeting a specific MAC address.
func (c *Client) Connect(ctx context.Context, address string) (string, error) {
	res, err := c.client.Connect(ctx, &rpc.ConnectRequest{Address: address})
	if err != nil {
		return "", err
	}
	return res.GetValue(), nil
}

// Disconnect disconnects itd from the currently connected InfiniTime watch and pauses auto-reconnect.
func (c *Client) Disconnect(ctx context.Context) (string, error) {
	res, err := c.client.Disconnect(ctx, &rpc.Empty{})
	if err != nil {
		return "", err
	}
	return res.GetValue(), nil
}
