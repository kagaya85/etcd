package etcd

import (
	"context"

	"go.etcd.io/etcd/clientv3"
)

// Client is etcd client
type Client struct {
	client *clientv3.Client
	config clientv3.Config
}

// NewClient creates consul client
func NewClient(cfg clientv3.Config) (*Client, error) {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{client: cli, config: cfg}, nil
}

// Put value into key
// TODO: add ttl
func (c *Client) Put(ctx context.Context, k, v string) (err error) {
	_, err = c.client.Put(ctx, k, v)
	return err
}

// Get value by key
func (c *Client) Get(ctx context.Context, k string) (v string, err error) {
	resp, err := c.client.Get(ctx, k, nil)
	if err != nil {
		return
	}
	v = string(resp.Kvs[0].Value)
	return
}

// Delete will delete the given key
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.client.Delete(ctx, key)
	return err
}

// Watch watch events change on key
func (c *Client) Watch(ctx context.Context, key string) clientv3.WatchChan {
	return c.client.Watch(ctx, key)
}
