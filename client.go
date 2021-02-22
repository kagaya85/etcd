package etcd

import (
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
