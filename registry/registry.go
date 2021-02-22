package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-kratos/etcd"
	"github.com/go-kratos/kratos/v2/registry"
	"go.etcd.io/etcd/clientv3"
)

var (
	_ registry.Registry = &Registry{}
)

// Config is etcd registry config
type Config struct {
	clientv3.Config
	// register service under prefixPath
	PrefixPath string
}

// Registry is etcd registry
type Registry struct {
	cfg  *Config
	cli  *etcd.Client
	lock sync.RWMutex
}

// New creates etcd registry
func New(cfg *Config) (r *Registry, err error) {
	cli, err := etcd.NewClient(cfg.Config)
	if err != nil {
		return
	}
	r = &Registry{
		cfg: cfg,
		cli: cli,
	}
	return
}

// Register the registration.
func (r *Registry) Register(service *registry.ServiceInstance) error {
	key := fmt.Sprintf("%s/%s/%s", r.cfg.PrefixPath, service.Name, service.ID)
	value, err := json.Marshal(service)
	if err != nil {
		return err
	}
	return r.cli.Put(context.Background(), key, string(value))
}

// Deregister the registration.
func (r *Registry) Deregister(service *registry.ServiceInstance) error {
	key := fmt.Sprintf("%s/%s/%s", r.cfg.PrefixPath, service.Name, service.ID)
	return r.cli.Delete(context.Background(), key)
}

// Service return the service instances in memory according to the service name.
func (r *Registry) Service(name string) ([]*registry.ServiceInstance, error) {
	return nil, nil
}

// Watch creates a watcher according to the service name.
func (r *Registry) Watch(name string) (registry.Watcher, error) {
	return nil, nil
}
