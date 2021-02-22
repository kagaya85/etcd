package etcd

import (
	"sync"

	"github.com/go-kratos/kratos/v2/registry"
)

var (
	_ registry.Registry = &Registry{}
)

// Config is etcd registry config
type Config struct {
}

// Registry is etcd registry
type Registry struct {
	cfg  *Config
	lock sync.RWMutex
}

// Register the registration.
func (r *Registry) Register(service *registry.ServiceInstance) error {
	return nil
}

// Deregister the registration.
func (r *Registry) Deregister(service *registry.ServiceInstance) error {
	return nil
}

// Service return the service instances in memory according to the service name.
func (r *Registry) Service(name string) ([]*registry.ServiceInstance, error) {
	return nil, nil
}

// Watch creates a watcher according to the service name.
func (r *Registry) Watch(name string) (registry.Watcher, error) {
	return nil, nil
}
