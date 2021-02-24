package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	cfg      *Config
	cli      *etcd.Client
	registry map[string]*serviceSet
	lock     sync.RWMutex
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

func (r *Registry) serviceKey(name, uuid string) string {
	return fmt.Sprintf("%s/%s/%s", r.cfg.PrefixPath, name, uuid)
}

// Register the registration.
func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	key := r.serviceKey(service.Name, service.ID)
	value, err := json.Marshal(service)
	if err != nil {
		return err
	}
	return r.cli.Put(context.Background(), key, string(value))
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	key := r.serviceKey(service.Name, service.ID)
	return r.cli.Delete(context.Background(), key)
}

// Service return the service instances in memory according to the service name.
func (r *Registry) Service(ctx context.Context, name string) (services []*registry.ServiceInstance, err error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	set := r.registry[name]
	if set == nil {
		return nil, fmt.Errorf("service %s not watch in registry", name)
	}
	ss, _ := set.services.Load().([]*registry.ServiceInstance)
	if ss == nil {
		return nil, fmt.Errorf("service %s not found in registry", name)
	}
	for _, s := range ss {
		services = append(services, s)
	}
	return
}

// Watch creates a watcher according to the service name.
func (r *Registry) Watch(ctx context.Context, name string) (registry.Watcher, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	set, ok := r.registry[name]
	if ok {
		return nil, errors.New("service had been watch")
	}

	set = &serviceSet{
		services:    &atomic.Value{},
		serviceName: name,
	}
	r.registry[name] = set
	w := newWatcher(set, r.cli)
	w.ctx, w.cancel = context.WithCancel(context.Background())
	set.lock.Lock()
	set.watcher = w
	set.lock.Unlock()
	ss, _ := set.services.Load().([]*registry.ServiceInstance)
	if len(ss) > 0 {
		w.event <- struct{}{}
	}
	return w, nil
}
