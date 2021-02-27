package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-kratos/kratos/v2/registry"
	"go.etcd.io/etcd/clientv3"
)

var (
	_ registry.Registry = &Registry{}
)

const (
	prefix = "/kratos/registry"
)

type options struct {
	// register service under prefixPath
	prefixPath string
	ttl        uint64
}

// Option is etcd registry opt
type Option func(o *options)

// PrefixPath with etcd prefix path.
func PrefixPath(prefix string) Option {
	return func(o *options) { o.prefixPath = prefix }
}

func WithTTL(ttl uint64) Option {
	return func(o *options) { o.ttl = ttl }
}

// Registry is etcd registry
type Registry struct {
	opt      *options
	cli      *clientv3.Client
	registry map[string]*serviceSet
	lock     sync.RWMutex
}

// New creates etcd registry
func New(cli *clientv3.Client, opts ...Option) (r *Registry, err error) {
	if err != nil {
		return
	}
	opt := &options{
		prefixPath: prefix,
	}
	for _, op := range opts {
		op(opt)
	}
	r = &Registry{
		cli: cli,
		opt: opt,
	}
	return
}

func (r *Registry) serviceKey(name, uuid string) string {
	return fmt.Sprintf("%s/%s/%s", r.opt.prefixPath, name, uuid)
}

func encode(s *registry.ServiceInstance) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func decode(ds []byte) *registry.ServiceInstance {
	var s *registry.ServiceInstance
	json.Unmarshal(ds, &s)
	return s
}

// Register the registration.
func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) (err error) {
	key := r.serviceKey(service.Name, service.ID)
	value := encode(service)
	lease := clientv3.NewLease(r.cli)
	var putOpt []clientv3.OpOption
	var lrp *clientv3.LeaseCreateResponse
	if r.opt.ttl > 0 {
		lrp, err = lease.Create(context.TODO(), int64(r.opt.ttl))
		if err != nil {
			return err
		}
		putOpt = append(putOpt, clientv3.WithLease(clientv3.LeaseID(lrp.ID)))
	}
	_, err = r.cli.Put(context.Background(), key, value, putOpt...)
	if err != nil {
		return
	}
	if lrp.ID > 0 {
		r.cli.KeepAlive(context.TODO(), clientv3.LeaseID(lrp.ID))
	}
	return err
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) (err error) {
	key := r.serviceKey(service.Name, service.ID)
	_, err = r.cli.Delete(context.Background(), key)
	return err
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
