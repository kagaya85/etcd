package registry

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/go-kratos/etcd"
	"github.com/go-kratos/kratos/v2/registry"
)

var (
	_ registry.Watcher = &watcher{}
)

type serviceSet struct {
	serviceName string
	watcher     *watcher
	cli         *etcd.Client
	services    *atomic.Value
	lock        sync.RWMutex
}

type watcher struct {
	event  chan struct{}
	name   string
	ctx    context.Context
	cli    *etcd.Client
	cancel context.CancelFunc
	set    *serviceSet
}

func newWatcher(set *serviceSet, cli *etcd.Client) *watcher {
	watch := &watcher{
		name:  set.serviceName,
		event: make(chan struct{}, 1),
		cli:   cli,
		set:   set,
	}
	go watch.watch()
	return watch
}

func (w *watcher) Next() (svrs []*registry.ServiceInstance, err error) {
	select {
	case <-w.ctx.Done():
		err = w.ctx.Err()
		return
	case <-w.event:
	}
	ss, ok := w.set.services.Load().([]*registry.ServiceInstance)
	if ok {
		for _, s := range ss {
			svrs = append(svrs, s)
		}
	}
	return
}

func (w *watcher) Close() error {
	return nil
}

func (w *watcher) watch() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.cli.Watch(w.ctx, w.name):
		}
	}
}
