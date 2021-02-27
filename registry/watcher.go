package registry

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/storage/storagepb"
	"github.com/go-kratos/etcd"
	"github.com/go-kratos/kratos/v2/registry"
	"go.etcd.io/etcd/clientv3"
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
	cli    *clientv3.Client
	cancel context.CancelFunc
	set    *serviceSet
	ch     clientv3.WatchChan
}

func newWatcher(set *serviceSet, cli *clientv3.Client) *watcher {
	w := &watcher{
		name:  set.serviceName,
		event: make(chan struct{}, 1),
		cli:   cli,
		set:   set,
	}
	w.ch = w.cli.Watch(w.ctx, w.name)
	go w.watch()
	return w
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
	w.cancel()
	return nil
}

func (w *watcher) watch() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}
		ss, _ := w.set.services.Load().([]*registry.ServiceInstance)
		smap := make(map[string]*registry.ServiceInstance, len(ss))
		for _, s := range ss {
			smap[s.ID] = s
		}
		for wresp := range w.ch {
			if wresp.Err() != nil || wresp.Canceled {
				w.ch = w.cli.Watch(w.ctx, w.name)
				continue
			}

			for _, ev := range wresp.Events {
				service := decode(ev.Kv.Value)
				switch ev.Type {
				case storagepb.PUT:
					smap[string(ev.Kv.Key)] = service
				case storagepb.DELETE:
					delete(smap, string(ev.Kv.Key))
				}
			}
		}
		newss := make([]*registry.ServiceInstance, len(smap))
		for _, s := range smap {
			newss = append(newss, s)
		}
		w.set.services.Store(newss)
	}
}
