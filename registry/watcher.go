package registry

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/storage/storagepb"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"go.etcd.io/etcd/clientv3"
)

var (
	_      registry.Watcher = &watcher{}
	logger                  = log.NewHelper("kratos/etcd", log.DefaultLogger)
)

type serviceSet struct {
	serviceName string
	watcher     *watcher
	cli         *clientv3.Client
	services    *atomic.Value
	lock        sync.RWMutex
}

type watcher struct {
	event  chan struct{}
	prefix string
	key    string
	ctx    context.Context
	cli    *clientv3.Client
	cancel context.CancelFunc
	set    *serviceSet
	ch     clientv3.WatchChan
}

func newWatcher(prefix string, set *serviceSet, cli *clientv3.Client) *watcher {
	w := &watcher{
		prefix: prefix,
		event:  make(chan struct{}, 1),
		cli:    cli,
		set:    set,
	}
	w.key = fmt.Sprintf("%s/%s", w.prefix, set.serviceName)
	w.init()
	w.ch = w.cli.Watch(context.Background(), w.key, clientv3.WithPrefix())
	go w.watch()
	return w
}

func (w *watcher) init() {
	resp, err := w.cli.Get(context.Background(), w.key, clientv3.WithPrefix())
	if err != nil {
		return
	}
	newss := make([]*registry.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		service, err := decode(kv.Value)
		if err != nil {
			logger.Errorf("failed to decode service of key:%s, value:%s", string(kv.Key), string(kv.Value))
			continue
		}
		newss = append(newss, service)
	}
	w.set.services.Store(newss)
}

func (w *watcher) Next() (svrs []*registry.ServiceInstance, err error) {
	select {
	case <-w.ctx.Done():
		err = w.ctx.Err()
		return
	case <-w.event:
		fmt.Println("get")
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
		var wresp clientv3.WatchResponse
		select {
		case <-w.ctx.Done():
			return
		case wresp = <-w.ch:
		}
		if wresp.Err() != nil || wresp.Canceled {
			w.ch = w.cli.Watch(context.Background(), w.key, clientv3.WithPrefix())
			continue
		}
		ss, _ := w.set.services.Load().([]*registry.ServiceInstance)
		smap := make(map[string]*registry.ServiceInstance, len(ss))
		for _, s := range ss {
			smap[s.ID] = s
		}
		for _, ev := range wresp.Events {

			insName := bytes.TrimPrefix(ev.Kv.Key, []byte(w.key+"/"))
			fmt.Printf("get event:%s, key:%s\n", ev.Type.String(), string(insName))
			switch ev.Type {
			case storagepb.PUT:
				service, err := decode(ev.Kv.Value)
				if err != nil {
					logger.Errorf("failed to decode service of key:%s, value:%s", string(ev.Kv.Key), string(ev.Kv.Value))
					continue
				}
				smap[string(insName)] = service
			case storagepb.DELETE:
				delete(smap, string(insName))
			}
		}
		newss := make([]*registry.ServiceInstance, 0, len(smap))
		for _, s := range smap {
			newss = append(newss, s)
		}
		w.set.services.Store(newss)
		select {
		case w.event <- struct{}{}:
			fmt.Println("send")
		default:
		}

	}
}
