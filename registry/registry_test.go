package registry

import (
	"testing"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

func TestRegister(t *testing.T) {
	conf := clientv3.Config{}
	conf.Endpoints = []string{"127.0.0.1:2379"}
	etcdCli, err := clientv3.New(conf)
	assert.Nil(t, err)
	register := New(etcdCli)
	service1 := &registry.ServiceInstance{}
	service1.Name = "test"
	service1.ID = "1"
	err = register.Register(context.Background(), service1)
	assert.Nil(t, err)
	watch, err := register.Watch(context.Background(), "test")
	assert.Nil(t, err)
	time.Sleep(time.Second)

	ss, err := watch.Next()
	assert.Nil(t, err)
	assert.Len(t, ss, 1)

	service2 := &registry.ServiceInstance{}
	service2.Name = "test"
	service2.ID = "2"
	err = register.Register(context.Background(), service2)

	ss, err = watch.Next()
	assert.Nil(t, err)
	assert.Len(t, ss, 2)

	ss, err = register.Fetch(context.Background(), "test")
	assert.Nil(t, err)
	assert.Len(t, ss, 2)
	service3 := &registry.ServiceInstance{}
	service3.Name = "test"
	service3.ID = "3"
	err = register.Register(context.Background(), service3)

	ss, err = watch.Next()
	assert.Nil(t, err)
	assert.Len(t, ss, 3)

	ss, err = register.Fetch(context.Background(), "test")
	assert.Nil(t, err)
	assert.Len(t, ss, 3)

	err = register.Deregister(context.Background(), service3)
	assert.Nil(t, err)

	ss, err = watch.Next()
	assert.Nil(t, err)
	assert.Len(t, ss, 2)

	ss, err = register.Fetch(context.Background(), "test")
	assert.Nil(t, err)
	assert.Len(t, ss, 2)
}
