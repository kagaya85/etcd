package config

import (
	"context"
	"google.golang.org/grpc"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const testKey = "/kratos/test/config"

func TestConfig(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"},
		DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err = client.Put(context.Background(), testKey, "test config"); err != nil {
		t.Fatal(err)
	}

	source, err := New(client, Path(testKey))
	if err != nil {
		t.Fatal(err)
	}

	kvs, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(kvs) != 1 || kvs[0].Key != testKey || string(kvs[0].Value) != "test config" {
		t.Fatal("config error")
	}

	w, err := source.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	go func() {
		time.Sleep(time.Millisecond * 10)
		if _, err = client.Put(context.Background(), testKey, "new config"); err != nil {
			t.Error(err)
		}
	}()

	if kvs, err = w.Next(); err != nil {
		t.Fatal(err)
	}

	if len(kvs) != 1 || kvs[0].Key != testKey || string(kvs[0].Value) != "new config" {
		t.Fatal("config error")
	}

	client.Delete(context.Background(), testKey)
}
