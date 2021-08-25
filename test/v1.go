package main

import (
	"context"
	"fmt"
	v1 "github.com/couchbase/gocb"
	"github.com/rolancia/go-easy-couchbase/easycb"
)

func main() {
	v1c, err := v1.Connect("localhost")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	ecb := easycb.NewEasyCB(&A{cluster: v1c}, easycb.Config{
		NumRetry:   10,
		RetryDelay: 100,
	})

	user := User{
		ID:   "tae",
		Name: "Taehyung",
		Age:  28,
	}
	if err := ecb.InsertWithRetry(ctx, &user); err != nil {
		panic(err)
	}

	fetchedUser := User{
		ID: "tae",
	}

	if cas, err := ecb.GetWithRetry(ctx, &fetchedUser); err != nil {
		panic(err)
	} else {
		fmt.Println("cas:", cas)
	}
}

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (d *User) DocumentMetadata() easycb.Metadata {
	return easycb.Metadata{
		Bucket:     "main",
		Scope:      "",     // gocb v1 does not support scope
		Collection: "user", // gocb v1 does not support collection. this collection will be prefix of document  key
		ID:         d.ID,
		ExtraIDs:   nil,
	}
}

func (d *User) DocumentDataPtr() interface{} {
	return d
}

type A struct {
	cluster *v1.Cluster
}

func (a *A) ErrTimeout() error {
	return v1.ErrTimeout
}

func (a *A) ErrTmpFail() error {
	return v1.ErrTmpFail
}

func (a *A) ErrBusy() error {
	return v1.ErrBusy
}

func (a *A) ErrKeyExists() error {
	return v1.ErrKeyExists
}

func (a *A) bucket(name string) (*v1.Bucket, error) {
	return a.cluster.OpenBucket(name, name+"11")
}

func (a *A) Insert(ctx context.Context, doc easycb.Document) (uint64, error) {
	buc, err := a.bucket(doc.DocumentMetadata().Bucket)
	if err != nil {
		return 0, err
	}

	cas, err := buc.Insert(doc.DocumentMetadata().DefaultKey(), doc.DocumentDataPtr(), doc.DocumentMetadata().Ex())
	return uint64(cas), err
}

func (a *A) Upsert(ctx context.Context, doc easycb.Document) (uint64, error) {
	buc, err := a.bucket(doc.DocumentMetadata().Bucket)
	if err != nil {
		return 0, err
	}

	cas, err := buc.Upsert(doc.DocumentMetadata().DefaultKey(), doc.DocumentDataPtr(), doc.DocumentMetadata().Ex())
	return uint64(cas), err
}

func (a *A) Replace(ctx context.Context, doc easycb.Document) (uint64, error) {
	buc, err := a.bucket(doc.DocumentMetadata().Bucket)
	if err != nil {
		return 0, err
	}

	cas, err := buc.Replace(doc.DocumentMetadata().DefaultKey(), doc.DocumentDataPtr(), v1.Cas(doc.DocumentMetadata().Cas()), doc.DocumentMetadata().Ex())
	return uint64(cas), err
}

func (a *A) Remove(ctx context.Context, meta easycb.Metadata) error {
	buc, err := a.bucket(meta.Bucket)
	if err != nil {
		return err
	}

	_, err = buc.Remove(meta.DefaultKey(), v1.Cas(meta.Cas()))
	return err
}

func (a *A) Get(ctx context.Context, doc easycb.Document) (uint64, error) {
	buc, err := a.bucket(doc.DocumentMetadata().Bucket)
	if err != nil {
		return 0, err
	}

	cas, err := buc.Get(doc.DocumentMetadata().DefaultKey(), doc.DocumentDataPtr())
	return uint64(cas), err
}

func (a *A) GetAndTouch(ctx context.Context, doc easycb.Document) (uint64, error) {
	buc, err := a.bucket(doc.DocumentMetadata().Bucket)
	if err != nil {
		return 0, err
	}

	cas, err := buc.Get(doc.DocumentMetadata().DefaultKey(), doc.DocumentDataPtr())
	return uint64(cas), err
}
