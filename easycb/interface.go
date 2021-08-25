package easycb

import (
	"context"
	"strings"
)

type CouchbaseInterface interface {
	ErrTimeout() error
	ErrTmpFail() error
	ErrBusy() error
	ErrKeyExists() error

	Insert(ctx context.Context, doc Document) (uint64, error)
	Upsert(ctx context.Context, doc Document) (uint64, error)
	Replace(ctx context.Context, doc Document) (uint64, error)

	Remove(ctx context.Context, meta Metadata) error

	Get(ctx context.Context, doc Document) (uint64, error)
	GetAndTouch(ctx context.Context, doc Document) (uint64, error)
}

type Document interface {
	DocumentMetadata() Metadata
	DocumentDataPtr() interface{}
}

type Metadata struct {
	Bucket     string
	Scope      string
	Collection string
	ID         string
	ExtraIDs   []string

	cas uint64
	ex  uint32
}

func (m Metadata) Cas() uint64 {
	return m.cas
}

func (m Metadata) Ex() uint32 {
	return m.ex
}

func (m Metadata) DefaultKey() string {
	affected := []string{m.Collection, m.ID}
	affected = append(affected, m.ExtraIDs...)

	return strings.Join(affected, ".")
}
