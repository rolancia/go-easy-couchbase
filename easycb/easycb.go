package easycb

import (
	"context"
	"time"
)

type EasyCB struct {
	operators CouchbaseInterface
	config    Config
}

func NewEasyCB(op CouchbaseInterface, cfg Config) *EasyCB {
	conf := Config{}
	conf.init()
	conf.merge(&cfg)

	ins := EasyCB{
		operators: op,
		config:    conf,
	}

	return &ins
}

type Config struct {
	NumRetry   int
	RetryDelay time.Duration
}

func (cfg *Config) init() {
	cfg.NumRetry = 10
	cfg.RetryDelay = 100 * time.Millisecond
}

func (cfg *Config) merge(other *Config) {
	if other.NumRetry != 0 {
		cfg.NumRetry = other.NumRetry
	}

	if other.RetryDelay != time.Duration(0) {
		cfg.RetryDelay = other.RetryDelay
	}
}

func (cb *EasyCB) InsertWithRetry(ctx context.Context, doc Document) error {
	err := withRetry(cb.config.NumRetry, cb.config.RetryDelay, func() (bool, error) {
		_, err := cb.operators.Insert(ctx, doc)
		if err != nil {
			if err == cb.operators.ErrTimeout() ||
				err == cb.operators.ErrBusy() ||
				err == cb.operators.ErrTmpFail() {
				return true, err
			} else {
				return false, err
			}
		} else {
			return false, nil
		}
	})

	return err
}

func (cb *EasyCB) GetWithRetry(ctx context.Context, doc Document) (uint64, error) {
	var retCas uint64
	err := withRetry(cb.config.NumRetry, cb.config.RetryDelay, func() (bool, error) {
		cas, err := cb.operators.Get(ctx, doc)
		if err != nil {
			if err == cb.operators.ErrTimeout() ||
				err == cb.operators.ErrBusy() ||
				err == cb.operators.ErrTmpFail() {
				return true, err
			} else {
				return false, err
			}
		} else {
			retCas = cas
			return false, nil
		}
	})

	return retCas, err
}
