package driver

import "time"

type Driver interface {
	Rlock(user, host string, timeout time.Duration) (bool, error)
	RUnlock(user, host string) (bool, error)
	Wlock(user, host string, timeout time.Duration) (bool, error)
	WUnlock(user, host string) error
}
