package driver

import "time"

type Driver interface {
	Rlock(user, host string, timeout time.Duration) (bool, error)
	RUnlock(user, host string) error
	Wlock(user, host string, timeout time.Duration) (bool, error)
	WUnlock(user, host string) error
	Cleanup(host string, timeout time.Duration) error
}
