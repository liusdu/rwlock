package rwlock

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	. "rwlock/driver"
)

// we should register a driver first
var (
	__driver   Driver
	__timeOut  time.Duration = time.Hour
	__hostname string
	__drivers  map[string]Driver
	// if lock or unlock error, times we need to try
	__retryCount = 2
)

type Rwlocker struct {
	User    string
	Host    string
	Timeout time.Duration
	driver  Driver
}

func (rwl *Rwlocker) Rlock() (bool, error) {
	return __driver.Rlock(rwl.User, rwl.Host, rwl.Timeout)
}
func (rwl *Rwlocker) RUnlock() error {
	for i := 0; i < __retryCount; i++ {
		if err := __driver.RUnlock(rwl.User, rwl.Host); err != nil {
			log.Infof("Runlock[%s-%s]: unexpect error: %s, try for the %d time,", rwl.User, rwl.Host, err, i+1)
		} else {
			log.Debugf("Runlock[%s-%s]: Release lock sucessfully", rwl.User, rwl.Host)
			break
		}
		log.Errorf("Runlock[%s-%s]: Faild to unlock", rwl.User, rwl.Host)
		return fmt.Errorf("Runlock[%s-%s]: Faild to unlock", rwl.User, rwl.Host)
	}
	return nil

}

func (rwl *Rwlocker) Wlock() (bool, error) {
	log.Debugf("Rwlock[%s-%s]: Getting Wlock", rwl.User, rwl.Host)
	return __driver.Wlock(rwl.User, rwl.Host, rwl.Timeout)
}
func (rwl *Rwlocker) WUnlock() error {
	log.Debugf("Rwlock[%s-%s]: Release Wlock", rwl.User, rwl.Host)
	for i := 0; i < __retryCount; i++ {
		if err := __driver.WUnlock(rwl.User, rwl.Host); err != nil {
			log.Infof("Wunlock[%s-%s]: unexpect error: %s, try for the %d time,", rwl.User, rwl.Host, err, i+1)
		} else {
			log.Debugf("Wunlock[%s-%s]: Release lock sucessfully", rwl.User, rwl.Host)
			break
		}
		log.Errorf("Wunlock[%s-%s]: Faild to unlock", rwl.User, rwl.Host)
		return fmt.Errorf("Wunlock[%s-%s]: Faild to unlock", rwl.User, rwl.Host)
	}
	return nil

}
func GetRwlocker(user string) (*Rwlocker, error) {
	if __driver == nil || __hostname == "" {
		return nil, fmt.Errorf("Rwlock: You should register a rwdriver first")
	}

	return &Rwlocker{
		User:    user,
		Host:    __hostname,
		Timeout: __timeOut,
		driver:  __driver,
	}, nil

}

func InitDriver(name string) error {
	if _, exist := __drivers[name]; !exist {
		log.Errorf("InitDriver: Unkown rwlocker driver: %s", name)
		return fmt.Errorf("InitDriver; Unkown rwlocker driver: %s", name)
	}
	__driver = __drivers[name]
	log.Infof("Rwlock: Init %s rwlock driver sucessfully,try to clean up garbages from the last run", name)
	__driver.Cleanup(__hostname, __timeOut)
	return nil
}

func RegisterDriver(name string, driver Driver) error {
	if _, exist := __drivers[name]; exist {
		log.Errorf("You have register this rwlocker driver: %s", name)
		return fmt.Errorf("You have register this rwlocker driver: %s", name)
	}
	log.Infof("Rwlock: Register rwlocker driver: %s", name)
	__drivers[name] = driver
	return nil
}
func init() {
	__drivers = make(map[string]Driver)
	if hm, err := os.Hostname(); err != nil {
		rand.Seed(time.Now().UnixNano())
		x := rand.Intn(100000)
		__hostname = "host" + string(x)
		log.Errorf("Failed to get hostname,error: %s , use %s as hostname", err, __hostname)
	} else {
		__hostname = hm
	}
}
