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
func (rwl *Rwlocker) RUnlock() (bool, error) {
	return __driver.RUnlock(rwl.User, rwl.Host)
}

func main() {
	fmt.Println("vim-go")
}

func GetRwlocker(user string) (*Rwlocker, error) {
	if __driver == nil || __hostname != "" {
		return nil, fmt.Errorf("You should register a rwdriver first")
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
		log.Errorf("Unkown rwlocker driver: %s", name)
		return fmt.Errorf("Unkown rwlocker driver: %s", name)
	}
	__driver = __drivers[name]
	log.Infof("Rwlock: Init %s rwlock driver sucessfully", name)
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
