package mysql

import (
	"fmt"
	"strings"
	"time"

	//log "github.com/Sirupsen/logrus"
	//"github.com/astaxie/beego/orm"

	lock "rwlock"
	"rwlock/driver/mysql/models"
)

var (
	__name = "mysql"
)

func init() {
	lock.RegisterDriver(__name, &MySQLDriver{Name: __name})
}

type MySQLDriver struct {
	Name string
}

func (msd *MySQLDriver) Rlock(user, host string, timeout time.Duration) (bool, error) {
	//can we insert this line
	err := models.InsertUser(user)
	if err != nil {
		//if failed for row exist; we can continue
		if ok := strings.Contains(err.Error(), "Duplicate entry"); !ok {
			return false, fmt.Errorf("Rlock[m: %s-%s]: Unexpected MySQL Rwlocker driver error: %s", user, host, err)
		}
	}

	var getlock bool
	getlock, err = models.RLock(user, host, timeout)
	if err != nil {
		return false, err
	} else {
		return getlock, err
	}

}
func (msd *MySQLDriver) RUnlock(user, host string) error {
	return models.RUnLock(user, host)
}
func (msd *MySQLDriver) Wlock(user, host string, timeout time.Duration) (bool, error) {
	//can we insert this line
	err := models.InsertUser(user)
	if err != nil {
		//if failed for row exist; we can continue
		if ok := strings.Contains(err.Error(), "Duplicate entry"); !ok {
			return false, fmt.Errorf("Some errors I can not treate")
		}
	}

	var getlock bool
	getlock, err = models.WLock(user, host, timeout)
	if err != nil {
		return false, err
	} else {
		return getlock, err
	}
	return true, nil

}
func (msd *MySQLDriver) WUnlock(user, host string) error {
	return models.RUnLock(user, host)

}
