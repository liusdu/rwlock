package models

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func WLock(user, host string, timeout time.Duration) (bool, error) {
	var (
		o    = orm.NewOrm()
		err  error
		lock *Rwlock
	)

	if err = o.Begin(); err != nil {
		log.Errorf("WLock[m: %s-%s]: Begin trasaction err: %s", user, host, err)
		return false, err
	}
	log.Debugf("WLock[m: %s-%s]: Begin trasaction success", user, host)

	defer endTransaction(o, err)

	// 2. Lock the user
	lock, err = lockUser(o, user)
	if err != nil {
		log.Errorf("RLock[m: %s-%s]: Failed ot get Rlock: %s", user, host, err)
		return false, fmt.Errorf("Rlock[m: %s-%s]: error: %s", user, host, err)
	}
	// false to get rlock return
	if lock == nil {
		log.Infof("WLock[m: %s-%s]: Failed ot get wlock", user, host)
		return false, nil
	}

	if ok := time.Now().After(lock.Time.Add(timeout)); ok || lock.Type == "" {
		// we shoud refile rwlock row and try to delete related row on host table;
		if lock.Type == "r" {
			if err = removeHostCount(o, lock, host); err != nil {
				return false, err
			}
		}
		lock.Time = time.Now()
		lock.Type = "w"

		if _, err = o.Update(lock, "time", "type"); err != nil {
			//TODO what should we do for this
			log.Errorf("WLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
			return false, fmt.Errorf("WLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
		}
	} else {
		// Lock by another wlock, give up
		log.Debugf("WLock[m: %s-%s]: Cannot get lock", user, host)
		return false, nil
	}
	return true, nil
}

// WUnLock is unlock of wlock
// return value:
//              bool : should retry
//              error: error
func WUnLock(user, host string) error {
	var (
		o    = orm.NewOrm()
		err  error
		lock *Rwlock
	)

	if err = o.Begin(); err != nil {
		log.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
		return fmt.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
	}

	defer endTransaction(o, err)
	// 2. Lock the user
	lock, err = lockUser(o, user)
	if err != nil {
		log.Errorf("WUnLock[m: %s-%s]: Failed ot get Rlock: %s", user, host, err)
		return fmt.Errorf("WUnlock[m: %s-%s]: error: %s", user, host, err)
	}

	// if r lock we should update count and time  for this host
	if lock.Type != "w" {
		log.Infof("Wunlock[m]: Can find this lock(%s), it is strange; maybe this lock is out of date", user)
		return nil

	} else {
		// we should check is it timeout
		lock.Type = ""
		if _, err = o.Update(lock, "type"); err != nil {
			return fmt.Errorf("Wunlock[m: %s-%s]: Unexcept error when update row: %s", host, user, err)
		}
	}
	return nil
}
