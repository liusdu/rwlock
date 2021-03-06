package models

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func Wlock(user, host string, timeout time.Duration) (bool, error) {
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

	defer func() {
		endTransaction(o, err)
	}()

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

	locktime := time.Unix(lock.Time, 0)

	if ok := time.Now().After(locktime.Add(timeout)); ok || lock.Type == "" {
		// we shoud refile rwlock row and try to delete related row on host table;
		log.Infof("WLock[m: %s-%s]: Last lock is out of date,replace it", user, host)
		if lock.Type == "r" {
			if err = removeHostCount(o, lock, host); err != nil {
				return false, err
			}
		}
		lock.Time = time.Now().UTC().Unix()
		lock.Type = "w"
		lock.LastWlock = host

		if _, err = o.Update(lock, "time", "type", "lastwlock"); err != nil {
			//TODO what should we do for this
			log.Errorf("WLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
			return false, fmt.Errorf("WLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
		}
	} else {
		// Lock by another wlock, give up
		log.Debugf("WLock[m: %s-%s]: lock is already aquired, give up", user, host)
		return false, nil
	}
	log.Debugf("WLock[m: %s-%s]: Get lock successfully", user, host)
	return true, nil
}

// WUnLock is unlock of wlock
// return value:
//              bool : should retry
//              error: error
func WUnlock(user, host string) error {
	var (
		o    = orm.NewOrm()
		err  error
		lock *Rwlock
	)

	if err = o.Begin(); err != nil {
		log.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
		return fmt.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
	}

	defer func() {
		endTransaction(o, err)
	}()

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
