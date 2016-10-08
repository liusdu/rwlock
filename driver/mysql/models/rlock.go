package models

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func Rlock(user, host string, timeout time.Duration) (bool, error) {
	var (
		o    = orm.NewOrm()
		err  error
		lock *Rwlock
	)

	// 1. begin the trasaction
	if err = o.Begin(); err != nil {
		log.Errorf("RwLock[m: %s-%s]: Begin trasaction err: %s", user, host, err)
		return false, err
	}
	log.Debugf("RLock[m: %s-%s]: Begin trasaction success", user, host)

	defer func() {
		endTransaction(o, err)
	}()

	// 2. Lock the user
	lock, err = lockUser(o, user)
	if err != nil {
		log.Errorf("RLock[m: %s-%s]: Failed to get Rlock: %s", user, host, err)
		return false, fmt.Errorf("Rlock[m: %s-%s]: error: %s", user, host, err)
	}

	// false to get rlock return
	if lock == nil {
		log.Debugf("RLock[m: %s-%s]: Failed ot get Rlock", user, host)
		return false, nil
	}

	// 3. check whethe we can get the lock
	// if r lock we should update count and time  for this host
	// if rlock is out of date, we should clean the count
	if lock.Type == "r" {
		// we do not need to remove the related row on host table
		// Time clumon of host table can tell me to do it later
		lock.Time = time.Now()
		if _, err = o.Update(lock, "time", "type"); err != nil {
			//TODO what should we do for this
			log.Errorf("RLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
			return false, fmt.Errorf("RLock[m: %s-%s]: Unable to update time and type of lock: %s", user, host, err)
		}
		//update host count
		if err = increaseHostCount(o, lock, host, timeout); err != nil {
			//TODO what should we do for this
			return false, err
		}

	} else {
		// we should check is it timeout
		ok := time.Now().After(lock.Time.Add(timeout))
		if (lock.Type == "w" && ok) || lock.Type != "w" {
			// The Wlock is out of date, replace with Rlock
			log.Infof("The Wlock is out of date, replace with Rlock")
			lock.Type = "r"
			lock.Time = time.Now()
			if _, err = o.Update(lock, "time", "type"); err != nil {
				return false, err
			}
			// insert host count
			if err = increaseHostCount(o, lock, host, timeout); err != nil {
				//TODO what should we do for this
				return false, err
			}

		} else {
			return false, nil
		}

	}
	return true, nil
}

// RunLock is unlock of rlock
// return value:
//              bool : should retry
//              error: error
func RUnlock(user, host string) error {
	var (
		o    = orm.NewOrm()
		err  error
		lock *Rwlock
	)

	if err = o.Begin(); err != nil {
		log.Errorf("Runlock[m]: Begin trasaction err: %s", err)
		return fmt.Errorf("Runlock[m]: Begin trasaction err: %s", err)
	}

	defer func() {
		endTransaction(o, err)
	}()

	// 2. Lock the user
	lock, err = lockUser(o, user)
	if err != nil {
		log.Errorf("RUnLock[m: %s-%s]: Failed ot get Rlock: %s", user, host, err)
		return fmt.Errorf("RUnlock[m: %s-%s]: error: %s", user, host, err)
	}

	// false to get rlock return
	if lock == nil {
		log.Infof("RUnLock[m: %s-%s]: Failed ot get Rlock", user, host)
		return nil
	}

	// if r lock we should update count and time  for this host
	if lock.Type != "r" {
		log.Infof("Runlock[m: %s]: Can find this lock, it is strange; maybe this lock is out of date", user)
		return nil

	} else {
		// we should check is it timeout
		log.Debugf("Runlock[m-%s-%s]: Reduce count", host, user)
		// insert host count
		var rm bool
		if rm, err = decreaseHostCount(o, lock, host); err != nil {
			return fmt.Errorf("Runlock[m]: unexpected error : %s", err)
		}
		if rm {
			lock.Type = ""
			if _, err = o.Update(lock, "type"); err != nil {
				return fmt.Errorf("Runlock[m: %s-%s]: Unexcept error when update row: %s", host, user, err)
			}
		}
	}
	return nil
}
