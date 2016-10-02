package models

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

// endTransaction stop Transaction due err
func endTransaction(o orm.Ormer, serr error) {
	if serr != nil {
		log.Errorf("Rwlock[m]: Trasaction error: %s", serr)
		if err := o.Rollback(); err != nil {
			log.Errorf("Rwlock[m]: Rollback error: %s", err)
		}
		return
	}
	if err := o.Commit(); err != nil {
		log.Errorf("Rwlock[m]: Trasaction commit error: %s", err)
		if err := o.Rollback(); err != nil {
			log.Errorf("Rwlock[m]: Rollback error from commit: %s", err)
		}
	}
}

func InsertUser(user string) error {

	var (
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("InsertUser: Begin trasaction err: %s", err)
		return err
	}

	log.Debugf("InsertUser: Begin trasaction success")

	defer endTransaction(o, err)
	lock := Rwlock{
		User: user,
		Time: time.Now()}

	if _, err = o.Insert(&lock); err != nil {
		return fmt.Errorf("InsertUser: insert row of rwlock err: %s", err)
	}
	return nil

}

func RLock(user, host string, timeout time.Duration) (bool, error) {
	var (
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("RwLock[m: %s-%s]: Begin trasaction err: %s", user, host, err)
		return false, err
	}
	log.Debugf("RLock[m: %s-%s]: Begin trasaction success", user, host)

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)

	if err == orm.ErrNoRows {
		// Someone delete this line for me, it is strange, But
		// system can go on, so just failed for this time.
		log.Errorf("Rlock[m: %s-%s]: Rwlock row is deleted abormally", user, host)
		return false, nil
	} else if err != nil {
		// I can not treat this error, may be  it is very dangerous
		// maybe  this is a small issue I have not catched.
		// So give up for this time
		log.Errorf("Rlock[m: %s-%s]: Unexport error: %s", user, host, err)
		return false, fmt.Errorf("Rlock[m: %s-%s]: Unexport error: %s", user, host, err)
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		log.Errorf("RLock[m: %s-%s] :No row in rwlock table, it is strange give up the lock", user, host)
		// No need to retry
		return false, nil
	} else if err != nil {
		log.Errorf("RLock[m: %s-%s]: Unexcept error: %s", user, host, err)
		//TODO what should we do for this
		return false, fmt.Errorf("RLock[m: %s-%s]: Unexcept error: %s", user, host, err)
	}

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

// increaseHostCount
// it insert a new record or increase the count by 1
func increaseHostCount(o orm.Ormer, user *Rwlock, hostname string, timeout time.Duration) error {
	host := &Host{}
	err := o.QueryTable("host").
		Filter("Hostname", hostname).
		Filter("User__User__exact", user.User).One(host)

	if err == orm.ErrNoRows {
		log.Debugf("IncreaseHostCount: user-host(%s-%s) row in table, insert it", user.User, hostname)
		host.Hostname = hostname
		host.User = user
		host.Count = 1
		host.Time = time.Now()
		if _, err = o.Insert(host); err != nil {
			return fmt.Errorf("increaseHostCount: Insert host table failed %s", err)
		}
		return nil

	} else if err != nil {
		return fmt.Errorf("IncreaseHost: CountUnexcept error: %s", err)
	}

	if ok := time.Now().After(host.Time.Add(timeout)); ok == true {
		host.Count = 1
	}
	host.Time = time.Now()
	host.Count += 1

	if _, err = o.Update(host, "count"); err != nil {
		return fmt.Errorf("IncreaseHostCount: update host table failed %s", err)
	}
	return nil

}

// removecreaseHostCount
// it delete  a new record of user-host
func removeHostCount(o orm.Ormer, user *Rwlock, hostname string) (err error) {
	host := &Host{
		Hostname: hostname,
		User:     user,
	}

	if _, err = o.Delete(host); err != nil {
		return fmt.Errorf("DeleteHostCount[m: %s-%s]: Unexcept error when delete row: %s", user.User, hostname, err)
	}

	return nil
}

// decreaseHostCount
// it delete  a new record or increase the count by 1
func decreaseHostCount(o orm.Ormer, user *Rwlock, hostname string) (rm bool, err error) {
	host := &Host{}
	err = o.QueryTable("host").
		Filter("Hostname__exact", hostname).
		Filter("User__User__exact", user.User).One(host)

	if err == orm.ErrNoRows {
		// We do not need let the upper func know this error, so just return nil
		// And no not retry
		log.Infof("DecreaseHostCount[m]: Unexpect error: no host row(%s,%s),maybe this lock is out fo date", hostname, user)
		return true, nil

	} else if err != nil {
		// Errors system can not deal with. So let system try again
		return false, fmt.Errorf("DecreaseHostCount[m]: Unexpect error for user-host(%s-%s): %s", user.User, hostname, err)
	}

	host.Count -= 1
	if host.Count <= 0 {
		log.Debugf("DecreaseHostCount[m-%s-%s]: count become zero, remove it,", hostname, user.User)
		if _, err = o.Delete(host); err != nil {
			return false, fmt.Errorf("DecreaseHostCount[m]: Unexcept error when delete row for user-host(%s-%s) : %s", user.User, hostname, err)
		}
		return true, nil

	} else {
		if _, err = o.Update(host, "count"); err != nil {
			return false, fmt.Errorf("DecreaseHostCount[m]: Unexcept error when update row for user-host(%s-%s) : %s", user.User, hostname, err)
		}
	}
	return false, nil
}

// RunLock is unlock of rlock
// return value:
//              bool : should retry
//              error: error
func RUnLock(user, host string) error {
	var (
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("Runlock[m]: Begin trasaction err: %s", err)
		return fmt.Errorf("Runlock[m]: Begin trasaction err: %s", err)
	}

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)
	if err == orm.ErrNoRows {
		// Someone delete this line for me, it is strange, But
		// system can go on, so just failed for this time.
		log.Infof("RUnlock[m]: Rwlock row is deleted abormally, maybe this lock is out of date")
		return nil
	} else if err != nil {
		// I can not treat this error, may be  it is very dangerous
		// maybe  this is a small issue I have not catched.
		// May be we need to retry
		log.Errorf("RUlock[m]: Unexport error: %s", err)
		return fmt.Errorf("RUlock[m]: Unexport error: %s", err)
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		// no need retry, becasue this lock is unlocked..
		log.Errorf("RUnlock[m]: No user(%s) row in table, it is strange; maybe this lock is outof date", user)
		return nil
	} else if err != nil {
		// This an error I can not deal with, So try another time to fix
		log.Errorf("Runlock[m]: Unexcept error for user(%s): %s", user, err)
		return fmt.Errorf("RUlock[m]: Unexcept error for user(%s): %s", user, err)
	}

	// if r lock we should update count and time  for this host
	if lock.Type != "r" {
		log.Infof("Runlock[m]: Can find this lock(%s), it is strange; maybe this lock is out of date", user)
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
