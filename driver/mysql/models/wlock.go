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
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("WLock[m: %s-%s]: Begin trasaction err: %s", user, host, err)
		return false, err
	}
	log.Debugf("WLock[m: %s-%s]: Begin trasaction success", user, host)

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)

	if err == orm.ErrNoRows {
		// Someone delete this line for me, it is strange, But
		// system can go on, so just failed for this time.
		log.Errorf("Wlock[m: %s-%s]: Rwlock row is deleted abormally", user, host)
		return false, nil
	} else if err != nil {
		// I can not treat this error, may be  it is very dangerous
		// maybe  this is a small issue I have not catched.
		// So give up for this time
		log.Errorf("Wlock[m: %s-%s]: Unexport error: %s", user, host, err)
		return false, fmt.Errorf("Wlock[m: %s-%s]: Unexport error: %s", user, host, err)
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		log.Errorf("WLock[m: %s-%s] :No row in rwlock table, it is strange give up the lock", user, host)
		// No need to retry
		return false, nil
	} else if err != nil {
		log.Errorf("WLock[m: %s-%s]: Unexcept error: %s", user, host, err)
		//TODO what should we do for this
		return false, fmt.Errorf("WLock[m: %s-%s]: Unexcept error: %s", user, host, err)
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
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
		return fmt.Errorf("Wunlock[m]: Begin trasaction err: %s", err)
	}

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)
	if err == orm.ErrNoRows {
		// Someone delete this line for me, it is strange, But
		// system can go on, so just failed for this time.
		log.Infof("WUnlock[m]: Rwlock row is deleted abormally, maybe this lock is out of date")
		return nil
	} else if err != nil {
		// I can not treat this error, may be  it is very dangerous
		// maybe  this is a small issue I have not catched.
		// May be we need to retry
		log.Errorf("WUlock[m]: Unexport error: %s", err)
		return fmt.Errorf("WUlock[m]: Unexport error: %s", err)
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		// no need retry, becasue this lock is unlocked..
		log.Errorf("WUnlock[m]: No user(%s) row in table, it is strange; maybe this lock is outof date", user)
		return nil
	} else if err != nil {
		// This an error I can not deal with, So try another time to fix
		log.Errorf("Runlock[m]: Unexcept error for user(%s): %s", user, err)
		return fmt.Errorf("RUlock[m]: Unexcept error for user(%s): %s", user, err)
	}

	// if r lock we should update count and time  for this host
	if lock.Type != "w" {
		log.Infof("Wunlock[m]: Can find this lock(%s), it is strange; maybe this lock is out of date", user)
		return nil

	} else {
		// we should check is it timeout
		log.Debugf("Wunlock[m-%s-%s]: Reduce count", host, user)
		lock.Type = ""
		if _, err = o.Update(lock, "type"); err != nil {
			return fmt.Errorf("Wunlock[m: %s-%s]: Unexcept error when update row: %s", host, user, err)
		}
	}
	return nil
}
