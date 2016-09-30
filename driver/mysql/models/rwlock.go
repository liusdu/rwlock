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
		log.Errorf("InsertUser error: %s", serr)
		if err := o.Rollback(); err != nil {
			log.Errorf("InsertUser rollback error::%s", err)
		}
		return
	}
	if err := o.Commit(); err != nil {
		log.Errorf("InsertUser commit error:%s", err)
		if err := o.Rollback(); err != nil {
			log.Errorf("InsertUser rollback error::%s", err)
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

	defer endTransaction(o, err)
	lock := Rwlock{
		User: user,
		Time: time.Now()}

	if _, err = o.Insert(&lock); err != nil {
		return err
	}
	if err = o.Commit(); err != nil {
		return err
	}

	return nil

}

func RLock(user, host string, timeout time.Duration) (bool, error) {
	var (
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("MysqlRLock: Begin trasaction err: %s", err)
		return false, err
	}

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)

	if err != nil {
		return false, err
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		log.Errorf("RLock :No user(%s) row in table, it is strange give up the lock", user)
		//TODO what should we do for this
		return false, nil
	} else {
		log.Errorf("RLock: Unexcept error for user(%s): %s", user, err)
		//TODO what should we do for this
		return false, err
	}

	// if r lock we should update count and time  for this host
	if lock.Type == "r" {
		lock.Time = time.Now()
		if _, err = o.Update(lock, "time", "type"); err != nil {
			//TODO what should we do for this
			return false, err
		}
		//update host count
		if err = increaseHostCount(o, lock, host); err != nil {
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
			if err = increaseHostCount(o, lock, host); err != nil {
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
func increaseHostCount(o orm.Ormer, user *Rwlock, hostname string) error {
	host := &Host{}
	err := o.QueryTable("host").
		Filter("Hostname", hostname).
		Filter("User__User__exact", user.User).One(&host)

	if err == orm.ErrNoRows {
		log.Debugf("No user-host(%s-%s) row in table, insert it", user.User, hostname)
		host.Hostname = hostname
		host.User = user
		host.Count = 1
		if _, err = o.Insert(user); err != nil {
			return err
		}

	} else {
		return fmt.Errorf("Unexcept error for user-host(%s-%s): %s", user.User, hostname, err)
	}

	host.Count += 1
	if _, err = o.Update(host, "count"); err != nil {
		return err
	}
	return nil

}

// decreaseHostCount
// it delete  a new record or increase the count by 1
func decreaseHostCount(o orm.Ormer, user *Rwlock, hostname string) (bool, error) {
	host := &Host{}
	err := o.QueryTable("host").
		Filter("Hostname__exact", hostname).
		Filter("User__User__exact", user.User).One(&host)

	if err == orm.ErrNoRows {
		return true, fmt.Errorf("Unexpect error: no host row(%s,%s),maybe this lock is out fo date", hostname, user)

	} else {
		return false, fmt.Errorf("Unexpect error for user-host(%s-%s): %s", user.User, hostname, err)
	}

	host.Count -= 1
	if host.Count == 0 {
		if _, err = o.Delete(host); err != nil {
			return false, err
		}
	} else {
		if _, err = o.Update(host, "count"); err != nil {
			return false, err
		}
	}
	return true, nil
}
func RUnLock(user, host string) (bool, error) {
	var (
		o   = orm.NewOrm()
		err error
	)

	if err = o.Begin(); err != nil {
		log.Errorf("MysqlRLock: Begin trasaction err: %s", err)
		return false, err
	}

	defer endTransaction(o, err)

	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	err = o.ReadForUpdate(lock)

	if err != nil {
		// I can not get the lock, so things maybe wrong
		log.Errorf("Can not Unlock for host-user(%s-%s)", host, user)
		return false, err
	}

	//read all rows with private key user
	//TODO error type??
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		log.Errorf("No user(%s) row in table, it is strange; maybe this lock is outof date", user)
		return true, fmt.Errorf("No user(%s) row in table, it is strange; maybe this lock is outof date", user)
	} else {
		log.Errorf("Unexcept error for user(%s): %s", user, err)
		return false, fmt.Errorf("Unexcept error for user(%s): %s", user, err)
	}

	// if r lock we should update count and time  for this host
	if lock.Type != "r" {
		log.Errorf("Can find this lock(%s), it is strange; maybe this lock is outof date", user)
		return true, fmt.Errorf("Can find this lock(%s), it is strange; maybe this lock is outof date", user)

	} else {
		// we should check is it timeout
		log.Debugf("Release a rlock(%s-%s)", host, user)
		lock.Type = ""
		lock.Time = time.Now()
		if _, err = o.Update(lock, "type"); err != nil {
			return false, err
		}
		// insert host count
		var success bool
		if success, err = decreaseHostCount(o, lock, host); success && err != nil {
			log.Errorf("Unexpect error, but try to cotinue : %s", err)
		}

		if !success && err != nil {
			return false, err
		}

	}
	return true, nil
}
