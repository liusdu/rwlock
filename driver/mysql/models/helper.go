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
		log.Errorf("Rwlock[m: %s] InsertUser Begin trasaction err: %s", err)
		return fmt.Errorf("InsertUser Begin trasaction err: %s", err)
	}

	log.Debugf("Rwlock[m: %s] InsertUser Begin trasaction success", user)

	defer endTransaction(o, err)
	lock := Rwlock{
		User: user,
		Time: time.Now()}

	if _, err = o.Insert(&lock); err != nil {
		return fmt.Errorf("InsertUser[m: %s]: insert row of rwlock err: %s", user, err)
	}
	return nil

}

func lockUser(o orm.Ormer, user string) (*Rwlock, error) {
	lock := &Rwlock{
		User: user}
	//TODO what types of error, we should dig!
	// select for update to lock the row
	err := o.ReadForUpdate(lock)

	if err == orm.ErrNoRows {
		// Someone delete this line for me, it is strange, But
		// system can go on, so just failed for this time.
		log.Errorf("LockUser[m: %s]: Lock do not exist, so give up", user)
		return nil, nil
	} else if err != nil {
		// I can not treat this error, may be  it is very dangerous
		// maybe  this is a small issue I have not catched.
		// So give up for this time
		log.Errorf("LockUser[m: %s]: Lock user error: %s", user, err)
		return nil, fmt.Errorf("LockUser[m: %s]: Lock user error: %s", user, err)
	}
	err = o.QueryTable("rwlock").Filter("user__exact", user).One(lock)
	if err == orm.ErrNoRows {
		log.Errorf("LockUser[m: %s] :lock do not exist, give up", user)
		// No need to retry
		return nil, nil
	} else if err != nil {
		log.Errorf("LockUser[m: %s]: Query user error: %s", user, err)
		//TODO what should we do for this
		return nil, fmt.Errorf("LockUser[m: %s]: Query user error: %s", user, err)
	}
	return lock, nil
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
