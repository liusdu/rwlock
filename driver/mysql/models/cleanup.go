package models

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func CleanupTables(hostname string, timeout time.Duration) error {
	var (
		o     = orm.NewOrm()
		locks []*Rwlock
		err   error
	)
	// list all host tables of this node, then remove them
	_, err = o.QueryTable("rwlock").All(&locks)
	if err == orm.ErrNoRows {
		// No need to retry
		log.Errorf("Cleanup[m: %s]there are no garbages", hostname)
		return nil
	} else if err != nil {
		log.Errorf("Cleanup[m: %s]: unexpected error: %s", hostname, err)
		//TODO what should we do for this
		return fmt.Errorf("Cleanup[m: %s]: unexpected error: %s", hostname, err)
	}

	for _, lock := range locks {
		cleanuplock(lock, hostname)
	}
	return nil
}

func cleanuplock(lock *Rwlock, hostname string) error {
	var (
		o   = orm.NewOrm()
		err error
	)

	if lock == nil {
		return nil
	}

	log.Debugf("Cleanup[m:%s-%s] Begin cleanup lock", lock.User, hostname)
	// 1. begin the trasaction
	if err = o.Begin(); err != nil {
		log.Errorf("Cleanuplock[m:%s-%s]: Begin trasaction err: %s", lock.User, hostname, err)
		return fmt.Errorf("Cleanuplock[m:%s-%s]: Begin trasaction err: %s", lock.User, hostname, err)
	}
	defer endTransaction(o, err)
	// 2. Lock the user
	lock, err = lockUser(o, lock.User)
	if err != nil || lock == nil {
		log.Errorf("Cleanuplock[m:%s-%s]: fail to get lock : %s", lock.User, hostname, err)
		return fmt.Errorf("Cleanuplock[m:%s-%s]: fail to get lock : %s", lock.User, hostname, err)
	}
	switch lock.Type {
	case "w":
		err = cleanupWlock(o, lock, hostname)
	case "r":
		err = cleanupRlock(o, lock, hostname)
	default:
		err = cleanupEmptylock(o, lock, hostname)
	}
	if err == nil {
		log.Debugf("Cleanup[m:%s-%s] cleanup lock successfully", lock.User, hostname)
	}
	return nil
}
func cleanupWlock(o orm.Ormer, lock *Rwlock, hostname string) error {
	if lock == nil {
		return nil
	}
	log.Debugf("cleanupwlock[m:%s-%s]: Cleanning wlock", lock.User, hostname)
	if lock.LastWlock == hostname || lock.LastWlock == "" {
		cnt, err := o.Delete(lock)
		if err != nil || cnt != 1 {
			log.Infof("Cleanupwlock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)
			return fmt.Errorf("Cleanupwlock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)

		}
	}
	return nil
}

func cleanupEmptylock(o orm.Ormer, lock *Rwlock, hostname string) error {
	log.Debugf("cleanupEmptylock[m:%s-%s]: Cleanning wlock", lock.User, hostname)
	cnt, err := o.Delete(lock)
	if err != nil || cnt != 1 {
		log.Infof("CleanupEmptylock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)
		return fmt.Errorf("CleanupEmptylock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)

	}
	return nil
}

func cleanupRlock(o orm.Ormer, lock *Rwlock, hostname string) error {
	var (
		hosts  []*Host
		latest time.Time
		err    error
	)

	log.Debugf("cleanupRlock[m:%s-%s]: Cleanning Rlock", lock.User, hostname)

	_, err = o.QueryTable("host").Filter("hostname__exact", hostname).All(&hosts)
	if err == orm.ErrNoRows || len(hosts) == 0 {
		log.Debug("Cleanrlock[m: %s]: clean up lock, no need to clean node infomation", lock.User)
		cnt, err := o.Delete(lock)
		if err != nil || cnt != 1 {
			log.Infof("Cleanupwlock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)
			return fmt.Errorf("Cleanupwlock[m:%s-%s]: Cleanning wlock error: %s or count error: %d", lock.User, hostname, err, cnt)
		}
		log.Debug("Cleanrlock[m: %s]: clean up rlock successfully", lock.User)
		return nil

	} else if err != nil {
		log.Errorf("Cleanup[m: %s]: unexpected error: %s", hostname, err)
		//TODO what should we do for this
		return fmt.Errorf("Cleanup[m: %s]: unexpected error: %s", hostname, err)
	}

	for _, h := range hosts {
		if h.Hostname == hostname {
			log.Debugf("cleanupRlock[m:%s-%s]: cleanup infomaton of this node,lock.User, hostname")
			_, err := o.Delete(h)
			if err != nil {
				log.Errorf("cleanupRlock[m:%s-%s]: Cleanning Rlock error: %s", lock.User, hostname, err)
				return fmt.Errorf("cleanupRlock[m:%s-%s]: Cleanning Rlock error: %s", lock.User, hostname, err)
			}
			continue
		}
		if latest.Before(h.Time) {
			latest = h.Time
		}
	}

	if latest.Equal(time.Time{}) {
		log.Debug("Cleanrlock[m: %s]: clean up lock", lock.User)
		_, err := o.Delete(lock)
		if err != nil {
			log.Errorf("cleanupRlock[m:%s-%s]: Cleanning Rlock error: %s", lock.User, hostname, err)
			return fmt.Errorf("cleanupRlock[m:%s-%s]: Cleanning Rlock error: %s", lock.User, hostname, err)
		}
		return nil

	}
	if lock.Time.After(latest) {
		lock.Time = latest
		if _, err = o.Update(lock, "time", "type"); err != nil {
			//TODO what should we do for this
			log.Errorf("Cleanrlock[m: %s]: clean up lock: Unable to update time and type of lock: %s", lock.User, err)
			return fmt.Errorf("Cleanrlock[m: %s]: clean up lock: Unable to update time and type of lock: %s", lock.User, err)
		}
	}

	return nil
}
