package models

import (
	//	"fmt"
	"time"

	//log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	. "github.com/go-check/check"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func checkTables(c *C, locktype string, user, hostname string, rlockref int64) {
	var o = orm.NewOrm()
	u := &Rwlock{User: user}
	err := o.Read(u)
	c.Assert(err, IsNil)
	c.Assert(u.Type, Equals, locktype)

	if locktype == "r" {
		host := &Host{}
		err = o.QueryTable("host").
			Filter("Hostname__exact", hostname).
			Filter("User__User__exact", user).One(host)
		c.Assert(err, IsNil)
		c.Assert(host.Count, Equals, rlockref)
	}
	if locktype == "w" {
		c.Assert(u.LastWlock, Equals, hostname)
	}

}

func (ms *MysqlSuite) TestRlock(c *C) {
	//1.  lock on node1 for three times
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err := Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	Rlock("aaa", "node1", ms.lockTimeout)
	Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(getlock, Equals, true)
	checkTables(c, "r", "aaa", "node1", 3)

	//2.  test afftect between locks
	err = InsertUser("bbb")
	c.Assert(getlock, Equals, true)
	Rlock("bbb", "node1", ms.lockTimeout)
	checkTables(c, "r", "bbb", "node1", 1)

	//3. test locks for timeout
	time.Sleep(ms.lockTimeout + time.Second)

	//3.1 get wlock
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

	// 3.2 failed to get rlock
	getlock, err = Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, false)

	time.Sleep(ms.lockTimeout + time.Second)

	// 3.3 when timeout,get rlock successfully
	getlock, err = Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	checkTables(c, "r", "aaa", "node1", 1)
}

func (ms *MysqlSuite) TestUnLock(c *C) {
	//1.  lock on node1 for three times
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err := Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	Rlock("aaa", "node1", ms.lockTimeout)
	Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(getlock, Equals, true)
	checkTables(c, "r", "aaa", "node1", 3)

	//2. unlock it and check the count and type
	err = RUnlock("aaa", "node1")
	c.Assert(err, IsNil)
	err = RUnlock("aaa", "node1")
	c.Assert(err, IsNil)
	checkTables(c, "r", "aaa", "node1", 1)

	err = RUnlock("aaa", "node1")
	c.Assert(err, IsNil)

	//2. If we get Wlock, we can still release lock
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

	err = RUnlock("aaa", "node1")
	c.Assert(err, IsNil)

}
