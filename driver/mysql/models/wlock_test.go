package models

import (
	//	"fmt"
	"time"

	//log "github.com/Sirupsen/logrus"
	//"github.com/astaxie/beego/orm"
	. "github.com/go-check/check"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func (ms *MysqlSuite) TestWlock(c *C) {
	//1.  wlocks for the same user can  not auqired concurrency
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err := Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, false)
	getlock, err = Wlock("aaa", "node2", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, false)
	checkTables(c, "w", "aaa", "node1", 0)

	//2.  wlocks do not afftect other wlock
	err = InsertUser("bbb")
	getlock, err = Wlock("bbb", "node1", ms.lockTimeout)
	c.Assert(getlock, Equals, true)
	Rlock("bbb", "node1", ms.lockTimeout)
	checkTables(c, "w", "bbb", "node1", 1)

	//3. test locks for timeout
	time.Sleep(ms.lockTimeout + time.Second)

	//3.1 get wlock
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

}

func (ms *MysqlSuite) TestWUnLock(c *C) {

	//test Wunlock works

	// 1. get wlock
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err := Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

	// 2. can not get twice
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, false)

	// 3. uolock
	err = WUnlock("aaa", "node1")
	c.Assert(err, IsNil)
	// 4. relock seccessfully
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	// 5. unlock
	err = WUnlock("aaa", "node1")
	c.Assert(err, IsNil)
	// 6. rerlock seccessfully
	getlock, err = Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

}
