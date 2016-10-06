package models

import (
	//"fmt"
	//"time"

	//log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	. "github.com/go-check/check"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func allTablesEmpty(c *C) {
	var o = orm.NewOrm()
	var u = Rwlock{}

	err := o.QueryTable("rwlock").One(&u)
	c.Assert(err, Equals, orm.ErrNoRows)
	var h = Host{}

	err = o.QueryTable("Host").One(&h)
	c.Assert(err, Equals, orm.ErrNoRows)
}

func (ms *MysqlSuite) TestCleanupTables(c *C) {
	//1.  lock on node1 for three times; for node2 for 2 times
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err := Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	Rlock("aaa", "node1", ms.lockTimeout)
	Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(getlock, Equals, true)
	checkTables(c, "r", "aaa", "node1", 3)
	Rlock("aaa", "node2", ms.lockTimeout)
	Rlock("aaa", "node2", ms.lockTimeout)

	// 2. cleanup tables related to node2
	CleanupTables("node1", ms.lockTimeout)
	// 3. can not get lock
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, false)

	// 4. cleanup tables related to node2
	CleanupTables("node2", ms.lockTimeout)
	// 5. lock seccuessful
	err = InsertUser("aaa")
	c.Assert(err, IsNil)
	getlock, err = Wlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)

	// 6. cleanup tables related to node2
	CleanupTables("node1", ms.lockTimeout)
	// 7. can get rlock
	err = InsertUser("aaa")
	getlock, err = Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	c.Assert(getlock, Equals, true)
	checkTables(c, "r", "aaa", "node1", 1)
	// 8. there must be no row in all tables
	CleanupTables("node1", ms.lockTimeout)

	// 9 . rlock and runlock
	getlock, err = Rlock("aaa", "node1", ms.lockTimeout)
	c.Assert(err, IsNil)
	err = RUnlock("aaa", "node1")
	c.Assert(err, IsNil)
	CleanupTables("node1", ms.lockTimeout)

	allTablesEmpty(c)
}
