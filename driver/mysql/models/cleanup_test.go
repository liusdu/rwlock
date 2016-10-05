package models

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	//log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	. "github.com/go-check/check"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func Test(t *testing.T) { TestingT(t) }

type CleanupSuite struct {
	locks    []Rwlock
	dbName   string
	dbServer string
	dbPort   string
	dbUser   string
	dbPwd    string
	conn     string
}

var _ = Suite(&CleanupSuite{})

func dropdb(conn string, dbname string) error {
	db, err := sql.Open("mysql", conn)
	defer db.Close()

	dbCreateSql := fmt.Sprintf("DROP DATABASE `%s`", dbname)

	_, err = db.Exec(dbCreateSql)
	return err

}

func createdb(conn string, dbname string) error {
	db, err := sql.Open("mysql", conn)
	if err != nil {
		return err
	}
	usedbSql := fmt.Sprintf("use %s", dbname)

	_, err = db.Exec(usedbSql)
	if err == nil {
		return nil
	}

	db, err = sql.Open("mysql", conn)
	defer db.Close()

	dbCreateSql := fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci", dbname)

	_, err = db.Exec(dbCreateSql)
	return err

}

func reCreateTables() error {
	name := "default"
	force := true
	verbose := true
	return orm.RunSyncdb(name, force, verbose)
}
func (cs *CleanupSuite) SetUpTest(c *C) {
	err := reCreateTables()
	c.Assert(err, IsNil)
}

func (cs *CleanupSuite) SetUpSuite(c *C) {
	cs.dbName = "lock2"
	cs.dbUser = "root"
	cs.dbPwd = "00010001"
	cs.dbServer = "127.0.0.1"
	cs.dbPort = "3306"

	conn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", cs.dbUser, cs.dbPwd, cs.dbServer, cs.dbPort, "mysql")
	err := createdb(conn, "lock2")
	cs.conn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", cs.dbUser, cs.dbPwd, cs.dbServer, cs.dbPort, cs.dbName)
	orm.RegisterDataBase("default", "mysql", cs.conn)
	c.Assert(err, IsNil)
	orm.NewOrm().Using("default")

}

func (cs *CleanupSuite) TearDownSuite(c *C) {
	err := dropdb(cs.conn, "lock2")
	c.Assert(err, IsNil)

}
func (cs *CleanupSuite) TestInsertUser(c *C) {
	//Insert a user
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	// Insert a dup user
	err = InsertUser("aaa")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Duplicate entry"), Equals, true)
	// insert another user
	err = InsertUser("bbb")
	c.Assert(err, IsNil)
}

func (cs *CleanupSuite) TestLockUser(c *C) {
	//Insert a user
	err := InsertUser("aaa")
	c.Assert(err, IsNil)
	// Insert a dup user
	o := orm.NewOrm()
	lock, err1 := lockUser(o, "aaa")
	c.Assert(err1, IsNil)
	c.Assert(lock.User, Equals, "aaa")
	lock, err1 = lockUser(o, "bbb")
	c.Assert(err1, IsNil)
	c.Assert(lock, IsNil)

}
