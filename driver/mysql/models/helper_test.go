package models

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/orm"
	. "github.com/go-check/check"
	_ "github.com/go-sql-driver/mysql" //sql driver
)

func Test(t *testing.T) { TestingT(t) }

type MysqlSuite struct {
	locks       []Rwlock
	dbName      string
	dbServer    string
	dbPort      string
	dbUser      string
	dbPwd       string
	conn        string
	lockTimeout time.Duration
}

var _ = Suite(&MysqlSuite{})

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
	verbose := false
	return orm.RunSyncdb(name, force, verbose)
}
func (ms *MysqlSuite) SetUpTest(c *C) {
	err := reCreateTables()
	c.Assert(err, IsNil)
}

func (ms *MysqlSuite) SetUpSuite(c *C) {
	ms.dbName = "lock2"
	ms.dbUser = "root"
	ms.dbPwd = "00010001"
	ms.dbServer = "127.0.0.1"
	ms.dbPort = "3307"
	ms.lockTimeout = time.Second * 2

	conn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", ms.dbUser, ms.dbPwd, ms.dbServer, ms.dbPort, "mysql")
	err := createdb(conn, "lock2")
	ms.conn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", ms.dbUser, ms.dbPwd, ms.dbServer, ms.dbPort, ms.dbName)
	orm.RegisterDataBase("default", "mysql", ms.conn)
	c.Assert(err, IsNil)
	orm.NewOrm().Using("default")
	log.SetLevel(log.DebugLevel)

}

func (ms *MysqlSuite) TearDownSuite(c *C) {
	err := dropdb(ms.conn, "lock2")
	c.Assert(err, IsNil)

}
func (ms *MysqlSuite) TestInsertUser(c *C) {
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

func (ms *MysqlSuite) TestLockUser(c *C) {
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
