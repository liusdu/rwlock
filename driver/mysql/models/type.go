package models

import (
	"github.com/astaxie/beego/orm"
)

var basicModels = []interface{}{
	new(Host),
	new(Rwlock),
}

// tables for RWlock
// Rwlock struct
type Rwlock struct {
	id   int     `orm:"column(id);auto"`
	User string  `orm:"pk;column(user);size(255);"`
	Type string  `orm:"column(type);size(255);"`
	Time int64   `orm:"column(time);null"`
	Host []*Host `orm:"reverse(many)"`
	// for wlock
	LastWlock string `orm:"column(lastwlock);size(255);"`
}

type Host struct {
	Id       int     `orm:"column(id);auto"`
	Count    int64   `orm:"column(count);null"`
	User     *Rwlock `orm:"rel(fk)"`
	Hostname string  `orm:"column(hostname);size(255);null"`
	Time     int64   `orm:"column(time);null"`
}

func init() {
	orm.RegisterModel(basicModels...)
}
