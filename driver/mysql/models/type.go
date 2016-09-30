package models

import (
	"time"

	"github.com/astaxie/beego/orm"
)

var basicModels = []interface{}{
	new(Rwlock),
	new(Host),
}

// tables for RWlock
// Rwlock struct
type Rwlock struct {
	id   int       `orm:"column(id);auto"`
	User string    `orm:"pk;column(user);size(255);"`
	Type string    `orm:"column(type);size(255);"`
	Time time.Time `orm:"column(time);type(datetime)"`
	Host []*Host   `orm:"reverse(many)"`
}

type Host struct {
	id       int     `orm:"column(id);auto"`
	Count    int64   `orm:"column(count);null"`
	User     *Rwlock `orm:"rel(fk)"`
	Hostname string  `orm:"column(hostname);size(255);null"`
}

func init() {
	orm.RegisterModel(basicModels...)
}
