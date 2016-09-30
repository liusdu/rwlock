package models

// Tables describes tables' interface
type Tables interface {
	Insert() (id int64, err error)
	Query(fields []string, sortby []string, order []string, offset int64, limit int64) (ml []interface{}, err error)
	Update() (err error)
	Delete() (err error)
}
