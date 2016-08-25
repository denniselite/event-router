package conn

import (
	_ "github.com/lib/pq"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

func Connect(connectionString string) (*gorm.DB, error) {

	return gorm.Open("postgres", connectionString)
}

func ConnectMySQL(connectionString string) (*gorm.DB, error) {

	return gorm.Open("mysql", connectionString)
}