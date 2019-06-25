package connection_mysql

import (
	"database/sql"
	"github.com/ArsistPdtion/workbook/my_crawler/simple_e1/data_model"
	_ "github.com/go-sql-driver/mysql"
	"github.com/labstack/gommon/log"
)

func ConnectMysql() *sql.DB {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/crawler")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func CloseMysql(db *sql.DB) {
	err := db.Close()
	if err != nil {
		log.Panic(err)
	}
}

func MysqlInsertOneData(db *sql.DB, data data_model.Movies) error {
	stmt, err := db.Prepare("insert into movie(name,imgurl) values(?,?);")
	if err != nil {
		log.Fatal(err)
	}
	res, err := stmt.Exec(data.Name, data.ImgUrl)
	if err != nil {
		log.Fatal(err)
	}
	lastId, err := res.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ID=%d, affect=%d\n", lastId, rowCnt)
	return nil

}

func MysqlInsertManyData(db *sql.DB, datas []interface{}) error {
	for _, data := range datas {
		err := MysqlInsertOneData(db, data.(data_model.Movies))
		if err != nil {
			log.Fatal(err)
		}
	}
	return nil
}
