package common

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type MysqlClient struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Dbname    string `json:"dbname"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	MysqlConn sql.DB `json:"mysqlConn"`
}

func (m *MysqlClient) GetConn() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", m.Username, m.Password, m.Host, m.Port, m.Dbname)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
		fmt.Println("Mysql Conn Error!")
	}
	conn.SetConnMaxIdleTime(5)
	conn.SetMaxOpenConns(5)
	err = conn.Ping()
	if err != nil {
		fmt.Println("Mysql Ping Error!")
		return
	}
	m.MysqlConn = *conn
}

func (m *MysqlClient) CloseConn() error {
	return m.MysqlConn.Close()
}

//获取表数据
func (m *MysqlClient) Query(sql string) []map[string]interface{} {
	rows, err := m.MysqlConn.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	return getQueryResult(rows)
}

// DML操作
func (m *MysqlClient) Exec(sql string, arg ...interface{}) error {
	tx, err := m.MysqlConn.Begin()
	if err != nil {
		Error.Println("open mysql database fail", err)
		return err
	}

	stmt, err := m.MysqlConn.Prepare(sql)
	if err != nil {
		Error.Println(err)
		tx.Rollback()
	}
	res, err := stmt.Exec()
	if err != nil {
		Error.Println(err)
		tx.Rollback()
	}
	lastId, err := res.LastInsertId()
	if err != nil {
		Error.Println(err)
		tx.Rollback()
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		Error.Println(err)
		tx.Rollback()
	}
	fmt.Printf("ID=%d, arrected=%d\n", lastId, rowCnt)
	return tx.Commit()
}

// 插入数据
func (m *MysqlClient) Insert(sql string) {

	stmt, err := m.MysqlConn.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	res, err := stmt.Exec()
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
	fmt.Printf("insert ok, ID=%d, rowCnt=%d\n", lastId, rowCnt)
}

// 删除数据
func (m *MysqlClient) Delete(sql string) {
	stmt, err := m.MysqlConn.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	res, err := stmt.Exec()
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
	fmt.Printf("delete ok, ID=%d, rowCnt=%d\n", lastId, rowCnt)
}

// 更新数据
func (m *MysqlClient) Update(sql string) {
	stmt, err := m.MysqlConn.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	res, err := stmt.Exec()
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
	fmt.Printf("update ok, ID=%d, rowCnt=%d\n", lastId, rowCnt)
}

// 查询结果转换
func getQueryResult(rows *sql.Rows) []map[string]interface{} {
	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength) //临时存储每行数据 切片

	for index := range cache { //为每一列初始化一个指针
		var a interface{}
		cache[index] = &a
	}

	var list []map[string]interface{} //返回的切片
	for rows.Next() {
		_ = rows.Scan(cache...)

		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *data.(*interface{}) //取实际类型
		}

		for k, v := range item {
			if v == nil {
				continue
			}
			item[k] = string(v.([]uint8))
		}
		list = append(list, item)
	}
	return list
}
