package common

import (
	"fmt"
	"log"
	"os"
)

var Outfile *os.File

var (
	Error *log.Logger
	Info  *log.Logger
	Warn  *log.Logger
)

func init() {
	var err error
	Outfile, err = os.OpenFile("./log/agent.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		log.Panicf("open log file fail:%s", err)
	}

	Error = log.New(Outfile, "[error]", log.Ldate|log.Ltime|log.Lshortfile)
	Info = log.New(Outfile, "[info]", log.Ldate|log.Ltime|log.Lshortfile)
	Warn = log.New(Outfile, "[warn]", log.Ldate|log.Ltime|log.Lshortfile)

}

func checkErr(funcName string, err error) {
	if err != nil {
		fmt.Println(funcName, err)
		panic(err)
	}
}
