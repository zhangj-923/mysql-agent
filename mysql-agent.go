package main

import (
	"fmt"
	"mysql-agent/common"
	"mysql-agent/internal"
	"time"
)

func main() {
	conf := common.Conf{}
	config := conf.GetModelClass("config.json")
	fmt.Printf("**********************Agent启动:%s**************************** \n", time.Now().Format("2006-01-02 15:04:05"))
	times := 1
	for {
		fmt.Printf("<<<<<<<<<<<<<<进行第 %d 次数据采集，当前时间为：%s>>>>>>>>>>>>>> \n", times, time.Now().Format("2006-01-02 15:04:05"))
		internal.Run()
		time.Sleep(time.Second * time.Duration(int(config.AgentCycle)))
		times += 1
		// 采集一次退出
		//break
	}
}
