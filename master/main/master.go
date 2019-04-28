package main

import (
	"crontab/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var (
	confFile string
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定master的json")
	flag.Parse()
}

func main() {

	var (
		err error
	)

	// 1初始化命令行参数
	initArgs()

	// 2初始化线程
	initEnv()

	// 3加载配置文件夹
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 4启动etcd任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 5启动Api HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

ERR:
	fmt.Println(err)
}
