package main

import (
	"crontab/worker"
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
	// worker -config ./master.json
	flag.StringVar(&confFile, "config", "./worker.json", "指定master的json")
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
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	// 4 初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

ERR:
	fmt.Println(err)
}
