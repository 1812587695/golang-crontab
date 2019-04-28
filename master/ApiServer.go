package master

import (
	"crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 单例
var (
	G_apiServer *ApiServer
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

// 保存任务接口
// jbo = {"name":"job","command":"echo hello","cronExpr":"*/5 * * * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	// 接口保存到etcd中

	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)

	// 1 解析post表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	// 2 取出表单中的job字段
	postJob = r.PostForm.Get("job")

	// 3 将json字符串转化成common.Job结构体(类似与java的对象)，便于调用
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	//4 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	//5返回正常应答（{}）
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		// 数据发送给客户端浏览器
		w.Write(bytes)
	}

	return
ERR:
	// 6返回错误应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 删除etcd任务
func handleJobDelete(w http.ResponseWriter, r *http.Request) {

	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	// 删除任务的名称
	name = r.PostForm.Get("name")

	// 去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	//返回正常应答（{}）
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		// 数据发送给客户端浏览器
		w.Write(bytes)
	}

	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 查看etcd中的任务列表
func handleJobList(w http.ResponseWriter, r *http.Request) {

	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)

	// 获取任务列表
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}

	//返回正常应答（{}）
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		// 数据发送给客户端浏览器
		w.Write(bytes)
	}

	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 强杀某个任务
func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)

	// 解析post表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	// 要杀死的任务名
	name = r.PostForm.Get("name")

	// 杀死任务
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	//返回正常应答（{}）
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		// 数据发送给客户端浏览器
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 初始化服务
func InitApiServer() (err error) {
	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir
		staticHandler http.Handler
	)

	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return err
	}

	// 创建HTTP服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	// 复制单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)

	return
}
