package common

import (
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
	"context"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`     // 任务名称
	Command  string `json:"command"`  // shell 命令
	CronExpr string `json:"cronExpr"` // cron 表达式
}

// 任务调度计划
type JobSchedule struct {
	Job      *Job                 // 要调度的任务信息
	Expr     *cronexpr.Expression // 解析好的cronexpr表达式
	NextTime time.Time            // 下次任务执行时间
}

// HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// 事件变化
type JobEvent struct {
	EventType int // SAVE,DELETE
	Job       *Job
}

// 应答方法
func BuildResponse(erron int, msg string, data interface{}) (resp []byte, err error) {

	// 1定义一个response结构体
	var (
		response Response
	)

	// 2将参数赋值给response
	response.Errno = erron
	response.Msg = msg
	response.Data = data

	// 3序列化json
	resp, err = json.Marshal(response)
	return
}

// 反序列化job
func UnpackJob(value []byte) (ret *Job, err error) {

	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}

	ret = job
	return
}

// 从etcd的key中提取任务名
// 例如：/cron/jbos/job10 抹掉 /cron/jobs/ 得到 jobs10
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 从/cron/jobs/job10提取job10
func ExtractKillerName(KillerKey string) string {
	return strings.TrimPrefix(KillerKey, JOB_KILLER_DIR)
}

// 任务变化事件有2种：1）更新任务 2）删除任务
func BuildJobEvent(enentType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: enentType,
		Job:       job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	//解析Job的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		//fmt.Println("解析Job的cron表达式",err)
		return
	}
	//fmt.Println(expr)
	//生成任务调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	//fmt.Println(expr.Next(time.Now()))
	//fmt.Println("构造任务执行计划",jobSchedulePlan)
	return
}

// 任务调度计划
type JobSchedulePlan struct {
	Job      *Job
	Expr     *cronexpr.Expression
	NextTime time.Time
}

// 任务执行
type JobExecuteInfo struct {
	Job      *Job      // 任务信息
	PlanTime time.Time // 理论上的调度时间
	RealTime time.Time // 实际上的调度时间
	CancelCtx context.Context // 任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的cancel函数
}

// 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // 执行状态
	Output      []byte          // 脚本输出
	Err         error           // 脚本错误原因
	StartTime   time.Time       // 脚本启动时间
	EndTime     time.Time       // 脚本结束时间
}
