package worker

import (
	"crontab/common"
	"fmt"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent              //etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan //任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  // 任务执行表
	jobResultChan     chan *common.JobExecuteResult      // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理map中的任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {

	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo  *common.JobExecuteInfo
		jobExecuting    bool
		jobExisted      bool
		err             error
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			fmt.Println("handleJobEvent保存事件", err)
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	case common.JOB_EVENT_DELETE: //删除事件
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消command执行, 判断任务是否在执行中
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() // 触发command杀死shell
		}

	}

	return
}

//尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	//调度 和 执行 是2件事

	// 执行的任务可能允许很久，1分钟会调度60次，但是只能执行1次,防止并发

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("任务正在执行，跳过本次调度", jobPlan.Job.Name)
		return
	}

	// 构建执行信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	// TODO
	fmt.Println("执行任务：", jobPlan.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecutoJob(jobExecuteInfo)

}

// 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	// 当前时间
	now = time.Now()

	// 如果任务表为空的话，随便睡眠多久
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}
	//fmt.Println(scheduler.jobPlanTable)
	//遍历map中的所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		//fmt.Println(jobPlan.NextTime)
		// 如果当前 map 中的 job任务 执行时间 小于 当前时间 或者等于当前时间
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//fmt.Println("执行任务：", jobPlan.Job.Name)
			scheduler.TryStartJob(jobPlan)

			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}
	}
	// 统计最近一个要过期的任务时间
	if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
		nearTime = &jobPlan.NextTime
	}

	// 下次调度间隔 (最近要执行的任务调度时间 - 当前时间)
	scheduleAfter = (*nearTime).Sub(now)

	return
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	fmt.Println("任务执行完成", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {

	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 初始化一次(1秒)
	scheduleAfter = scheduler.TrySchedule()
	//fmt.Println("初始化一次", scheduleAfter)

	//调度的延时定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务command.job
	for {
		select {
		// 监听任务变化事件
		case jobEvent = <-scheduler.jobEventChan:
			// 对内存维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)

		case <-scheduleTimer.C: // 最近的任务到期了
			//调度一次任务
			scheduleAfter = scheduler.TrySchedule()
			//fmt.Println("调度一次任务", scheduleAfter)
			// 重置调度间隔
			scheduleTimer.Reset(scheduleAfter)
			//fmt.Println("重置调度间隔", scheduleAfter)

		case jobResult = <-scheduler.jobResultChan: //监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	go G_scheduler.scheduleLoop()
	return
}

// 传回任务执行结果
func (scheduler *Scheduler) PushJobResuld(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
