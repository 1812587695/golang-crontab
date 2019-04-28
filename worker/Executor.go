package worker

import (
	"crontab/common"
	"os/exec"
	"time"
	"math/rand"
)

// 任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

// 执行一个任务
func (executor *Executor) ExecutoJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		// 任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		// 记录开始时间
		result.StartTime = time.Now()

		// 随机睡眠（0-1s），防止分布式时间抢锁问题
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		// 上锁
		err = jobLock.TryLock()
		// 释放锁
		defer jobLock.Unlock()

		if err != nil { // 如果上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// 上锁成功后，重置任务启动时间
			result.StartTime = time.Now()

			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "C:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command)

			// 执行并捕获输出
			output, err = cmd.CombinedOutput()

			// 记录开始时间
			result.EndTime = time.Now()

			result.Output = output
			result.Err = err

		}

		// 任务执行完成后，把执行的结果返回给scheduler，scheduler会从executing中删除掉执行的记录
		G_scheduler.PushJobResuld(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}

	return
}
