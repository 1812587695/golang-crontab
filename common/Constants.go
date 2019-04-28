package common

// 常量
var (
	JOB_SAVE_DIR = "/cron/jobs/" // etcd任务保存目录

	JOB_KILLER_DIR = "/cron/killer/" // 任务强杀目录

	//任务所目录
	JOB_LOCK_DIR = "/cron/lock/"

	// 保存任务事件
	JOB_EVENT_SAVE = 1

	//删除任务事件
	JOB_EVENT_DELETE = 2

	// 强杀任务事件
	JOB_EVENT_KILL = 3
)
