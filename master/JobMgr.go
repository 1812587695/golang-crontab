package master

import (
	"context"
	"crontab/common"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 超时时间
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到kv和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务etcd
// 启动etcd ./etcd --listen-client-urls 'http://0.0.0.0:2379' --advertise-client-urls 'http://0.0.0.0:2379'
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey     string
		jobValue   []byte
		putResp    *clientv3.PutResponse
		oldJobObj  common.Job
		ctx        context.Context
		cancelFunc func()
	)

	// 任务信息json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	// etcd的保存key
	jobKey = common.JOB_SAVE_DIR + job.Name

	ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(ctx, jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}

	defer cancelFunc()

	// 如果是更新，那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
	}

	oldJob = &oldJobObj

	return
}

//删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {

	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	// etcd的保存key
	jobKey = common.JOB_SAVE_DIR + name

	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {

		// json反序列化旧值
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 查询etcd所有任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)

	//保存任务的路径
	dirKey = common.JOB_SAVE_DIR

	// 获取etcd中路径所有信息
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组空间
	jobList = make([]*common.Job, 0)

	// 遍历所有任务，进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {

	var (
		killerKey          string
		leaseGrantResponse *clientv3.LeaseGrantResponse
		leaseId            clientv3.LeaseID
	)

	// 通知worker杀死对应的任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让workder监听到一次put操作, 创建一个租约让其稍后自动过期即可

	if leaseGrantResponse, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantResponse.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}
