using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace X.JobClusters
{

    public class JobCluster
    {
        private IServiceHelper serviceHelper;
        private int defaultsleepmilliseconds;
        private Task monitorTask;
        private CancellationTokenSource monitorTaskToken = new CancellationTokenSource();
        private string serviceName;
        private string serviceInstanceId;
        private EventInfo eventInfo;

        public JobCluster(string serviceName, string serviceInstanceId, EventInfo eventInfo, int defaultsleepmilliseconds = 10000)
        {
            this.serviceName = serviceName;
            this.serviceInstanceId = serviceInstanceId;
            this.eventInfo = eventInfo;
            this.defaultsleepmilliseconds = defaultsleepmilliseconds;
            this.serviceHelper = eventInfo.GetServiceHelper();
        }
        private void Check()
        {
            if (eventInfo == null)
            {
                throw new NullReferenceException("eventInfo is null");
            }
            //if (eventInfo.OnChangeRunState == null)
            //{
            //    throw new NullReferenceException("eventInfo.OnChangeRunState is null");
            //}
            //if (eventInfo.RunJob == null)
            //{
            //    throw new NullReferenceException("eventInfo.RunJob is null");
            //}
            //if (eventInfo.GetIsRuning == null)
            //{
            //    throw new NullReferenceException("eventInfo.GetIsRuning is null");
            //}
            if (eventInfo.WriteLog == null)
            {
                throw new NullReferenceException("eventInfo.WriteLog is null");
            }
            if (eventInfo.CalculationRule == null)
            {
                throw new NullReferenceException("eventInfo.CalculationRule is null");
            }
            if (eventInfo.GetServiceHelper == null)
            {
                throw new NullReferenceException("eventInfo.GetServiceHelper is null");
            }
            if (eventInfo.CheckHealth == null)
            {
                throw new NullReferenceException("eventInfo.CheckHealth is null");
            }
        }
        public void Start()
        {
            Check();
            monitorTask = Task.Factory.StartNew(
                () =>
                {
                    MonitorCluster();
                }, monitorTaskToken.Token);
        }
        public void Stop()
        {
            SetServiceEnable(false);
            if (serviceHelper != null)
            {
                serviceHelper.Dispose();
            }
            if (monitorTask != null)
            {
                if (!monitorTask.IsCompleted)
                {
                    monitorTaskToken.Cancel();
                }
                if (monitorTask.IsCompleted)
                {
                    monitorTask.Dispose();
                }
            }
        }
        public virtual void RegisterService()
        {
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】不存在,正在注册", serviceName, serviceInstanceId));
            serviceHelper.RegisterService(serviceName, serviceInstanceId, string.Empty);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经注册", serviceName, serviceInstanceId));
            SetServiceEnable(true);
        }

        public virtual void SetServiceRule(string rule, bool ismaster)
        {
            if (!ismaster)
            {
                DeleteServiceRule(true);
            }
            else
            {
                DeleteServiceRule(false);
            }
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在注册规则【{2}】【{3}】", serviceName, serviceInstanceId, ismaster ? "master" : "slave", rule));
            serviceHelper.SetServiceRule(serviceName, rule, ismaster);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经注册规则【{2}】【{3}】", serviceName, serviceInstanceId, ismaster ? "master" : "slave", rule), null, JobClusterLogLevel.Waning);
            if (!ismaster)
            {
                ClearServiceSlaveRuleVote();
            }
        }
        public virtual void SetServiceEnable(bool enable)
        {
            serviceHelper.SetServiceEnable(serviceName, serviceInstanceId, enable);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经设置为{2}", serviceName, serviceInstanceId, enable ? "启用" : "禁用"), null, JobClusterLogLevel.Waning);
        }

        public virtual string GetCurrentServiceRule(bool ismaster)
        {
            var rules = serviceHelper.GetServiceRule(serviceName, ismaster);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,获取到规则为{2}", serviceName, serviceInstanceId, string.Join(",", rules)));
            return rules;
        }
        public virtual void DeleteServiceRule(bool isMaster)
        {
            if (!isMaster)
            {
                ClearServiceSlaveRuleVote();
            }
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在删除{2}规则投票", serviceName, serviceInstanceId, isMaster?"master":"候选"));
            serviceHelper.DeleteServiceRule(serviceName,isMaster);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经删除{2}规则投票", serviceName, serviceInstanceId, isMaster ? "master" : "候选"), null, JobClusterLogLevel.Waning);
        }
        public virtual void ClearServiceSlaveRuleVote()
        {
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在清空候选规则投票", serviceName, serviceInstanceId));
            serviceHelper.ClearServiceSlaveRuleVote(serviceName);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经清空候选规则投票", serviceName, serviceInstanceId), null, JobClusterLogLevel.Waning);
        }

        public virtual void AddServiceSlaveRuleVote()
        {
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在给候选规则投票", serviceName, serviceInstanceId));
            serviceHelper.AddServiceSlaveRuleVote(serviceName, serviceInstanceId);
            eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,已经给候选规则投票", serviceName, serviceInstanceId), null, JobClusterLogLevel.Waning);
        }
        public virtual bool WaitingSlaveVote()
        {
            string currentSlaveServiceRule = GetCurrentServiceRule(false);
            if (currentSlaveServiceRule != null)//投票环节
            {
                var voteinstanceids = serviceHelper.GetServiceSlaveRuleVote(serviceName);
                if (!voteinstanceids.Exists(x => x == serviceInstanceId))
                {
                    AddServiceSlaveRuleVote();
                }
                eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在等待候选规则投票结束", serviceName, serviceInstanceId));
                return true;
            }
            else
            {
                return false;
            }

        }
        public virtual void GenerateRule(List<InstanceInfo> serviceInstanceList)
        {
            if (serviceInstanceList.Count > 0)
            {
                var masterInstance = serviceInstanceList.Where(x => x.Enable).OrderBy(x => x.Ctime).FirstOrDefault();
                if (eventInfo.CheckHealth())
                {
                    var currentServiceInstance = serviceInstanceList.Find(x => x.InstanceId == serviceInstanceId);
                    if (currentServiceInstance != null && currentServiceInstance.Enable == false)
                    {
                        if (serviceInstanceList.Count(x => x.Enable) > 1)
                        {
                            currentServiceInstance.Enable = false;
                            SetServiceEnable(false);
                        }
                    }
                }
                eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,获取的master服务【{0}.{2}】", serviceName, serviceInstanceId, masterInstance == null ? "" : masterInstance.InstanceId));

                #region 发起候选或更新主规则
                if (masterInstance != null && masterInstance.InstanceId == serviceInstanceId)//只有第一个服务器才能发起候选或更新主规则
                {
                    string currentSlaveServiceRule = GetCurrentServiceRule(false);
                    if (currentSlaveServiceRule != null)//候选全员投票通过
                    {
                        string rule = eventInfo.CalculationRule(serviceInstanceList);
                        if (rule != currentSlaveServiceRule)//规则变更重新投票
                        {
                            SetServiceRule(rule, false);
                        }
                        else
                        {
                            var voteinstanceids = serviceHelper.GetServiceSlaveRuleVote(serviceName);
                            bool isVoted = true;
                            foreach (var item in voteinstanceids)
                            {
                                if (!serviceInstanceList.Exists(x => x.Enable = true && x.InstanceId == item))
                                {
                                    isVoted = false;
                                }
                            }
                            if (isVoted)//全员投票通过，更新主规则
                            {
                                SetServiceRule(rule, true);
                            }
                        }
                    }
                    else
                    {
                        string masterRule = serviceHelper.GetServiceRule(serviceName, true);
                        string rule = eventInfo.CalculationRule(serviceInstanceList);
                        eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,正在比对规则,new rule:{2},server rulelist:{3}", serviceName, serviceInstanceId, rule, string.Join("|", masterRule)));

                        if (masterRule != rule)
                        {
                            SetServiceRule(rule, false);
                        }
                    }
                }
                #endregion
            }
        }

        private void HandlerException(Exception exception)
        {
            if (exception is ZooKeeperNet.KeeperException.SessionExpiredException)
            {
                serviceHelper.Dispose();
                eventInfo.WriteLog(string.Format("当前服务【{0}.{1}】,会话过期,正在重连", serviceName, serviceInstanceId));
                this.serviceHelper = eventInfo.GetServiceHelper();
            }
        }
        public virtual void MonitorCluster()
        {
            while (!monitorTaskToken.IsCancellationRequested)
            {
                try
                {
                    var serviceInstanceList = serviceHelper.GetServiceInstanceList(serviceName);
                    eventInfo.WriteLog(string.Format("当前服务【{0}】,获取到实例:{1}", serviceName, string.Join(",", serviceInstanceList.Select(x => string.Format("InstanceId:{0},enable:{1}", x.InstanceId, x.Enable)))));
                    if (serviceInstanceList.Count == 0 || !serviceInstanceList.Exists(x => x.InstanceId == serviceInstanceId))
                    {
                        RegisterService();
                        serviceInstanceList = serviceHelper.GetServiceInstanceList(serviceName);
                    }
                    if (serviceInstanceList.Exists(x => x.InstanceId == serviceInstanceId && x.Enable == false))
                    {
                        SetServiceEnable(true);
                        serviceInstanceList.Find(x => x.InstanceId == serviceInstanceId).Enable = true;
                    }
                    GenerateRule(serviceInstanceList);

                }
                catch (Exception ex)
                {
                    eventInfo.WriteLog(ex.Message, ex);
                    HandlerException(ex);
                }
                finally
                {
                    Thread.Sleep(defaultsleepmilliseconds);
                }
            }
        }
    }
}
