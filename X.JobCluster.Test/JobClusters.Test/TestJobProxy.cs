using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters.Test
{
    public class TestJobProxy : TestJob, IDisposable
    {
        log4net.ILog Log = log4net.LogManager.GetLogger(typeof(TestJobProxy).Name);
        JobCluster jobCluster;

        public string ServiceName { get; set; }
        public string ServiceInstanceId { get; set; }
        public TestJobProxy(string ServiceName, string ServiceInstanceId, string ydids = "0,1,2,3,4,5,6,7,8,9",string zookeeperConstr= "localhost:2181")
        {
            this.ServiceInstanceId = ServiceInstanceId;
            this.ServiceName = ServiceName;


            var watcher = new CountdownWatcher();
            Func<IServiceHelper> funcIServiceHelper = () =>
            {
                IServiceHelper serviceHelper = new ZookeeperServiceHelper(zookeeperConstr, watcher, TimeSpan.FromSeconds(60));
                return serviceHelper;
            };
            EventInfo eventinfo = new EventInfo()
            {
                //GetIsRuning = () => { return true; },
                //OnChangeRunState = (isNeedWait) => { },
                //RunJob = () => { },
                WriteLog = (msg, ex, level) =>
                {
                    if (level >= JobClusterLogLevel.Waning || ex != null)
                    {
                        Log.Error(msg, ex);
                    }
                    else
                    {
                        Log.Info(msg);
                    }
                },
                CalculationRule = (list) =>
                {
                    string[] currentyds = ydids.Split(',');
                    return RuleHelper.CalculationRule(list, currentyds);
                },
                GetServiceHelper = funcIServiceHelper,
                CheckHealth = () => {
                    if (DateTime.Now.Minute % 3 == 0 && ServiceInstanceId.GetHashCode() % 2 == 0)
                    {
                        return false;
                    }
                    return true;

                }
            };
            jobCluster = new JobCluster(ServiceName, ServiceInstanceId, eventinfo);
            watcher.WaitConnected(TimeSpan.FromSeconds(5));
            jobCluster.Start();
        }

        public void Dispose()
        {
            if (jobCluster != null)
            {
                jobCluster.Stop();
            }
        }

        public override void Run()
        {
            if (jobCluster.WaitingSlaveVote())
            {
                return;
            }
            var rules = jobCluster.GetCurrentServiceRule(true);
            CurrentRule = RuleHelper.GetCurrentRule(rules, ServiceInstanceId);
            if (CurrentRule.Count == 0)
            {
                Log.Info(string.Format("服务[{0}.{1}]没有适用的规则", ServiceName, ServiceInstanceId));
                return;
            }

            Log.Info(string.Format("服务[{0}.{1}],使用获取到的规则【{2}】执行任务开始", ServiceName, ServiceInstanceId, string.Join(",", CurrentRule)));
            base.Run();
            Log.Info(string.Format("服务[{0}.{1}],使用获取到的规则【{2}】执行任务结束", ServiceName, ServiceInstanceId, string.Join(",", CurrentRule)));
        }
    }
}
