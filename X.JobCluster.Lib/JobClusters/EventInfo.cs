using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters
{
    public class EventInfo
    {
        public delegate void RunBodyHandler();
        public delegate bool GetIsRuningHandler();
        public delegate void ChangeRunStateHandler(bool isNeedWait);
        public delegate void LogHandler(string msg, Exception exception = null, JobClusterLogLevel level = JobClusterLogLevel.Info);
        public delegate string CalculationRuleHandler(List<InstanceInfo> instances);
        //public RunBodyHandler RunJob;
        //public GetIsRuningHandler GetIsRuning;
        public LogHandler WriteLog;
        //public ChangeRunStateHandler OnChangeRunState;
        public CalculationRuleHandler CalculationRule;
        public Func<IServiceHelper> GetServiceHelper;
        public Func<bool> CheckHealth;
    }
}
