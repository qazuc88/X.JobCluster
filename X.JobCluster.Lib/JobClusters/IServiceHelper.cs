using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters
{
    public interface IServiceHelper : IDisposable
    {
        void RegisterService(string servicename, string serviceInstanceId, string servicedata);
        void SetServiceEnable(string servicename, string serviceInstanceId, bool enable);
        List<InstanceInfo> GetServiceInstanceList(string servicename);
        void SetServiceRule(string serviceName, string rule, bool ismaster);
        string GetServiceRule(string serviceName, bool ismaster);
        void DeleteServiceRule(string serviceName,bool isMaster);
        void ClearServiceSlaveRuleVote(string serviceName);
        void AddServiceSlaveRuleVote(string serviceName, string serviceInstanceId);
        List<string> GetServiceSlaveRuleVote(string serviceName);
    }
}
