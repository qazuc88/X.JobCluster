using AutoMapper;
using Newtonsoft.Json;
using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet;
using static ZooKeeperNet.KeeperException;

namespace X.JobClusters
{

    public class NullWatcher : IWatcher
    {
        public readonly static NullWatcher Instance = new NullWatcher();
        //public override Task process(WatchedEvent @event)
        //{
        //    return Task.CompletedTask;
        //}

        public void Process(WatchedEvent @event)
        {

        }
    }


    public class ZookeeperServiceHelper : IServiceHelper
    {
        private static MapperConfiguration mapperconfig;
        private static Mapper mapper;
        static ZookeeperServiceHelper()
        {
            mapperconfig = new MapperConfiguration(cfg => cfg.CreateMap<Stat, InstanceInfo>());
            mapper = new Mapper(mapperconfig);
        }
        private string ZooKeeperConnstr;
        private ZooKeeper zooKeeper;
        private IWatcher watcher;

        public ZookeeperServiceHelper(string zooKeeperConnstr, IWatcher watcher = null, TimeSpan? sessionTimeout = null)
        {
            ZooKeeperConnstr = zooKeeperConnstr;
            if (sessionTimeout == null)
            {
                sessionTimeout = TimeSpan.FromSeconds(60);
            }
            zooKeeper = CreateClient(sessionTimeout.Value, watcher);
        }
        protected ZooKeeper CreateClient(TimeSpan sessionTimeout, IWatcher watcher = null)
        {
            if (watcher == null)
            {
                watcher = NullWatcher.Instance;
            }
            this.watcher = watcher;
            ZooKeeper zk = new ZooKeeper(ZooKeeperConnstr, sessionTimeout, watcher);
            return zk;
        }
        public string GetPathForService(string servicename)
        {
            return string.Format("/Root/Service/{0}/Instance", servicename);
        }
        public string GetPathForServiceInstance(string servicename, string serviceInstanceId)
        {
            return string.Format("/Root/Service/{0}/Instance/{1}", servicename, serviceInstanceId);
        }
        public string GetPathForServiceInstanceEnable(string servicename, string serviceInstanceId)
        {
            return string.Format("/Root/Service/{0}/Instance/{1}/Enable", servicename, serviceInstanceId);
        }
        public string GetPathForServiceRuleRoot(string servicename)
        {
            return string.Format("/Root/Service/{0}/Rule", servicename);
        }
        public string GetPathForServiceRuleInstance(string servicename, bool ismaster)
        {
            return string.Format("/Root/Service/{0}/Rule/{1}", servicename, ismaster ? "master" : "slave");
        }
        public string GetPathForServiceRuleVoteInstance(string servicename, bool ismaster, string serviceInstanceId)
        {
            return string.Format("/Root/Service/{0}/Rule/{1}/Vote/{2}", servicename, ismaster ? "master" : "slave", serviceInstanceId);
        }
        public string GetPathForServiceRuleVote(string servicename, bool ismaster)
        {
            return string.Format("/Root/Service/{0}/Rule/{1}/Vote", servicename, ismaster ? "master" : "slave");
        }
        public void RegisterService(string servicename, string serviceInstanceId, string servicedata)
        {
            string path = GetPathForServiceInstance(servicename, serviceInstanceId);
            CreatePaths(path);
            zooKeeper.SetData(path, servicedata.GetBytes(), -1);
        }

        public void SetServiceEnable(string servicename, string serviceInstanceId, bool enable)
        {

            string enablepath = GetPathForServiceInstanceEnable(servicename, serviceInstanceId);

            var enablestat = zooKeeper.Exists(enablepath, false);
            string enablestr = enable.ToString().ToLower();
            if (enablestat == null)
            {
                zooKeeper.Create(enablepath, enablestr.GetBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
            }
            else
            {
                zooKeeper.SetData(enablepath, enablestr.GetBytes(), -1);
            }
        }
        public void UpdateServiceData(string servicename, string serviceInstanceId, string servicedata)
        {
            string path = GetPathForServiceInstance(servicename, serviceInstanceId);
            zooKeeper.SetData(path, servicedata.GetBytes(), -1);
        }
        public List<InstanceInfo> GetServiceInstanceList(string servicename)
        {
            List<InstanceInfo> ls = new List<InstanceInfo>();
            string path = GetPathForService(servicename);
            var exists = zooKeeper.Exists(path, false);
            if (exists != null)
            {
                var list = zooKeeper.GetChildren(path, false);
                foreach (var item in list)
                {
                    string pathc = GetPathForServiceInstance(servicename, item);
                    var stat = zooKeeper.Exists(pathc, false);
                    if (stat != null)
                    {
                        var instance = mapper.DefaultContext.Mapper.Map<InstanceInfo>(stat);
                        string datapath = GetPathForServiceInstanceEnable(servicename, item);
                        bool enable = false;
                        if (zooKeeper.Exists(datapath, false) != null)
                        {
                            byte[] enablebts = zooKeeper.GetData(datapath, false, null);
                            enable = enablebts != null && UTF8Encoding.UTF8.GetString(enablebts) == "true";
                        }
                        instance.InstanceId = item;
                        instance.Enable = enable;
                        ls.Add(instance);
                    }
                }
            }
            return ls;
        }
        public void DeletePath(string path)
        {

            var stat = zooKeeper.Exists(path, false);
            if (stat != null)
            {
                if (stat.NumChildren > 0)
                {
                    var result = zooKeeper.GetChildren(path, false);
                    foreach (var item in result)
                    {
                        string pathc = string.Format("{0}/{1}", path, item);
                        var resultc = zooKeeper.Exists(pathc, false);
                        if (resultc != null)
                        {
                            if (resultc.NumChildren >= 0)
                            {
                                Console.WriteLine("delete child {0}", pathc);
                                DeletePath(pathc);
                            }
                        }
                        Console.WriteLine(item);
                    }
                }
                Console.WriteLine("delete {0}", path);
                zooKeeper.Delete(path, -1);
            }
        }
        public void CreatePaths(string path)
        {
            string[] nodepaths = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            StringBuilder currenntpath = new StringBuilder();
            foreach (var item in nodepaths)
            {
                currenntpath.Append("/").Append(item);
                string createpath = currenntpath.ToString();
                if (zooKeeper.Exists(createpath, false) == null)
                {
                    zooKeeper.Create(createpath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
        }
        private void WriteLog(Exception exception)
        {
            Console.WriteLine(exception);
        }

        public void Dispose()
        {
            // Console.WriteLine(" dispose state:{0}", zooKeeper.State.ToString());
            var task = Task.Factory.StartNew(() =>
              {
                  zooKeeper.Dispose();
              });
            task.Wait(5000);
        }

        public void SetServiceRule(string serviceName, string rule, bool ismaster)
        {
            string rulepath = GetPathForServiceRuleInstance(serviceName, ismaster);
            CreatePaths(rulepath);
            zooKeeper.SetData(rulepath, rule.GetBytes(), -1);
        }

        public string GetServiceRule(string serviceName, bool ismaster)
        {
            string result = null;
            string rulepath = GetPathForServiceRuleInstance(serviceName, ismaster);
            if (zooKeeper.Exists(rulepath, false) != null)
            {
                string pathc = rulepath;
                byte[] bts = zooKeeper.GetData(pathc, false, null);
                if (bts != null)
                {
                    result = UTF8Encoding.UTF8.GetString(bts);
                }
            }
            return result;
        }
        public void DeleteServiceRule(string serviceName, bool isMaster)
        {
            string path = GetPathForServiceRuleInstance(serviceName, isMaster);
            if (zooKeeper.Exists(path, false) != null)
            {
                zooKeeper.Delete(path, -1);
            }
        }
        public void ClearServiceSlaveRuleVote(string serviceName)
        {
            string slavepath = GetPathForServiceRuleVote(serviceName, false);
            if (zooKeeper.Exists(slavepath, false) != null)
            {
                var list = zooKeeper.GetChildren(slavepath, false);
                foreach (var item in list)
                {
                    zooKeeper.Delete(string.Format("{0}/{1}", slavepath, item), -1);
                }
                zooKeeper.Delete(slavepath, -1);
            }
        }
        public void AddServiceSlaveRuleVote(string serviceName, string serviceInstanceId)
        {
            string votepath = GetPathForServiceRuleVote(serviceName, false);
            if (zooKeeper.Exists(votepath, false) == null)
            {
                zooKeeper.Create(votepath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
            }
            string votepathInstance = GetPathForServiceRuleVoteInstance(serviceName, false, serviceInstanceId);
            if (zooKeeper.Exists(votepath, false) == null)
            {
                zooKeeper.Create(votepath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
            }
        }
        public List<string> GetServiceSlaveRuleVote(string serviceName)
        {
            List<string> result = new List<string>();

            string votepath = GetPathForServiceRuleVote(serviceName, false);
            var exists = zooKeeper.Exists(votepath, false);
            if (exists != null)
            {
                var list = zooKeeper.GetChildren(votepath, false);
                result.AddRange(list);
            }
            return result;
        }
    }
}
