using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters
{
    public class RuleHelper
    {
        public static string CalculationRule(List<InstanceInfo> instanceInfos, params string[] partition)
        {
            string[] currentyds = partition;
            Dictionary<string, List<string>> dic = new Dictionary<string, List<string>>();
            var orderbylist = instanceInfos.OrderBy(x => x.InstanceId).ToList();
            var activityList = orderbylist.FindAll(x => x.Enable == true);
            if (activityList.Count == 0)
            {
                activityList = orderbylist;
            }
            int avgcount = currentyds.Length / activityList.Count;
            for (int i = 0, k = 0, j = currentyds.Length; i < j; i++)
            {
                var ydid = currentyds[i];
                List<string> kv = null;


                string instanceId = activityList[k].InstanceId;
                if (dic.ContainsKey(instanceId))
                {
                    kv = dic[instanceId];
                }
                else
                {
                    kv = new List<string>();
                    dic[instanceId] = kv;
                }
                kv.Add(ydid);
                k++;
                if (k >= activityList.Count)
                {
                    k = 0;
                }
            }
            var rule = JsonConvert.SerializeObject(dic);
            return rule;
        }
        public static List<string> GetCurrentRule(string masterRule, string currentServiceInstanceId)
        {
            if (masterRule != null)
            {
                var dic = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(masterRule);
                if (dic.ContainsKey(currentServiceInstanceId))
                {
                    var currentrule = dic[currentServiceInstanceId];
                    return currentrule;
                }
            }
            return new List<string>(0);
        }
    }
}
