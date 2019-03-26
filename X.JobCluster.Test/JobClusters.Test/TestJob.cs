using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters
{
    public class TestJob
    {
        public List<string> CurrentRule { get; set; }
        public virtual void Run()
        {
            Console.WriteLine("test job run rule :{0} ", string.Join(",", CurrentRule));
        }
    }
    
    
}
