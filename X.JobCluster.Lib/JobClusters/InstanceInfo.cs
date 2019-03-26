using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X.JobClusters
{
    public class InstanceInfo
    {
        public int DataLength { get; set; }
        public long EphemeralOwner { get; set; }
        public int Aversion { get; set; }
        public int Cversion { get; set; }
        public int Version { get; set; }
        public long Mtime { get; set; }
        public long Ctime { get; set; }
        public long Mzxid { get; set; }
        public long Czxid { get; set; }
        public long Pzxid { get; set; }
        public int NumChildren { get; set; }
        public bool Enable { get; set; }
        public string InstanceId { get; set; }
    }
}
