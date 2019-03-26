using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace X.JobClusters
{
    public class CountdownWatcher : IWatcher
    {
        volatile private bool connected;

        public void Process(WatchedEvent @event)
        {
            if (@event.State == KeeperState.SyncConnected)
            {
                connected = true;
            }
            else
            {
                connected = false;
            }
            Console.WriteLine("event:{0}", JsonConvert.SerializeObject(@event));
        }
        public void WaitConnected(TimeSpan timeSpan)
        {
            var timeout = DateTime.Now.Add(timeSpan);
            while (!connected && DateTime.Now < timeout)
            {
                Thread.Sleep(10);
            }
            if (!connected)
            {
                throw new TimeoutException("Did not connect Zookeeper");
            }
        }
    }
}
