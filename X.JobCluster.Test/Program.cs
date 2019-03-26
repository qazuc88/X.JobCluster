using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using X.JobClusters;
using X.JobClusters.Test;

namespace test
{
    class Program
    {
        static string ZookeeperConstr = "localhost:2181";//"localhost:2183,localhost:2182,localhost:2184,localhost:2185,localhost:2186,localhost:2181,localhost:2187";
        static void Main(string[] args)
        {
            TestJobClusterByConfig();

            return;
            TestJobCluster();
            //test();
            Console.WriteLine("please enter a key");
            ConsoleKey consoleKey = ConsoleKey.NoName;
            while ((consoleKey = Console.ReadKey().Key) != ConsoleKey.Q)
            {
                try
                {
                    CountdownWatcher countdownWatcher = new CountdownWatcher();
                    using (var helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher))
                    {
                        countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
                        if (consoleKey == ConsoleKey.D)
                        {
                            Console.WriteLine("delete root");
                            helper.DeletePath("/Root");
                        }
                        else
                        {
                            helper.RegisterService("hs", "hs1", "hs1data");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
            foreach (ZookeeperServiceHelper item in zookeeperHelpers)
            {
                try
                {
                    item.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }
        public static void test()
        {
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    break;
                    try
                    {
                        CountdownWatcher countdownWatcher = new CountdownWatcher();
                        using (var helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher))
                        {
                            countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
                            helper.DeletePath("/Root");
                            Console.WriteLine("delete root");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    Thread.Sleep(1);
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    break;
                    try
                    {
                        CountdownWatcher countdownWatcher = new CountdownWatcher();
                        using (var helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher))
                        {
                            countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
                            helper.RegisterService("hs", "hs1", "hs1data");
                            Console.WriteLine("RegisterService root");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    Thread.Sleep(1);
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    break;
                    try
                    {
                        CountdownWatcher countdownWatcher = new CountdownWatcher();
                        using (var helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher))
                        {
                            countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
                            helper.UpdateServiceData("hs", "hs1", DateTime.Now.ToString());
                            Console.WriteLine("UpdateServiceData {0}", "");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    Thread.Sleep(1);
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (int i = 0; i < 10; i++)
                {
                    try
                    {
                        CountdownWatcher countdownWatcher = new CountdownWatcher();
                        using (var helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher))
                        {
                            countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
                            var list = helper.GetServiceInstanceList("hs");
                            Console.WriteLine("GetServiceList {0}", JsonConvert.SerializeObject(list));
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    Thread.Sleep(1);
                }
            });
        }
        static System.Collections.Concurrent.ConcurrentBag<ZookeeperServiceHelper> zookeeperHelpers = new System.Collections.Concurrent.ConcurrentBag<ZookeeperServiceHelper>();
        private static ZookeeperServiceHelper TestCreateService()
        {
            CountdownWatcher countdownWatcher = new CountdownWatcher();
            ZookeeperServiceHelper helper = new ZookeeperServiceHelper(ZookeeperConstr, countdownWatcher, TimeSpan.FromSeconds(20));
            zookeeperHelpers.Add(helper);

            countdownWatcher.WaitConnected(TimeSpan.FromSeconds(5));
            helper.RegisterService("hs", "sf01", "testsf01data");
            var list = helper.GetServiceInstanceList("hs");
            Console.WriteLine("GetServiceList {0}", JsonConvert.SerializeObject(list));
            return helper;
        }
        private static void TestJobClusterByConfig()
        {
            var config = new
            {
                ServiceName = System.Configuration.ConfigurationManager.AppSettings["ServiceName"],
                ServiceInstanceId = System.Configuration.ConfigurationManager.AppSettings["ServiceInstanceId"],
                ServiceMillisecondsTimeout = int.Parse(System.Configuration.ConfigurationManager.AppSettings["ServiceMillisecondsTimeout"]),
                ZookeeperConstr = ConfigurationManager.AppSettings["ZookeeperConstr"],
                Ydids = ConfigurationManager.AppSettings["ydids"]
            };

            TestJobProxy testJobCluster = new TestJobProxy(config.ServiceName, config.ServiceInstanceId,config.Ydids,config.ZookeeperConstr);
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        testJobCluster.Run();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("{0}", ex.ToString());
                    }
                    finally
                    {
                        Thread.Sleep(config.ServiceMillisecondsTimeout);
                    }
                }
            });

            Console.WriteLine("Please enter a key to exit");
            Console.ReadKey();
        }
        private static void TestJobCluster()
        {
            for (int i = 0, j = 40; i < j; i++)
            {
                TestJobProxy testJobCluster = new TestJobProxy("tesths", "testhIinstance" + i);
                Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        try
                        {
                            testJobCluster.Run();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("{0}", ex.ToString());
                        }
                        finally
                        {
                            Thread.Sleep(3000);
                        }
                    }
                });
            }
        }
    }
}
