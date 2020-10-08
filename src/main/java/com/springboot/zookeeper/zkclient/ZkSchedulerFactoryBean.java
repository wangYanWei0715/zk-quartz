package com.springboot.zookeeper.zkclient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class ZkSchedulerFactoryBean extends SchedulerFactoryBean {

   private static final Logger LOG = LoggerFactory.getLogger(ZkSchedulerFactoryBean.class);

    private static CuratorFramework curatorFramework;
    private static String ZOOKEEPER_CONNECTION_STRING = "127.0.0.1:2181";

    private static final String baseName = "/zkBase";
    private LeaderLatch leaderLatch;



    public ZkSchedulerFactoryBean() throws Exception {
        this.setAutoStartup(false);
        //LeaderLatch 利用zk的临时有序节点特性 节点释放不能再进行lead选举
        //LeaderSelector 利用zk临时有序节点特性 节点释放后可以重新进行lead选举

        leaderLatch = new LeaderLatch(getCuratorFramework(),baseName);
        leaderLatch.addListener(new ZkJobLeaderLatchListener(getIp(),this));
        leaderLatch.start();
    }

    @Override
    protected void startScheduler(Scheduler scheduler, int startupDelay) throws SchedulerException {
       if (this.isAutoStartup()){
           super.startScheduler(scheduler, startupDelay);
       }
    }

    @Override
    public void destroy() throws SchedulerException {
        CloseableUtils.closeQuietly(leaderLatch);
        super.destroy();
    }

    public CuratorFramework getCuratorFramework(){
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000, 3);

        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZOOKEEPER_CONNECTION_STRING).retryPolicy(exponentialBackoffRetry).build();
        curatorFramework.start();
        return curatorFramework;

    }

    private String getIp(){
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return host;
    }

    class ZkJobLeaderLatchListener implements LeaderLatchListener{

        private String ip ;
        private ZkSchedulerFactoryBean zkSchedulerFactoryBean;

        public ZkJobLeaderLatchListener(String ip) {
            this.ip = ip;
        }

        public ZkJobLeaderLatchListener(String ip, ZkSchedulerFactoryBean zkSchedulerFactoryBean) {
            this.ip = ip;
            this.zkSchedulerFactoryBean = zkSchedulerFactoryBean;
        }

        @Override
        public void isLeader() {
            LOG.info("这个是leader,这个的ip是:"+ip);
            zkSchedulerFactoryBean.setAutoStartup(true);
            zkSchedulerFactoryBean.start();
        }

        @Override
        public void notLeader() {
            LOG.info("这个不是leader,这个的ip是:"+ip);
            zkSchedulerFactoryBean.setAutoStartup(false);
            zkSchedulerFactoryBean.stop();
        }
    }


}

