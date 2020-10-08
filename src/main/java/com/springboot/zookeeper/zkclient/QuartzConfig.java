package com.springboot.zookeeper.zkclient;

import org.quartz.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QuartzConfig {


    @Bean
    public ZkSchedulerFactoryBean zkSchedulerFactoryBean() throws Exception {
        ZkSchedulerFactoryBean zkSchedulerFactoryBean =  new ZkSchedulerFactoryBean();
        zkSchedulerFactoryBean.setJobDetails(jobDetail());
        zkSchedulerFactoryBean.setTriggers(trigger());
        return zkSchedulerFactoryBean;

    }


    @Bean
    public JobDetail jobDetail(){
        return JobBuilder.newJob(QuartzJob.class).storeDurably().build();
    }

    @Bean
    public Trigger trigger(){
        SimpleScheduleBuilder simpleScheduleBuilder =
                SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(1)
                        .repeatForever();
        return TriggerBuilder.newTrigger()
                .forJob(jobDetail())
                .withSchedule(simpleScheduleBuilder)
                .build();
    }




}
