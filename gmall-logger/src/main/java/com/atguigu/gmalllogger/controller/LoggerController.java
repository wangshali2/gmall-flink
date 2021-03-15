package com.atguigu.gmalllogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController // = @Controller + @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")
//    @ResponseBody
    public String test1() {
        System.out.println("Success");
        return "Success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam(value = "age", defaultValue = "18") String age) {
        System.out.println("Success");
        return name + ":" + age;
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String logStr) {

        //将数据写入日志文件
        log.info(logStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", logStr);

        return "Success";
    }


}
