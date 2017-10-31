package cn.lijie.controller;

import cn.lijie.kafka.MyProducer;
import cn.lijie.hbase.HbaseUtils;
import cn.touna.profile.present.api.ProfileService;
import cn.touna.profile.present.entity.ProFileQuery;
import cn.touna.profile.present.entity.Response;
import cn.touna.profile.present.entity.ResponseData;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;


/**
 * 临时给测试用的一个工程
 */
@Controller
@RequestMapping("/dubbo")
public class MyController {

    @Autowired
    public ProfileService profileService;


    @RequestMapping(value = "/api")
    @ResponseBody
    public Response<List<ResponseData>> getFileStorageOverview(String idcard, String mobile) throws Exception {

        ProFileQuery pfq = new ProFileQuery(idcard, mobile);
        Response<List<ResponseData>> listResponse = profileService.queryUserProfile(pfq);
        return listResponse;
    }

    @RequestMapping(value = "/send")
    @ResponseBody
    public String sendKfk(String topic, String message) throws Exception {
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(message)) {
            return "topic和message都不能为空";
        }
        MyProducer.produce(topic, message);
        return "ok";
    }

    @RequestMapping(value = "/tranc")
    @ResponseBody
    public String tran(String table) throws Exception {
        if (StringUtils.isBlank(table)) {
            return "表名不能为空！";
        }
        HbaseUtils.trancate(table);
        return "ok";
    }

    @RequestMapping(value = "/delete")
    @ResponseBody
    public String delete(String tablename, String idcard, String mobile, int flag) throws Exception {

        if (flag != 1) {
            flag = 0;
        }

        if (StringUtils.isNotBlank(idcard)) {
            HbaseUtils.delete(tablename, idcard, flag);
        }

        if (StringUtils.isNotBlank(mobile)) {
            HbaseUtils.delete(tablename, mobile, flag);
        }

        if (StringUtils.isNotBlank(idcard) && StringUtils.isNotBlank(mobile)) {
            HbaseUtils.delete(tablename, mobile + idcard, flag);
        }

        return "ok";
    }


}