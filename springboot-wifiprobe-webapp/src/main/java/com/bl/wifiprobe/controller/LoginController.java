package com.bl.wifiprobe.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Controller
public class LoginController {
    @ResponseBody
    @RequestMapping(value="/login",method = RequestMethod.POST,produces="application/json;charset=UTF-8")
    public String login(@RequestBody JSONObject jsonParam){
        System.out.println(jsonParam.toJSONString());
        JSONObject result= new JSONObject();


        result.put("status","true");
        return  result.toJSONString();
    }
}
