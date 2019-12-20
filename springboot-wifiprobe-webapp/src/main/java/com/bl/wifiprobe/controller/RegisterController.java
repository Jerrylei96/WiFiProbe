package com.bl.wifiprobe.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
public class RegisterController {

    @ResponseBody
    @RequestMapping(value="/register",method = RequestMethod.POST,produces="application/json;charset=UTF-8")
    public String register(@RequestBody JSONObject jsonParam){
        System.out.println(jsonParam.toJSONString());
        JSONObject result= new JSONObject();

        result.put("status","true");
        return  result.toJSONString();
    }
}
