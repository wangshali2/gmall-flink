package com.atguigu.gmallpublisher1.controller;

import com.atguigu.gmallpublisher1.service.ProductService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private ProductService productService;

    /**
     * @return {
     * "status": 0,
     * "msg": "",
     * "data": {
     * "categories": [
     * "苹果",
     * "三星",
     * "华为",
     * "oppo",
     * "vivo",
     * "小米74"
     * ],
     * "series": [
     * {
     * "name": "手机品牌",
     * "data": [
     * 8909,
     * 9071,
     * 5387,
     * 7787,
     * 9242,
     * 9356
     * ]
     * }
     * ]
     * }
     * }
     */
    @RequestMapping("/trademark")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        if (date == 0) {
            date = getToday();
        }

        //查询按照品牌分组的交易额数据
        Map gmvByTm = productService.getGmvByTm(date, limit);

        //创建集合分别用于存放品牌和交易额数据
        ArrayList<Object> tmList = new ArrayList<>();
        ArrayList<Object> gmvList = new ArrayList<>();

        for (Object key : gmvByTm.keySet()) {
            tmList.add(key);
            gmvList.add(gmvByTm.get(key));
        }

        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(tmList, "\",\"") +
                "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"商品品牌\"," +
                "        \"data\": [" +
                StringUtils.join(gmvList,",") +
                "        ]" +
                "      }" +
                "    ]" +
                "  }" +
                "}";
    }

    /**
     * 访问路径:/api/sugar/gmv
     *
     * @return :{
     * "status": 0,
     * "msg": "",
     * "data": 1201078.570281297
     * }
     */
    @RequestMapping("/gmv")
    public String getGmvTotal(@RequestParam(value = "date", defaultValue = "0") int date) throws Exception {

        if (date == 0) {
            //没有指定日期,则使用今天作为日期字段
            date = getToday();
        }

        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + productService.getGmvTotal(date) +
                "}";
    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        String dateStr = sdf.format(ts);
        return Integer.parseInt(dateStr);
    }

}
