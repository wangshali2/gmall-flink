package com.atguigu.gmallpublisher1.service.impl;

import com.atguigu.gmallpublisher1.mapper.ProductMapper;
import com.atguigu.gmallpublisher1.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    private ProductMapper productMapper;

    @Override
    public BigDecimal getGmvTotal(int date) throws Exception {
        return productMapper.selectGmvTotal(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {

        HashMap<String, BigDecimal> result = new HashMap<>();

        //查询ClickHouse获取按照品牌分组的交易额数据
        //	List{
        //		Map[(tm_name->小米),(sum_amount->159),(c->)]
        //		Map[(tm_name->苹果),(sum_amount->180),(c->)]
        //	}
        List<Map> mapList = productMapper.selectGmvByTm(date, limit);

        //Map[(小米->159),(苹果->180),....]
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("sum_amount"));
        }

        //返回数据
        return result;
    }
}
