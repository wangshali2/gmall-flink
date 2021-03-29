package com.atguigu.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ProductService {

    //获取交易总额
    BigDecimal getGmvTotal(int date) throws Exception;

    //按照品牌获取TopN的交易额
    Map getGmvByTm(int date, int limit);

}
