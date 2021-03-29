package com.atguigu.gmallpublisher1.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductMapper {

    @Select("select sum(order_amount) from product_stats_200923 where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmvTotal(int date) throws Exception;

    @Select("select tm_name,sum(order_amount) sum_amount from product_stats_200923 where toYYYYMMDD(stt)=#{date} group by tm_name order by sum_amount desc limit #{limit}")
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);
}
