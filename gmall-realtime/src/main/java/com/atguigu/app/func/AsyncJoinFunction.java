package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

public interface AsyncJoinFunction<T> {

    String getKey(T t);

    void join(T t, JSONObject dimJSON);

}
