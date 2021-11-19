package com.oysq.flink.project.udf;

import com.oysq.flink.project.domain.Access;
import com.oysq.flink.project.utils.GaoDeUtil;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Map;

public class GaodeLocationMapFunction extends RichMapFunction<Access, Access> {
    @Override
    public Access map(Access access) throws Exception {
        try {
            Map message = GaoDeUtil.getMessage(access.getIp());
            if (message != null) {
                access.setProvince(String.valueOf(message.get("province")));
                access.setCity(String.valueOf(message.get("city")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return access;
    }
}
