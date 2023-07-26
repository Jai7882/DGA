package com.jia.dga.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.ds.bean.TDsTaskDefinition;
import com.jia.dga.ds.mapper.TDsTaskDefinitionMapper;
import com.jia.dga.ds.service.TDsTaskDefinitionService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-26
 */
@Service
@DS("ds")
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

    @Override
    public List<TDsTaskDefinition> getTaskDefinitionListForAssess(List<Long> taskCodeList) {
        // 根据taskCode从数据空出查询任务定义 并且从task_param中提取sql
        List<TDsTaskDefinition> taskDefinitionList = this.list(new QueryWrapper<TDsTaskDefinition>().in("code", taskCodeList));
        // 从task_param中提取sql
        for (TDsTaskDefinition definition : taskDefinitionList) {
            String sql = extractSQL(definition.getTaskParams());
            definition.setSql(sql);
        }
        return taskDefinitionList;
    }

    private String extractSQL(String taskParams) {
        JSONObject object = JSON.parseObject(taskParams);
        String shell = object.getString("rawScript");
        // 2 从sql中拆解 以with开头 若不满足则以insert开头
        int startIndex = shell.indexOf("with");
        if (startIndex == -1) {
            startIndex = shell.indexOf("insert");
        }
        if (startIndex == -1){
            return null;
        }

        // 结尾下标 从开始下标开始之后出现的第一个;下标 否则以"为条件判断结尾
        int endIndex = shell.indexOf(";",startIndex);
        if (endIndex == -1){
            endIndex = shell.indexOf("\"",startIndex);
        }
        return shell.substring(startIndex, endIndex);
    }
}
