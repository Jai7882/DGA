package com.jia.dga.governance.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.common.constant.MetaConst;
import com.jia.dga.ds.bean.TDsTaskInstance;
import com.jia.dga.ds.service.TDsTaskInstanceService;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import com.jia.dga.meta.bean.TableMetaInfo;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * ClassName: TableOutputAssessor
 * Package: com.jia.dga.governance.assessor.quality
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 19:14
 * @Version 1.0
 */
@Component("TABLE_OUTPUT_MONITOR")
public class TableOutputAssessor extends Assessor {
    @Autowired
    TDsTaskInstanceService taskInstanceService;

//    @Override
//    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
//        List<TDsTaskInstance> taskInstanceList = assessParam.getTDsTaskInstanceList();
//        if (taskInstanceList==null){
//            return;
//        }
//        TDsTaskInstance lastTaskInstance = null;
//        for (TDsTaskInstance instance : taskInstanceList) {
//            if (instance.getState().toString().equals(MetaConst.TASK_STATE_FAILED)) {
//                return;
//            } else {
//                if (lastTaskInstance == null) {
//                    lastTaskInstance = instance;
//                } else {
//                    if (lastTaskInstance.getEndTime().compareTo(instance.getEndTime()) < 0) {
//                        // 更靠后的任务
//                        lastTaskInstance = instance;
//                    }
//                }
//            }
//        }
//        String assessDate = assessParam.getAssessDate();
//        JSONObject jsonObject = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
//        Integer days = jsonObject.getInteger("days");
//        Integer percent = jsonObject.getInteger("percent");
//        // 获得当日产出的时效
//        List<String> beforeDateList = new ArrayList<>();
//
//        for (int i = 1; i <= days; i++) {
//            Date assessDt = DateUtils.addDays(DateUtils.parseDate(assessDate, "yyyy-MM-dd"), -i);
//            beforeDateList.add(DateFormatUtils.format(assessDt, "yyyy-MM-dd"));
//        }
//        QueryWrapper<TDsTaskInstance> queryWrapper = new QueryWrapper<TDsTaskInstance>().in("date_format(start_time,'%Y-%m-%d')", beforeDateList)
//                .eq("state", MetaConst.TASK_STATE_SUCCESS)
//                .eq("name", assessParam.getTableMetaInfo().getSchemaName() + "." + assessParam.getTableMetaInfo().getTableName());
//        List<TDsTaskInstance> taskInstances = taskInstanceService.list(queryWrapper);
//        // 获得days天的平均产出时效 并于当日产出时效对比 大于计0分并差评
//        long sumDurationMs = 0L;
//        for (TDsTaskInstance instance : taskInstances) {
//            long duringTimeMs = instance.getEndTime().getTime() - instance.getStartTime().getTime();
//            sumDurationMs += duringTimeMs;
//        }
//        assert lastTaskInstance != null;
//        long lastDurationTime = lastTaskInstance.getEndTime().getTime() - lastTaskInstance.getStartTime().getTime();
//        long avgDuration = sumDurationMs / taskInstances.size();
//        if (lastDurationTime>avgDuration*(percent+100L)/100){
//            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
//            governanceAssessDetail.setAssessProblem("当日产出时效未超过前"+days+"天的平均值:"+avgDuration*(percent+100L)/100);
//        }
//
//    }

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

        //1    取当日最后成功的任务实例
        List<TDsTaskInstance> tDsTaskInstancesList = assessParam.getTDsTaskInstanceList();
        if(tDsTaskInstancesList==null){
            return;
        }

        TDsTaskInstance  lastTaskInstance=null;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstancesList) {
            if(tDsTaskInstance.getState().toString().equals(MetaConst.TASK_STATE_SUCCESS)){
                if(lastTaskInstance==null){
                    lastTaskInstance=tDsTaskInstance;
                }else if(lastTaskInstance.getEndTime().compareTo(tDsTaskInstance.getEndTime())<0){
                    lastTaskInstance= tDsTaskInstance;   //更靠后的任务
                }
            }
        }


        //2    取 days参数  percent参数
        String paramJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObj = JSON.parseObject(paramJson);
        Integer days = paramJsonObj.getInteger("days");
        BigDecimal paramPercent = paramJsonObj.getBigDecimal("percent");

        //3    根据days查询数据库 的成功的任务列表  // 参数 ：  1 要查询的日期列表  ,2  成功的, 3 当前考评表的
        //获得前n天日期列表
        List<String> beforeDateList =new ArrayList<>();
        String assessDate = assessParam.getAssessDate();
        Date assessDt = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        for (int i = 1; i <=days; i++) {
            Date beforeDate = DateUtils.addDays(assessDt, -i);
            String beforeDateString = DateFormatUtils.format(beforeDate, "yyyy-MM-dd");
            beforeDateList.add(beforeDateString);
        }
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();

        QueryWrapper<TDsTaskInstance> queryWrapper = new QueryWrapper<TDsTaskInstance>().in("date_format(start_time,'%Y-%m-%d')", beforeDateList)
                .eq("state", MetaConst.TASK_STATE_SUCCESS)
                .eq("name", tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());

        List<TDsTaskInstance> beforeDaysTaskInstanceList = taskInstanceService.list(queryWrapper);
        if(beforeDaysTaskInstanceList==null){
            return;
        }

        //4    求成功任务耗时的平均值
        long sumDurationMs=0L;
        for (TDsTaskInstance tDsTaskInstance : beforeDaysTaskInstanceList) {
            long durationMs = tDsTaskInstance.getEndTime().getTime() - tDsTaskInstance.getStartTime().getTime();
            sumDurationMs+=durationMs;
        }
        long avgDuration=sumDurationMs/beforeDaysTaskInstanceList.size();


        assert lastTaskInstance != null;
        long lastDurationMs = lastTaskInstance.getEndTime().getTime() - lastTaskInstance.getStartTime().getTime();

        //5   当日耗时- 平均耗时/ 平均耗时   对比percent
        BigDecimal curPercent = BigDecimal.valueOf(lastDurationMs-avgDuration).movePointRight(2).divide(BigDecimal.valueOf(avgDuration),2,BigDecimal.ROUND_HALF_UP);

        //6  如果超过 0分 差评

        if(curPercent.compareTo(paramPercent)>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("当日计算耗时超过前"+days+"天平均耗时"+curPercent);
        }

    }
}
