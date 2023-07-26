package com.jia.dga.governance.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.common.util.SpringBeanProvider;
import com.jia.dga.ds.bean.TDsTaskDefinition;
import com.jia.dga.ds.bean.TDsTaskInstance;
import com.jia.dga.ds.service.TDsTaskDefinitionService;
import com.jia.dga.ds.service.TDsTaskInstanceService;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import com.jia.dga.governance.bean.GovernanceMetric;
import com.jia.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.jia.dga.governance.service.GovernanceAssessDetailService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jia.dga.governance.service.GovernanceMetricService;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.service.TableMetaInfoService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-25
 */
@Service
@DS("dga")
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {


    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceMetricService governanceMetricService;

    @Autowired
    SpringBeanProvider springBeanProvider;

    @Autowired
    TDsTaskDefinitionService tDsTaskDefinitionService;

    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    public void allMetricAssess(String assessDate) {
        remove(new QueryWrapper<GovernanceAssessDetail>().eq("assess_date", assessDate));
        // 1 查询出要考评的表 含最新元数据和辅助信息 List<TableMetaInfo>
        // 为了避免循环查询数据库 使用join方式查询数据
        List<TableMetaInfo> tableGovernanceMetricList = tableMetaInfoService.getTableMetaWithExtraList();

        // 2 查询出要考评的指标列表 List<GovernanceMetric>
        List<GovernanceMetric> list = governanceMetricService.list(new QueryWrapper<GovernanceMetric>().eq("is_disabled", "0"));

        // 3 DS信息
        // 3.1 任务实例 考评日期：当日
        List<TDsTaskInstance> taskInstances = tDsTaskInstanceService.list(new QueryWrapper<TDsTaskInstance>().eq("date_format(start_time,'%Y-%m-%d')", assessDate));
        List<Long> taskCodeList = taskInstances.stream().map(TDsTaskInstance::getTaskCode).collect(Collectors.toList());
        Map<String,List<TDsTaskInstance>> taskInstanceMap = new HashMap<>(256);
        for (TDsTaskInstance taskInstance : taskInstances) {
            List<TDsTaskInstance> tDsTaskInstances = taskInstanceMap.get(taskInstance.getName());
            if (tDsTaskInstances==null){
                tDsTaskInstances = new ArrayList<>();
            }
            tDsTaskInstances.add(taskInstance);
            taskInstanceMap.put(taskInstance.getName(),tDsTaskInstances);
        }
        // 3.2 任务定义 全部
        //List<TDsTaskDefinition> taskDefinitionList = tDsTaskDefinitionService.list(new QueryWrapper<TDsTaskDefinition>().in("code", taskCodeList));
        // 根据taskCode从数据空出查询任务定义 并且从task_param中提取sql
        List<TDsTaskDefinition> taskDefinitionList = tDsTaskDefinitionService.getTaskDefinitionListForAssess(taskCodeList);
        Map<String, TDsTaskDefinition> definitionMap = taskDefinitionList.stream().collect(Collectors.toMap(TDsTaskDefinition::getName, tDsTaskDefinition -> tDsTaskDefinition));


        // 4 每张表的每个指标逐一进行考评 通过两个列表的双层循环 List<GovernanceAssessDetail>
        List<GovernanceAssessDetail> governanceAssessDetails = new ArrayList<>();
        for (TableMetaInfo tableMetaInfo : tableGovernanceMetricList) {
            for (GovernanceMetric governanceMetric : list) {
                String skipAssessTables = governanceMetric.getSkipAssessTables();
                // 跳过白名单
                if (skipAssessTables != null ) {
                    String[] split = skipAssessTables.split(",");
                    boolean flag = false;
                    for (String s : split) {
                        if (tableMetaInfo.getTableName().equals(s)) {
                            flag = true;
                            break;
                        }
                    }
                    if (flag) {
                        continue;
                    }
                }
                Assessor assessor = springBeanProvider.getBean(governanceMetric.getMetricCode(), Assessor.class);
                AssessParam param = new AssessParam();
                // 任务定义
                TDsTaskDefinition definition = definitionMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
                param.setTDsTaskDefinition(definition);
                // 任务实例
                List<TDsTaskInstance> instances = taskInstanceMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
                param.setTDsTaskInstanceList(instances);

                param.setAssessDate(assessDate);
                param.setGovernanceMetric(governanceMetric);
                param.setTableMetaInfo(tableMetaInfo);
                param.setTableMetaInfoList(tableGovernanceMetricList);
                GovernanceAssessDetail governanceAssessDetail = assessor.metricAssess(param);
                System.out.println(governanceAssessDetail);
                governanceAssessDetails.add(governanceAssessDetail);
            }
        }
        // 5 保存到mysql中
        this.saveBatch(governanceAssessDetails);
    }

}
