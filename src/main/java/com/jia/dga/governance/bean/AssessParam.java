package com.jia.dga.governance.bean;

import com.jia.dga.ds.bean.TDsTaskDefinition;
import com.jia.dga.ds.bean.TDsTaskInstance;
import com.jia.dga.meta.bean.TableMetaInfo;
import lombok.Data;

import java.util.List;

/**
 * ClassName: AssessParam
 * Package: com.jia.dga.governance.bean
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 19:28
 * @Version 1.0
 */
@Data
public class AssessParam {
    //考评日期
    String assessDate;

    //元数据
    TableMetaInfo tableMetaInfo;

    //指标
    GovernanceMetric governanceMetric;

    List<TableMetaInfo> tableMetaInfoList;
    //ds...
    //当前考评表的任务定义
    TDsTaskDefinition tDsTaskDefinition;

    //当前考评表的任务实例
    List<TDsTaskInstance> tDsTaskInstanceList;

}
