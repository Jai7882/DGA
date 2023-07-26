package com.jia.dga.governance.assessor;

import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.beans.factory.parsing.Problem;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * ClassName: Assessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 16:47
 * @Version 1.0
 */
public abstract class  Assessor {
    public GovernanceAssessDetail metricAssess(AssessParam assessParam){

        GovernanceAssessDetail governanceAssessDetail = new GovernanceAssessDetail();
        //  1 获取元数据
        //  2 获得指标信息
        //  3 填写考评信息 填写默认值的逻辑每个指标都一样
        governanceAssessDetail.setAssessDate(assessParam.getAssessDate());
        governanceAssessDetail.setTableName(assessParam.getTableMetaInfo().getTableName());
        governanceAssessDetail.setSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        governanceAssessDetail.setMetricId(String.valueOf(assessParam.getGovernanceMetric().getId()));
        governanceAssessDetail.setMetricName(assessParam.getGovernanceMetric().getMetricName());
        governanceAssessDetail.setAssessComment(assessParam.getGovernanceMetric().getGovernanceType());
        governanceAssessDetail.setTecOwner(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName());
        governanceAssessDetail.setAssessScore(BigDecimal.TEN);

        //  4 判断得分的逻辑 问题信息不一样 备注也不一样 url 每个指标各不相同
        try{
            checkProblem(governanceAssessDetail,assessParam);
        }catch (Exception e){
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);
            e.printStackTrace(writer);
            governanceAssessDetail.setAssessExceptionMsg(stringWriter.toString().substring(0,Math.min(stringWriter.toString().length(), 2000)));
            governanceAssessDetail.setIsAssessException("1");
        }


        //  5 处理考评时的异常 每个指标都一样

        //  6 考评时间
        governanceAssessDetail.setCreateTime(new Date());
        return governanceAssessDetail;
    };

    public abstract void checkProblem(GovernanceAssessDetail governanceAssessDetail,AssessParam assessParam) throws Exception;

}
