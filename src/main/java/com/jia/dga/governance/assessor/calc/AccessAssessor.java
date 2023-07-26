package com.jia.dga.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: AccessAssessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 21:05
 * @Version 1.0
 */
@Component("RECENT_NO_ACC")
public class AccessAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {
        // 1 获取days参数
        JSONObject object = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
        Long days = object.getLong("days");
        // 2 获取当前表的最后访问日期
        Date tableLastAccessDate = DateUtils.truncate(assessParam.getTableMetaInfo().getTableLastAccessTime(), Calendar.DATE);
        // 3 获取考评日期
        Date date = DateUtils.parseDate(assessParam.getAssessDate(), "yyyy-MM-dd");
        // 4 求考评日期 到最后访问日期的 日期天数差值只看日期 不考虑时间
        long diffMs = date.getTime() - tableLastAccessDate.getTime();
        long diffDays = TimeUnit.DAYS.convert(diffMs, TimeUnit.MILLISECONDS);
        // 5 用天数差值和days参数作比较 超过0分 同时给出差评原因
        if (diffDays > days) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("最后访问日期超过" + days + "天,最后访问日期为" + DateFormatUtils.format(tableLastAccessDate,"yyyy-MM-dd"));
        }
    }
}
