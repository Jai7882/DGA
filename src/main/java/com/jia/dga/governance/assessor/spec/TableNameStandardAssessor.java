package com.jia.dga.governance.assessor.spec;

import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: TableNameStandardAssessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 10:07
 * @Version 1.0
 */
@Component("TABLE_NAME_STANDARD")
public class TableNameStandardAssessor extends Assessor {

    // 1 定义各层表达式
    Pattern ods = Pattern.compile("^ods_\\w+_(inc|full)$");
    Pattern dwd = Pattern.compile("^dwd_\\w+_\\w+_(inc|full|acc)$");
    Pattern dws = Pattern.compile("^dws_\\w+_\\w+_\\w+_(1d|nd|td)$");
    Pattern dim = Pattern.compile("^dim_\\w+_(zip|full)$");
    Pattern ads = Pattern.compile("^ads_\\w+$");
    Pattern dm = Pattern.compile("^dm_\\w+$");


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {
        // 2 将待考评地表名放入对应层的表达式中
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        Matcher matcher = null;
        if (dwLevel.equals("ODS")) {
            matcher = ods.matcher(tableName);
        } else if (dwLevel.equals("DWD")) {
            matcher = dwd.matcher(tableName);
        } else if (dwLevel.equals("DIM")) {
            matcher = dim.matcher(tableName);
        } else if (dwLevel.equals("DWS")) {
            matcher = dws.matcher(tableName);
        } else if (dwLevel.equals("ADS")) {
            matcher = ads.matcher(tableName);
        } else if (dwLevel.equals("DM")) {
            matcher = dm.matcher(tableName);
        } else {
            // 未纳入分层 给5分
            governanceAssessDetail.setAssessScore(BigDecimal.valueOf(5));
            governanceAssessDetail.setAssessProblem("未纳入分层:"+tableName);
            return;
        }
        if (!matcher.matches()){
            // 3 执行比较 不合格差评 0分
            governanceAssessDetail.setAssessScore(BigDecimal.valueOf(0));
            governanceAssessDetail.setAssessProblem("表明不符合规范");
        }



    }


}
