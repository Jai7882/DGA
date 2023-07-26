package com.jia.dga.governance.assessor.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import com.jia.dga.meta.bean.TableMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ClassName: HasSimilarTableAssessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 8:33
 * @Version 1.0
 */
@Component("HAS_SIMILAR_TABLE")
public class HasSimilarTableAssessor extends Assessor {


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws ParseException {
        // 1 取得percent参数
        JSONObject object = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
        BigDecimal percent = object.getBigDecimal("percent");
        // 2 获取当前考评表元数据
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();

        if (tableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals("ODS")){
            return;
        }
        // 3 获得所有表的元数据列表
        List<TableMetaInfo> tableMetaInfoList = assessParam.getTableMetaInfoList();
        Set<String> similarTableNameList = new HashSet<>();
        // 4 三层循环
        // 4.1 迭代 循环所有表 依次和考评表比较
        for (TableMetaInfo metaInfoFromList : tableMetaInfoList) {
            // 排除当前考评表本身
            if (metaInfoFromList.getTableName().equals(tableMetaInfo.getTableName()) && metaInfoFromList.getSchemaName().equals(tableMetaInfo.getSchemaName())) {
                continue;
            }
            // 排除非同层次表
            if (!metaInfoFromList.getTableMetaInfoExtra().getDwLevel().equals(tableMetaInfo.getTableMetaInfoExtra().getDwLevel())) {
                continue;
            }
            // 过滤ODS层

            // 取得字段集合
            // 考评表字段集合
            List<JSONObject> currentObjects = JSON.parseArray(tableMetaInfo.getColNameJson(), JSONObject.class);
            // 比对表字段集合
            List<JSONObject> assessObjects = JSON.parseArray(metaInfoFromList.getColNameJson(), JSONObject.class);
            Integer count = 0;

            // 4.2 循环考评表的所有字段
            for (JSONObject currentColJsonObject : currentObjects) {
                // 4.3 每个字段和其他表的字段比较
                for (JSONObject assessObject : assessObjects) {
                    // 只要有名称相同的字段 字段相同数++
                    // 相似比例 = 字段相同个数/总字段个数
                    String currentName = currentColJsonObject.getString("name");
                    String assessName = assessObject.getString("name");

                    if (currentName.equals(assessName)) {
                        count++;
                    }
                }
                BigDecimal percentCurrent = BigDecimal.valueOf(count).divide(BigDecimal.valueOf(currentObjects.size()), 2, RoundingMode.HALF_UP).movePointRight(2);
                BigDecimal percentOther = BigDecimal.valueOf(count).divide(BigDecimal.valueOf(assessObjects.size()), 2, RoundingMode.HALF_UP).movePointRight(2);
                if (percentCurrent.compareTo(percent)>0 || percentOther.compareTo(percent)>0){
                    // 每发现一个相似表记录下来
                    similarTableNameList.add(metaInfoFromList.getTableName());
                }

            }


        }
        // 5 相似比例和percent参数作比较 如果超过percent 则给0分并差评
        if (similarTableNameList.size()>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在相似表："+ StringUtils.join(similarTableNameList,","));
        }
    }
}
