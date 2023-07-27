package com.jia.dga.governance.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jia.dga.common.constant.MetaConst;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;

/**
 * ClassName: TrafficAssessor
 * Package: com.jia.dga.governance.assessor.quality
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 13:05
 * @Version 1.0
 */
@Component("TRAFFIC_OVER_AVG")
public class TrafficAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        // 指标前n天思路
        // 1 定义指标
        // 2 考评器类
        // 3 计算出当天的产出目录名称 前n天每天的产出目录名
        String assessDate = assessParam.getAssessDate();
        JSONObject object = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
        Integer days = object.getInteger("days");
        BigDecimal upperLimit = object.getBigDecimal("upper_limit");
        BigDecimal lowerLimit = object.getBigDecimal("lower_limit");
        if (MetaConst.LIFECYCLE_TYPE_DAY.equals(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType())) {
            //计算前n天的分区目录名
            JSONArray jsonArray = JSON.parseArray(assessParam.getTableMetaInfo().getPartitionColNameJson());
            String partitionName = jsonArray.getJSONObject(0).getString("name");
            String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
            Date partitionDate = DateUtils.addDays(DateUtils.parseDate(assessDate, "yyyy-MM-dd"), -1);
            String formattedDate = DateFormatUtils.format(partitionDate, "yyyy-MM-dd");
            String currentPartitionDirName = tableFsPath + "/" + partitionName + "=" + formattedDate;

            // 4 通过目录名访问hdfs遍历获得该目录的大小
            // 获得当前考评日期前一天的文件大小
            Long currentSize = getBatchDirSize(currentPartitionDirName, assessParam.getTableMetaInfo().getTableFsOwner());
            long totalCount = 0L;
            long totalDaysSize = 0L;
            // 获得days天内的文件总大小
            for (int i = 1; i <= days; i++) {
                partitionDate = DateUtils.addDays(DateUtils.parseDate(assessDate, "yyyy-MM-dd"), -i);
                formattedDate = DateFormatUtils.format(partitionDate, "yyyy-MM-dd");
                currentPartitionDirName = tableFsPath + "/" + partitionName + "=" + formattedDate;
                Long daysSize = getBatchDirSize(currentPartitionDirName, assessParam.getTableMetaInfo().getTableFsOwner());
                if (daysSize > 0) {
                    totalDaysSize += daysSize;
                    totalCount++;
                }
            }
            // 5 当天数据产出/前n天的平均产出 得到比例
            // 实际计算的数据天数大于0才有计算的价值 因为可能存在长期数据无产出
            if (totalCount > 0) {
                long avgSizeTotal = totalDaysSize / totalCount;
                // 6 将上个步骤得到的比例和考评指标的参数上限和下限参考值对比 在范围内 不做操作 范围外计0分并给差评
                if (currentSize > avgSizeTotal * (upperLimit.longValue() + 100L) / 100L) {
                    governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                    governanceAssessDetail.setAssessProblem("产出高于现有" + totalCount + "日平均数据量的" + upperLimit.longValue() + "%");
                } else {
                    if (currentSize < avgSizeTotal * lowerLimit.longValue() / 100L){
                        governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                        governanceAssessDetail.setAssessProblem("产出低于现有" + totalCount + "日平均数据量的" + lowerLimit.longValue() + "%");
                    }
                }
            }
        }

    }

    private Long getBatchDirSize(String currentPartitionDirName, String tableFsOwner) throws Exception {
        FileSystem fs = FileSystem.get(new URI(currentPartitionDirName), new Configuration(), tableFsOwner);
        Long size = 0L;
        if (fs.exists(new Path(currentPartitionDirName))) {
            FileStatus[] fileStatuses = fs.listStatus(new Path(currentPartitionDirName));
            size = getTotalSizeRec(fileStatuses, fs, 0L);
        }
        return size;
    }

    private Long getTotalSizeRec(FileStatus[] fileStatuses, FileSystem fs, long l) throws Exception {
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                FileStatus[] subFileStatuses = fs.listStatus(fileStatus.getPath());
                getTotalSizeRec(subFileStatuses, fs, l);
            } else {
                l += fileStatus.getLen();
            }
        }
        return l;
    }

}
