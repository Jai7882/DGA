package com.jia.dga.governance.assessor.security;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: BeyongPermissionAssessor
 * Package: com.jia.dga.governance.assessor.security
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 10:38
 * @Version 1.0
 */
@Component("BEYOND_PERMISSION")
public class BeyongPermissionAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        // 1 扫描考评表目录下的子目录及其文件的权限是否超过建议权限
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        String filePermission = jsonObject.getString("file_permission");
        String dirPermission = jsonObject.getString("dir_permission");
        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();
        // 对目录文件进行遍历递归
        List<String> box = checkPermission(filePermission,dirPermission,tableFsPath,tableFsOwner);
        // 根据List<String> 判断得分 如果0分 差评
        if (box.size()>0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在超越参考权限的目录:"+JSON.toJSONString(box));
        }

    }

    private List<String> checkPermission(String filePermission, String dirPermission, String tableFsPath, String tableFsOwner) throws Exception {
        // 起点 表的文件目录 工具 FileSystem、比较参数 容器 List<String>
        // 1 准备递归的相关材料
        // 工具
        FileSystem fs = FileSystem.get(new URI(tableFsPath), new Configuration(), tableFsOwner);
        // 2 容器
        // 得到权限越级的目录或文件的路径 可能存在多个 List<String>
        List<String> box = new ArrayList<>();
        // 起点
        FileStatus[] fileStatuses = fs.listStatus(new Path(tableFsPath));
        // 3 调用递归
        checkPermissionRec(fileStatuses,box,fs,filePermission,dirPermission);
        // 4 返回最终结果
        return box;
    }

    private void checkPermissionRec(FileStatus[] fileStatuses, List<String> box, FileSystem fs, String filePermission, String dirPermission) throws Exception {
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()){
                boolean isBeyondPermission = comparePermission(dirPermission,fileStatus.getPermission());
                if (isBeyondPermission){
                    box.add(fileStatus.getPath().toString());
                }
                FileStatus[] subFileStatuses = fs.listStatus(fileStatus.getPath());
                checkPermissionRec(subFileStatuses,box,fs,filePermission,dirPermission);
            }else {
                boolean isBeyondPermission = comparePermission(filePermission,fileStatus.getPermission());
                if (isBeyondPermission){
                    box.add(fileStatus.getPath().toString());
                }
            }
        }
    }

    //比较一个目录或文件的权限
    private boolean comparePermission(String filePermission, FsPermission currentPermission) {
        int groupPermission = currentPermission.getGroupAction().ordinal();
        int userPermission = currentPermission.getUserAction().ordinal();
        int otherPermission = currentPermission.getOtherAction().ordinal();
        int paramUserPermission = Integer.parseInt(filePermission.substring(0, 1));
        int paramGroupPermission = Integer.parseInt(filePermission.substring(1, 2));
        int paramOtherPermission = Integer.parseInt(filePermission.substring(2));

        return userPermission > paramUserPermission
                || groupPermission > paramGroupPermission
                || otherPermission > paramOtherPermission;
    }


}
