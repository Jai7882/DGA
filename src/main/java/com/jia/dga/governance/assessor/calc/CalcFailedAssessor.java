package com.jia.dga.governance.assessor.calc;

import com.jia.dga.common.constant.MetaConst;
import com.jia.dga.ds.bean.TDsTaskInstance;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: CalcFailedAssessor
 * Package: com.jia.dga.governance.assessor.calc
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 17:34
 * @Version 1.0
 */
@Component("HAS_CALC_FAILED")
public class CalcFailedAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        // 从参数中提取任务参数列表
        List<TDsTaskInstance> taskInstanceList = assessParam.getTDsTaskInstanceList();
        if (taskInstanceList==null){
            return;
        }
        boolean flag = false;
        // 循环检查是否有任务失败的状态 如果有失败 0分差评

        for (TDsTaskInstance instance : taskInstanceList) {
            if (instance.getState().toString().equals(MetaConst.TASK_STATE_FAILED)) {
                flag = true;
                break;
            }
        }

        if (flag){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在失败任务");
        }
    }
}
