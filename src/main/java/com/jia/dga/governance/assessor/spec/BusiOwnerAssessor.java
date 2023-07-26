package com.jia.dga.governance.assessor.spec;

import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * ClassName: BusiOwnerAssessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 19:06
 * @Version 1.0
 */
@Component("HAS_BUSI_OWNER")
public class BusiOwnerAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String busiOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getBusiOwnerUserName();
        if (busiOwnerUserName==null || busiOwnerUserName.trim().length()==0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("业务负责人为空");
            String url = assessParam.getGovernanceMetric().getGovernanceUrl().replace("{tableId}", assessParam.getTableMetaInfo().getId().toString());
            governanceAssessDetail.setGovernanceUrl(url);
        }
    }
}
