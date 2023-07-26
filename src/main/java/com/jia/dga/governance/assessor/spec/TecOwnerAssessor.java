package com.jia.dga.governance.assessor.spec;

import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * ClassName: TecOwnerAssessor
 * Package: com.jia.dga.governance.assessor
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 16:42
 * @Version 1.0
 */
@Component("HAS_TEC_OWNER")
public class TecOwnerAssessor extends Assessor {


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String ownerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName();
        if (ownerUserName==null || ownerUserName.trim().length()==0){
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("技术负责人为空");
            String url = assessParam.getGovernanceMetric().getGovernanceUrl().replace("{tableId}", assessParam.getTableMetaInfo().getId().toString());
            governanceAssessDetail.setGovernanceUrl(url);
            System.out.println(1/0);
        }
    }
}
