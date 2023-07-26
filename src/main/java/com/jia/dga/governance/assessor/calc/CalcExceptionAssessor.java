package com.jia.dga.governance.assessor.calc;

import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

/**
 * ClassName: CalcExceptionAssessor
 * Package: com.jia.dga.governance.assessor.calc
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/26 17:34
 * @Version 1.0
 */
@Component("HAS_CALC_EXC")
public class CalcExceptionAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {

    }
}
