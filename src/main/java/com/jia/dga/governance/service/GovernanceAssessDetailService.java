package com.jia.dga.governance.service;

import com.jia.dga.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author jia
 * @since 2023-07-25
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {
    public void allMetricAssess(String assessDate);
}
