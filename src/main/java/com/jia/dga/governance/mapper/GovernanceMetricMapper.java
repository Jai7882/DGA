package com.jia.dga.governance.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.governance.bean.GovernanceMetric;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 考评指标参数表 Mapper 接口
 * </p>
 *
 * @author jia
 * @since 2023-07-25
 */
@Mapper
@DS("dga")
public interface GovernanceMetricMapper extends BaseMapper<GovernanceMetric> {

}
