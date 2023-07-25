package com.jia.dga.governance.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import com.jia.dga.governance.bean.GovernanceMetric;
import com.jia.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.jia.dga.governance.service.GovernanceAssessDetailService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jia.dga.governance.service.GovernanceMetricService;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-25
 */
@Service
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {


    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceMetricService governanceMetricService;

    public void allMetricAssess(String assessDate){
        // 1 查询出要考评的表 含最新元数据和辅助信息 List<TableMetaInfo>
        // 为了避免循环查询数据库 使用join方式查询数据
        List<TableMetaInfo> tableGovernanceMetricList = tableMetaInfoService.getTableMetaWithExtraList();

        // 2 查询出要考评的指标列表 List<GovernanceMetric>
        List<GovernanceMetric> list = governanceMetricService.list(new QueryWrapper<GovernanceMetric>().eq("is_disabled", "0"));

        // 3 DS信息

        // 4 每张表的每个指标逐一进行考评 通过两个列表的双层循环 List<GovernanceAssessDetail>
        for (TableMetaInfo tableMetaInfo : tableGovernanceMetricList) {
            for (GovernanceMetric governanceMetric : list) {
                if (governanceMetric.getMetricCode().equals("HAS_TEC_OWNER")){

                }else if (governanceMetric.getMetricCode().equals("HAS_TRA_OWNER")){

                }
            }
        }


        // 5 保存到mysql中
    }

}
