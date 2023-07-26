package com.jia.dga;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.governance.service.GovernanceAssessDetailService;
import com.jia.dga.meta.service.TableMetaInfoService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DgaApplicationTests {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Test
    public void testInitMeta() throws Exception {
        tableMetaInfoService.initTableMetaInfo("2023-05-02","gmall");
    }

    @Test
    public void testMetricAssess() throws Exception {
        governanceAssessDetailService.allMetricAssess("2023-05-02");
    }
}
