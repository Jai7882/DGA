package com.jia.dga;

import com.jia.dga.governance.service.GovernanceAssessDetailService;
import com.jia.dga.meta.service.TableMetaInfoService;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class DgaApplicationTests {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Test
    public void testInitMeta() throws Exception {
        tableMetaInfoService.initTableMetaInfo("2022-07-22","gmall");
    }

    @Test
    public void testMetricAssess() throws Exception {
        governanceAssessDetailService.allMetricAssess("2023-05-02");
    }
}
