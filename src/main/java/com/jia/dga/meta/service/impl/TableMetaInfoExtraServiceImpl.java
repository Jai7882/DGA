package com.jia.dga.meta.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.common.MetaConst;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.bean.TableMetaInfoExtra;
import com.jia.dga.meta.mapper.TableMetaInfoExtraMapper;
import com.jia.dga.meta.service.TableMetaInfoExtraService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 * 元数据表附加信息 服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
@Service
public class  TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {


    @Override
    public void initTableMateExtra(String assessDate, List<TableMetaInfo> tableMetaInfoList) {
        // 1.检查要生成哪些辅助信息表
        // 只初始化未生成的表
        List<String> tableNameList = tableMetaInfoList.stream().map(tableMetaInfo -> tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()).collect(Collectors.toList());

        List<TableMetaInfoExtra> existExtraList = this.list(new QueryWrapper<TableMetaInfoExtra>().in("concat(schema_name,',',table_name)", tableNameList));
        Set<String> existExtraSet = existExtraList.stream().map(tableMetaInfoExtra -> tableMetaInfoExtra.getSchemaName() + "." + tableMetaInfoExtra.getTableName()).collect(Collectors.toSet());
        tableMetaInfoList.removeIf(tableMetaInfo -> existExtraSet.contains(tableMetaInfo.getSchemaName()+"."+tableMetaInfo.getTableName()));
        ArrayList<TableMetaInfoExtra> tableMetaInfoExtraList = new ArrayList<>(tableMetaInfoList.size());

        // 2.初始化数据库中未被创建的表
        for (TableMetaInfo metaInfo : tableMetaInfoList) {
            TableMetaInfoExtra tableMetaInfoExtra = new TableMetaInfoExtra();
            tableMetaInfoExtra.setTableName(metaInfo.getTableName());
            tableMetaInfoExtra.setSchemaName(metaInfo.getSchemaName());
            tableMetaInfoExtra.setLifecycleType(MetaConst.LIFECYCLE_TYPE_UNSET);
            tableMetaInfoExtra.setLifecycleDays(-1L);
            tableMetaInfoExtra.setSecurityLevel(MetaConst.SECURITY_LEVEL_UNSET);
            tableMetaInfoExtra.setDwLevel(getInitDwLevelByTableName(metaInfo.getTableName()));
            tableMetaInfoExtra.setCreateTime(new Date());
            tableMetaInfoExtraList.add(tableMetaInfoExtra);
        }
        this.saveBatch(tableMetaInfoExtraList);

    }

    private String getInitDwLevelByTableName(String tableName){
        if(tableName.startsWith("ods")){
            return "ODS";
        } else if (tableName.startsWith("dwd")) {
            return "DWD";
        }else if (tableName.startsWith("dim")) {
            return "DIM";
        }else if (tableName.startsWith("dws")) {
            return "DWS";
        }else if (tableName.startsWith("ads")) {
            return "ADS";
        }else if (tableName.startsWith("dm")) {
            return "DM";
        }else  {
            return "OTHER";
        }
    }
}
