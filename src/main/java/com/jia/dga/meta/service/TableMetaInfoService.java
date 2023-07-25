package com.jia.dga.meta.service;

import com.jia.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.extension.service.IService;
import com.jia.dga.meta.bean.TableMetaInfoForQuery;
import com.jia.dga.meta.bean.TableMetaInfoVO;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    public void initTableMetaInfo(String assessDate,String schemaName) throws MetaException, Exception;

    List<TableMetaInfoVO> getTableListForPage(TableMetaInfoForQuery metaInfoForQuery);

    Integer getTableTotalForPage(TableMetaInfoForQuery metaInfoForQuery);

    TableMetaInfo getTableMetaInfoById(Long tableId);

    List<TableMetaInfo> getTableMetaWithExtraList();
}
