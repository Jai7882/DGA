package com.jia.dga.meta.service;

import com.jia.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.hadoop.hive.metastore.api.MetaException;

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

}
