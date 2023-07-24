package com.jia.dga.meta.service;

import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.bean.TableMetaInfoExtra;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {

    public void initTableMateExtra(String assessDate, List<TableMetaInfo> tableMetaInfoList);

}
