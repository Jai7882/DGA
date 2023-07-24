package com.jia.dga.meta.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
@Mapper
@DS("dga")
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {

}
