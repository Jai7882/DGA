package com.jia.dga.ds.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.ds.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author jia
 * @since 2023-07-26
 */
@Mapper
@DS("ds")
public interface TDsTaskDefinitionMapper extends BaseMapper<TDsTaskDefinition> {

}
