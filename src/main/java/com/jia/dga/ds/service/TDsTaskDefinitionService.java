package com.jia.dga.ds.service;

import com.jia.dga.ds.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author jia
 * @since 2023-07-26
 */
public interface TDsTaskDefinitionService extends IService<TDsTaskDefinition> {

    List<TDsTaskDefinition> getTaskDefinitionListForAssess(List<Long> taskCodeList);
}
