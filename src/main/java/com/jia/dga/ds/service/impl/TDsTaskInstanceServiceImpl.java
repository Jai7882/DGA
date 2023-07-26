package com.jia.dga.ds.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.ds.bean.TDsTaskInstance;
import com.jia.dga.ds.mapper.TDsTaskInstanceMapper;
import com.jia.dga.ds.service.TDsTaskInstanceService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-26
 */
@Service
@DS("ds")
public class TDsTaskInstanceServiceImpl extends ServiceImpl<TDsTaskInstanceMapper, TDsTaskInstance> implements TDsTaskInstanceService {

}
