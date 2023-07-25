package com.jia.dga.meta.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.jia.dga.meta.bean.TableMetaInfoVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

import java.util.List;

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

    @Select("${sql}")
    public List<TableMetaInfoVO> selectTableMetaInfoPage(@Param("sql") String sql);

    @Select("${sql}")
    public Integer selectTableMetaInfoTotalPage(@Param("sql") String sql);

    @Select("select t1.*, t2.*, t2.id t2_id ,t2.create_time t2_create_time\n" +
            "from table_meta_info t1\n" +
            "         join table_meta_info_extra t2 on t1.schema_name = t2.schema_name\n" +
            "    and t1.table_name = t2.table_name\n" +
            "where assess_date = (select max(assess_date)\n" +
            "                     from table_meta_info t3\n" +
            "                     where t1.schema_name = t3.schema_name\n" +
            "                       and t1.table_name = t3.table_name)")
    @ResultMap("tableMetaResultMap")
    public List<TableMetaInfo> selectTableMetaWithExtraList();
}
