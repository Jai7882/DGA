package com.jia.dga.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.PropertyPreFilter;
import com.alibaba.fastjson.support.spring.PropertyPreFilters;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.jia.dga.common.util.SqlUtil;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.bean.TableMetaInfoExtra;
import com.jia.dga.meta.bean.TableMetaInfoForQuery;
import com.jia.dga.meta.bean.TableMetaInfoVO;
import com.jia.dga.meta.mapper.TableMetaInfoMapper;
import com.jia.dga.meta.service.TableMetaInfoExtraService;
import com.jia.dga.meta.service.TableMetaInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.arrow.flatbuf.Int;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表 服务实现类
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
@Service
@DS("dga")
public class TableMetaInfoServiceImpl extends ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService {


    HiveMetaStoreClient hiveMetaStoreClient;

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;

    @Value("${dga.meta.url}")
    String url;


    //select *  from table_meta_info t1 join table_meta_info_extra t2 on t1.schema_name = t2.schema_name
    // and t1.table_name = t2.table_name  where t1.schema_name like '%%' and t1.table_name like '%%'
    // and t1.dw_level = '' and assess_date = (select max(assess_date) from table_meta_info t3 where t1.schema_name = t3.schema_name
    // and t1.table_name = t3.table_name)
    //根据查询条件和分页进行查询
    @Override
    public List<TableMetaInfoVO> getTableListForPage(TableMetaInfoForQuery metaInfoForQuery) {
        StringBuilder sql = new StringBuilder(200);
        sql.append("select t1.id ,t1.table_name,t1.schema_name,table_comment,table_size,table_total_size," +
                "tec_owner_user_name,busi_owner_user_name, table_last_access_time,table_last_modify_time" +
                " from table_meta_info t1 join table_meta_info_extra t2 on t1.schema_name = t2.schema_name\n" +
                "and t1.table_name = t2.table_name ");
        sql.append("where  assess_date = (select max(assess_date) from table_meta_info t3 where t1.schema_name = t3.schema_name\n" +
                "and t1.table_name = t3.table_name)");
        if (metaInfoForQuery.getTableName()!=null && metaInfoForQuery.getTableName().trim().length()!=0){
            sql.append(" and t1.table_name like '%").append(SqlUtil.filterUnsafeSql(metaInfoForQuery.getTableName())).append("%'");
        }
        if (metaInfoForQuery.getSchemaName()!=null && metaInfoForQuery.getSchemaName().trim().length()!=0){
            sql.append(" and t1.schema_name like '%").append(SqlUtil.filterUnsafeSql(metaInfoForQuery.getSchemaName())).append("%'");
        }
        if (metaInfoForQuery.getDwLevel()!=null && metaInfoForQuery.getDwLevel().trim().length()!=0){
            sql.append(" and t2.dw_level ='").append(SqlUtil.filterUnsafeSql(metaInfoForQuery.getDwLevel())).append("'");
        }
        // 获取行数 并拼接到sql中
        Integer row = (metaInfoForQuery.getPageNo()-1) *  metaInfoForQuery.getPageSize();
        sql.append(" limit " + row + "," + metaInfoForQuery.getPageSize());

        return this.baseMapper.selectTableMetaInfoPage(sql.toString());
    }

    @Override
    public Integer getTableTotalForPage(TableMetaInfoForQuery metaInfoForQuery) {
        StringBuilder sql = new StringBuilder(200);
        sql.append("select count(*)" +
                " from table_meta_info t1 join table_meta_info_extra t2 on t1.schema_name = t2.schema_name\n" +
                "and t1.table_name = t2.table_name ");
        sql.append(" where  assess_date = (select max(assess_date) from table_meta_info t3 where t1.schema_name = t3.schema_name\n" +
                "and t1.table_name = t3.table_name)");
        if (metaInfoForQuery.getTableName()!=null && metaInfoForQuery.getTableName().trim().length()!=0){
            sql.append(" and t1.table_name like '%").append(metaInfoForQuery.getTableName()).append("%'");
        }
        if (metaInfoForQuery.getSchemaName()!=null && metaInfoForQuery.getSchemaName().trim().length()!=0){
            sql.append(" and t1.schema_name like '%").append(metaInfoForQuery.getSchemaName()).append("%'");
        }
        if (metaInfoForQuery.getDwLevel()!=null && metaInfoForQuery.getDwLevel().trim().length()!=0){
            sql.append(" and t2.dw_level ='").append(metaInfoForQuery.getDwLevel()).append("'");
        }

        return this.baseMapper.selectTableMetaInfoTotalPage(sql.toString());
    }

    @Override
    public TableMetaInfo getTableMetaInfoById(Long tableId) {
        TableMetaInfo tableMetaInfo = getById(tableId);
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfoExtraService.getOne(new QueryWrapper<TableMetaInfoExtra>()
                .eq("table_name", tableMetaInfo.getTableName())
                .eq("schema_name", tableMetaInfo.getSchemaName()));
        tableMetaInfo.setTableMetaInfoExtra(tableMetaInfoExtra);
        return tableMetaInfo;
    }

    @Override
    public List<TableMetaInfo> getTableMetaWithExtraList() {
        return baseMapper.selectTableMetaWithExtraList();
    }

    @Override
    public void initTableMetaInfo(String assessDate, String schemaName) throws Exception {
        List<String> tables = hiveMetaStoreClient.getAllTables(schemaName);

        List<TableMetaInfo> tableMetaInfoList = new ArrayList<>();

        for (String tableName : tables) {
            // 从hive中提取数据
            TableMetaInfo metaInfo = getTableInfoFromHive(schemaName, tableName);
            //从hdfs中补充元数据信息
            addHdfsInfo(metaInfo);
            metaInfo.setAssessDate(assessDate);
            metaInfo.setCreateTime(new Date());
            //将补全信息的metainfo添加到集合中
            tableMetaInfoList.add(metaInfo);
        }

        //初始化
        this.saveBatch(tableMetaInfoList);
        tableMetaInfoExtraService.initTableMateExtra(assessDate,tableMetaInfoList);


        //初始化辅助信息表
        System.out.println(tableMetaInfoList);
    }



    private void addHdfsInfo(TableMetaInfo metaInfo) throws Exception {
        //从hdfs中获取数据
        FileSystem fs = FileSystem.get(new URI(metaInfo.getTableFsPath()), new Configuration(), metaInfo.getTableFsOwner());
        FileStatus[] fileStatuses = fs.listStatus(new Path(metaInfo.getTableFsPath()));
        
        //递归方法调用
        addFileInfoRec(fileStatuses,fs,metaInfo);

        metaInfo.setFsCapcitySize(fs.getStatus().getCapacity());
        metaInfo.setFsUsedSize(fs.getStatus().getUsed());
        metaInfo.setFsRemainSize(fs.getStatus().getRemaining());



    }

    private void addFileInfoRec(FileStatus[] fileStatuses, FileSystem fs, TableMetaInfo metaInfo) throws Exception {
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()){
                FileStatus[] subFileStatuses = fs.listStatus(fileStatus.getPath());
                addFileInfoRec(subFileStatuses,fs,metaInfo);
            }else {
                //文件累计大小
                Long tableNewSize = metaInfo.getTableSize()+fileStatus.getLen();
                metaInfo .setTableSize(tableNewSize);

                //结合副本数的文件大小
                long totalSize = metaInfo.getTableTotalSize() + fileStatus.getLen() * fileStatus.getReplication();
                metaInfo.setTableTotalSize(totalSize);

                // 作比较 : 比较当前文件的最后修改时间和文件的最大的最后修改时间
                if(metaInfo.getTableLastModifyTime() == null){
                    metaInfo.setTableLastModifyTime(new Date(fileStatus.getModificationTime()));
                }else {
                    // 保留两者中较大值作为最新修改日期
                    if (metaInfo.getTableLastModifyTime().compareTo(new Date(fileStatus.getModificationTime()))<0){
                        metaInfo.setTableLastModifyTime(new Date(fileStatus.getModificationTime()));
                    }
                }

                // 作比较 : 比较当前文件的最后访问时间和文件的最大的最后访问时间
                if(metaInfo.getTableLastAccessTime() == null){
                    metaInfo.setTableLastAccessTime(new Date(fileStatus.getAccessTime()));
                }else {
                    // 保留两者中较大值作为最新访问日期
                    if (metaInfo.getTableLastAccessTime().compareTo(new Date(fileStatus.getAccessTime()))<0){
                        metaInfo.setTableLastAccessTime(new Date(fileStatus.getAccessTime()));
                    }
                }

            }
        }

    }

    private TableMetaInfo getTableInfoFromHive(String schemaName,String tableName) throws Exception {
        Table table = hiveMetaStoreClient.getTable(schemaName, tableName);
        System.out.println(tableName);
        TableMetaInfo tableMetaInfo = new TableMetaInfo();
        tableMetaInfo.setTableName(table.getTableName());
        tableMetaInfo.setSchemaName(schemaName);
        //过滤器 用于调整JSON转换过程中保留指定的字段
        PropertyPreFilters.MySimplePropertyPreFilter filter = new PropertyPreFilters().addFilter("name", "type", "comment");
        tableMetaInfo.setColNameJson(JSON.toJSONString(table.getSd().getCols(), filter));
        tableMetaInfo.setPartitionColNameJson(JSON.toJSONString(table.getPartitionKeys(), filter));
        tableMetaInfo.setTableFsOwner(table.getOwner());
        tableMetaInfo.setTableParametersJson(JSON.toJSONString(table.getParameters()));
        tableMetaInfo.setTableComment(table.getParameters().get("comment"));
        tableMetaInfo.setTableFsPath(table.getSd().getLocation());
        tableMetaInfo.setTableInputFormat(table.getSd().getInputFormat());
        tableMetaInfo.setTableOutputFormat(table.getSd().getOutputFormat());
        tableMetaInfo.setTableRowFormatSerde(table.getSd().getSerdeInfo().getSerializationLib());
        tableMetaInfo.setTableCreateTime(DateFormatUtils.format(new Date(table.getCreateTime() * 1000L), "yyyy-MM-dd HH:mm:ss"));
        tableMetaInfo.setTableType(table.getTableType());
        tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(table.getSd().getBucketCols()));
        tableMetaInfo.setTableBucketNum((long) table.getSd().getNumBuckets());
        return tableMetaInfo;
    }

    @PostConstruct
    public void initMetaClient() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, url);
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }

    }
}
