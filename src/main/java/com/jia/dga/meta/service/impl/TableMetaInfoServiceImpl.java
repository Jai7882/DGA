package com.jia.dga.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.PropertyPreFilter;
import com.alibaba.fastjson.support.spring.PropertyPreFilters;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.mapper.TableMetaInfoMapper;
import com.jia.dga.meta.service.TableMetaInfoExtraService;
import com.jia.dga.meta.service.TableMetaInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
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
