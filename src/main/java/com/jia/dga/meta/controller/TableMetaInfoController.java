package com.jia.dga.meta.controller;

import com.alibaba.fastjson.JSON;
import com.jia.dga.meta.bean.TableMetaInfo;
import com.jia.dga.meta.bean.TableMetaInfoExtra;
import com.jia.dga.meta.bean.TableMetaInfoForQuery;
import com.jia.dga.meta.bean.TableMetaInfoVO;
import com.jia.dga.meta.service.TableMetaInfoExtraService;
import com.jia.dga.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author jia
 * @since 2023-07-22
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;
    @GetMapping("/table-list")
    public String getTableList(TableMetaInfoForQuery metaInfoForQuery){
        List<TableMetaInfoVO> tableMetaList = tableMetaInfoService.getTableListForPage(metaInfoForQuery);
        Integer totalPage = tableMetaInfoService.getTableTotalForPage(metaInfoForQuery);
        HashMap<String, Object> result = new HashMap<>();
        result.put("total",totalPage);
        result.put("list",tableMetaList);
        return JSON.toJSONString(result);
    }

    @GetMapping("/table/{tableId}")
    public String getTableById(@PathVariable("tableId") Long tableId){
        return JSON.toJSONString(tableMetaInfoService.getTableMetaInfoById(tableId));
    }
@PostMapping("/init-tables/{schemaName}/{assessDate}")
    public String initTables(@PathVariable("schemaName") String schemaName,@PathVariable("assessDate") String assessDate) throws Exception {
        tableMetaInfoService.initTableMetaInfo(assessDate,schemaName);
        return null;
    }

    @PostMapping("/tableExtra")
    public String saveTableExtra(@RequestBody TableMetaInfoExtra tableMetaInfoExtra){
        tableMetaInfoExtra.setUpdateTime(new Date());
        return tableMetaInfoExtraService.saveOrUpdate(tableMetaInfoExtra)?"success":"Not success";
    }
}
