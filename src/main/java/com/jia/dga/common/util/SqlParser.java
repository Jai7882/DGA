package com.jia.dga.common.util;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Collections;
import java.util.Stack;

/**
 * ClassName: SqlParser
 * Package: com.jia.dga.common.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/27 10:27
 * @Version 1.0
 */
public class SqlParser {

    public static void parse(String sql, Dispatcher dispatcher) throws Exception {
        // 1 创建分析引擎 用于把sql转换为语法树
        ParseDriver parseDriver = new ParseDriver();
        // 2 转语法树
        // 得到树的顶点
        ASTNode node = parseDriver.parse(sql);
        //向下取第一个子节点
        ASTNode child = (ASTNode) node.getChild(0);
        // 3 准备进行遍历 创建一个遍历器 安装定制的节点处理器
        DefaultGraphWalker graphWalker = new DefaultGraphWalker(dispatcher);
        // 4 执行遍历
        graphWalker.startWalking(Collections.singletonList(child),null);
    }



}
