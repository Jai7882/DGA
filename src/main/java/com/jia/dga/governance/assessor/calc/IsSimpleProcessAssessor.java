package com.jia.dga.governance.assessor.calc;

import com.google.common.collect.Sets;
import com.jia.dga.common.util.SqlParser;
import com.jia.dga.governance.assessor.Assessor;
import com.jia.dga.governance.bean.AssessParam;
import com.jia.dga.governance.bean.GovernanceAssessDetail;
import lombok.Getter;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * ClassName: IsSimpleProessAssessor
 * Package: com.jia.dga.governance.assessor.calc
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/27 11:00
 * @Version 1.0
 */
@Component("HAS_SIMPLE_FACTOR")
public class IsSimpleProcessAssessor extends Assessor {

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) throws Exception {
        // 1 取sql
        if (assessParam.getTDsTaskDefinition()== null || assessParam.getTDsTaskDefinition().getSql()==null){
            return;
        }
        String sql  = assessParam.getTDsTaskDefinition().getSql();
        // 2 利用工具分析sql 前提是自定义处理器
        Dispatcher myDispatcher = new MyDispatcher();
        SqlParser.parse(sql,myDispatcher);


    }

    // 自定义节点处理器
    private static class MyDispatcher implements Dispatcher{

        @Getter
        Set<String> complicatedOperateSet = new HashSet<>();
        Set<Integer> complicatedOperateTypeSet = Sets.newHashSet(
                HiveParser.TOK_JOIN,
                HiveParser.TOK_LEFTOUTERJOIN,
                HiveParser.TOK_RIGHTOUTERJOIN,
                HiveParser.TOK_FULLOUTERJOIN,
                HiveParser.TOK_SELECTDI,
                HiveParser.TOK_FUNCTIONDI,
                HiveParser.TOK_GROUPBY,
                HiveParser.TOK_UNIONALL,
                HiveParser.TOK_FUNCTION,
                HiveParser.TOK_FUNCTIONSTAR);

        @Getter
        Set<String> whereFiledSet = new HashSet<>();

        @Getter
        Set<String> fromTableNameSet = new HashSet<>();
        //每到达一个节点要处理的事情
        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {
            // 1 采集sql中的复杂操作 join/group by/函数/union
            ASTNode astNode = (ASTNode) node;
            if (complicatedOperateTypeSet.contains(astNode.getType())){
                complicatedOperateSet.add(astNode.getText());
            }


            // 2 采集where条件涉及的分区字段

            // 3 来源表的表名 用来确定分区字段是否为分区字段

            // 4 结合上述步骤分析是否简单加工
            return null;
        }
    }



}
