import bean.ColumnData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import util.VerticaDbUtil;

public class Test {

    private static final Log log = LogFactory.getLog(VerticaDbUtil.class);

    public static void main(String[] args) throws Exception {
        //创建表和索引
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + "jytest3" + "(id varchar(10),name varchar(20)," +
                "age int,class int) segmented by" + " hash(id) all nodes ksafe";
        if (VerticaDbUtil.execDDL(createTableSql)) {
            log.info("创建表和索引成功");
        }

        //单独创建表
        String createSingleTableSql = "create table is not exists " + "public.jytest2" + "(id varchar(10),name varchar(10)," +
                "name varchar(20),age int,class int)";
        log.info("创建表的结果：" + VerticaDbUtil.execDDL(createSingleTableSql));

        //单独创建索引
        String createIndexSql = "create projection if not exists public.jytest1_id as select id form jytest order by id" +
                " segmented by hash(id) all nodes";
        log.info("创建索引的结果：" + VerticaDbUtil.execDDL(createIndexSql));

        //删除索引
        String dropIndexSql = "drop projection is exists jytest2_b0,jytest2_b1 cascade";
        log.info("删除索引的结果：" + VerticaDbUtil.execDDL(createTableSql));

        //修改表备注
        String commentSql = "comment on table jytest is '测试表测试'";
        boolean flag = VerticaDbUtil.execDDL(commentSql);
        log.info("修改表备注结果:" + flag);

        //删除表
        String dropTableSql = "drop table jytest add column register_date date";
        log.info("添加表字段结果:" + VerticaDbUtil.execDDL(dropTableSql));

        //表添加字段
        String addColumnSql = "alter table jytest add column register_date date";
        log.info("添加表字段结果：" + VerticaDbUtil.execDDL(addColumnSql));

        //表字段添加注释
        String addColumnCommentSql = "comment on column jytest_b0.id is '序号'";

        //表字段删除
        String dropColumnSql = "alter table jytest3 drop column class cascade";
        log.info("删除表字段结果：" + VerticaDbUtil.execDDL(dropColumnSql));

        //数据查询
        String querySql = "select id from jytest limit 10";
        ColumnData columnData = VerticaDbUtil.executeSql(querySql);
        log.info(columnData.getColumnDataList());
    }
}
