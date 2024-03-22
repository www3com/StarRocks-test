package com.upbos.starrocks;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class StarrocksTestApplicationTests {

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Test
    void contextLoads() {
    }

    @Test
    public void select() {
        Thread[] threads = new Thread[1000];
        for (int i = 0; i < threads.length; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                list(Thread.currentThread().getName(), finalI);
            }, "thread" + i);
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("所有线程执行完毕");
    }

    private void list(String threadName, int limit) {
        long start = System.currentTimeMillis();
        var sql = "select * from 发票_开具_基本信息 limit " + limit;
        var list = jdbcTemplate.queryForList(sql);
        long end = System.currentTimeMillis();
        System.out.println(threadName + ": 耗时：" + (end - start) + ", 大小: " + list.size());
    }

    @Test
    public void insert() {
        Thread[] threads = new Thread[6];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                insertItems(Thread.currentThread().getName());
            }, "thread" + i);
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("所有线程执行完毕");
    }

    public void insertItems(String threadName) {
        var dates = generateDates();
        var buyers = generateBuyers();

        String sql = """
                insert into 发票_开具_基本信息 (id, 发票代码, 数电票号码, 销方识别号, 销方名称, 购方识别号, 购买方名称, 开票日期, 金额,
                                                税额, 价税合计, 发票来源, 发票票种, 发票状态, 是否正数发票, 发票风险等级, 开票人, 备注)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """;
        Snowflake snowflake = IdUtil.getSnowflake(1, 1);
        int batchSize = 2000;
        int total = 1200 * 10000;
        long total_start = System.currentTimeMillis();
        for (int i = 0; i < total / batchSize; i++) {
            List<Object[]> list = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (int j = 0; j < batchSize; j++) {
                long id = snowflake.nextId();
                int je_b = RandomUtil.randomInt(1, 10000);
                int je_a = RandomUtil.randomInt(1, 10);
                String je = je_b + "." + je_a;
                int se_b = RandomUtil.randomInt(1, 100);
                int se_a = RandomUtil.randomInt(1, 10);
                String se = se_b + "." + se_a;
                BigDecimal jeb = new BigDecimal(je);
                BigDecimal seb = new BigDecimal(se);
                Object[] objects = {id, "", String.valueOf(snowflake.nextId()), RandomUtil.randomString(18),
                        "成都胜殊川云企业管理咨询服务中心", RandomUtil.randomString(18), RandomUtil.randomEle(buyers,5),
                        RandomUtil.randomEle(dates, 12*28), jeb, seb, jeb.add(seb),
                        "电子发票服务平台", "电子发票（普通发票）", "是", "是", "发票风险等级", "罗灿", "备注"};
                list.add(objects);
            }
            jdbcTemplate.batchUpdate(sql, list);
            long end = System.currentTimeMillis();
            System.out.println(threadName + ", batch insert time:" + (end - start) + "ms, 已导入：" + (i + 1) * batchSize + "条");
        }
        long total_end = System.currentTimeMillis();
        System.out.println(threadName + ", total time:" + (total_end - total_start) + "ms");
    }

    private List<String> generateDates() {
        List<String> dates = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            var m = i < 10 ? "0" + i : "" + i;
            for (int j = 1; j <=28; j++) {
                var d = j < 10 ? "0" + j : "" + j;
                dates.add("2023-" + m + "-" + d);
            }
        }
        return dates;
    }
    private List<String> generateBuyers() {
        List<String> buyers = new ArrayList<>();
        buyers.add("北京中航国际科技有限公司");
        buyers.add("四川成都中海科技有限公司");
        buyers.add("河北人工资能有限公司");
        buyers.add("浏阳市视频有限公司");
        buyers.add("神州数码科技有限公司");
        return buyers;
    }
}
