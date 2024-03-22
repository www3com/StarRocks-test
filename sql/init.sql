

create table 发票_开具_基本信息 (
                                    id bigint NOT NULL,
    `发票代码` varchar(100) NULL,
    `数电票号码` varchar(100) NULL,
    `销方识别号` varchar(100) NULL,
    `销方名称` varchar(100) NULL,
    `购方识别号` varchar(100) NULL,
    `购买方名称` varchar(100) NULL,
    `开票日期` varchar(100) NULL,
    `金额` decimal(10, 2) NULL,
    `税额` decimal(10, 2) NULL,
    `价税合计` decimal(10, 2) NULL,
    `发票来源` varchar(100) NULL,
    `发票票种` varchar(100) NULL,
    `发票状态` varchar(100) NULL,
    `是否正数发票` varchar(100) NULL,
    `发票风险等级` varchar(100) NULL,
    `开票人` varchar(100) NULL,
    `备注` varchar(100) NULL
) PRIMARY KEY (id)
 DISTRIBUTED BY HASH(id)