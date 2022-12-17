# coding: utf-8
import read_table

select_sql = '''
select
	t1.custom_id,
	t1.trade_date,
	t1.trade_type,
	t1.fund_code,
	t1.trade_money,
	t1.fund_shares,
	t1.fund_nav,
	t1.dt,
	t2.u_gender,
	t2.u_EDU,
	t2.u_RSK_ENDR_CPY,
	t2.u_NATN,
	t2.u_OCCU,
	t2.u_IS_VAIID_INVST,
	t3.i_fund_type,
	t3.i_management,
	t3.i_custodian,
	t3.i_invest_type
from algorithm.zq_fund_trade t1
left JOIN (
	select cust_id, gender as u_gender, EDU as u_EDU, RSK_ENDR_CPY as u_RSK_ENDR_CPY, NATN as u_NATN, OCCU as u_OCCU, IS_VAIID_INVST as u_IS_VAIID_INVST
	from algorithm.user_info
	where dt = '2022-09-14'
) t2
on t1.custom_id = t2.cust_id
left JOIN (
	select ts_code, fund_type as i_fund_type, management as i_management, custodian as i_custodian, invest_type as i_invest_type
	from algorithm.zq_fund_basic
	where dt = '2022-11-02'
) t3
on t1.fund_code = t3.ts_code
'''

# select_sql = "select * from algorithm.zq_fund_trade limit 5"
table_name = read_table.read_table_to_hive(select_sql=select_sql)
print(table_name)
