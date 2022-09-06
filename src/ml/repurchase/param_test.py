params_dict = {"taskid":11,
               "category":["女装","男装","童装"],
               "userData":{"dt":"dt","city":"city_code","user_id":"vip_id","online_signup_time":"signup_date","sex":"sex_id","age":"age","tableName":"labelx.push_rpt_member_labels"},
               "trafficData":{"duration":"duration","exposure":"EXPOSURE","card_add":"BROWSE","user_id":"vip_id","event_code":"event_code","sku":"sku","collect":"EXPOSURE","click":"CLICK","event_time":"event_time","tableName":"labelx.push_event_vip_traffic"},
               "orderData":{"user_id":"vip_id","order_time":"order_time","sku":"sku","sale_quantity":"sale_quantity","order_id":"order_id","sale_amount":"sale_amount","tableName":"labelx.push_event_vip_order"},
               "goodsData":{"dt":"dt","cate":"category_large","sku":"sku","tableName":"labelx.push_event_vip_order"},
               "trainingScope":"过去15天",
               "forecastPeriod":"未来15天",
               "eventCode":{"event_code":{}}}

hdfs_path = 'hdfs:///user/ai/cdp/fugou/model'