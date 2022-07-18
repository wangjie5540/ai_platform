
import json

input_params = {"taskid":52,
                      "category":["蔬菜","水果"],
                      "userData":
                          {"is_consume_online":"",
                           "is_new":"",
                           "city":"",
                           "sex":"sex",
                           "consume_level":"",
                           "yuliu_id":"",
                           "tableName":"labelx.push_user",
                           "dt":"dt",
                           "recent_view_day":"recent_view_day",
                           "province":"",
                           "user_id":"user_id",
                           "online_signup_time":"",
                           "age":"age"},
                      "trafficData":
                          {"cart_remove":"",
                           "cart_add":"",
                           "click":"CLICK",
                           "tableName":"labelx.push_traffic_behavior",
                           "duration":"duration",
                           "search":"",
                           "exposure":"EXPOSURE",
                           "card_add":"CART_ADD",
                           "user_id":"user_id",
                           "event_code":"event_code",
                           "sku":"sku",
                           "collect":"COLLECT",
                           "event_time":"event_time",
                           "browse":""},
                      "orderData":
                          {"user_id":"user_id",
                           "order_time":"order_time",
                           "sku":"sku",
                           "order_id":"order_id",
                           "sale_quantity":"",
                           "sale_amount":"",
                           "tableName":"labelx.push_order_behavior"},
                      "goodsData":
                          {"dt":"dt",
                           "cate":"cate",
                           "sku":"sku",
                           "tableName":"labelx.push_goods"},
                      "trainingScope":"过去60天",
                      "forecastPeriod":"未来60天",
                      "eventCode":
                          {"event_code":
                               {"search":"",
                                "cart_remove":"",
                                "exposure":"EXPOSURE",
                                "cart_add":"",
                                "click":"CLICK",
                                "collect":"COLLECT",
                                "browse":""}}}


from create_dataset import CreateDataset

create_data = CreateDataset()
cate_list = []
for cateid in input_params['category']:
    cate = '"'+cateid+'"'
    cate_list.append(cate)
catestr = "("+",".join(cate_list)+")"


dataset = create_data.ConstructFeatures(
                            input_params['trainingScope'],
                            input_params['forecastPeriod'],
                            catestr,
                            True,
                            input_params['orderData']['tableName'],
                            input_params['trafficData']['tableName'],
                            input_params['userData']['tableName'],
                            input_params['goodsData']['tableName'],
                            input_params['orderData'],
                            input_params['trafficData'],
                            input_params['userData'],
                            input_params['goodsData'],
                            input_params['eventCode'][input_params['trafficData']['event_code']],
                             None)
dataset