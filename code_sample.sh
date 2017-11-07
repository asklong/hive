#!/bin/bash
source /data0/wangxin/tools/shell/util.sh
runday=$(date +%Y-%m-%d -d "1 days ago")
[[ ! -z $1 ]] && runday=$(date -d "$1" +%Y-%m-%d)
order_day=$(date +"%Y-%m-%d" -d "$runday 15 days ago")
order_output=wangjie/data/app_param_gmv/day=$runday/type=app
hadoop fs -test -e $order_output
if [ $? -ne 0 ];then
    hadoop fs -mkdir -p $order_output
else
    hadoop fs -rm -r $order_output
fi

function app_order()
{
hive -e "
    use gdm;
    set mapred.reduce.tasks = 10;
    add file /data0/wangxin/tools/script/event_udf.py;
    INSERT OVERWRITE DIRECTORY '${order_output}'
    SELECT  w.dt,
            d.ord_status_cd_1,
            w.app_version,
            w.os_plant,
            w.event_id,
            w.event_param,
            d.parent_sale_ord_id,
            d.after_prefr_amount,
            d.item_sku_id,
            w.sid,
            w.clk_sku_id,
            w.flow,
            w.source,
            w.browser_uniq_id,
            w.user_log_acct,
            d.sale_ord_time
    FROM 
        (
            SELECT TRANSFORM(dt,app_version,os_plant,first_event_id,first_event_param,item_id,sale_ord_id,browser_uniq_id,user_log_acct) 
            using 'python event_udf.py order' as 
            (dt,app_version,os_plant,event_id,event_param,sid,clk_sku_id,order_sku_id,sale_ord_id,flow,source,browser_uniq_id,user_log_acct) 
            FROM gdm.gdm_m14_wireless_order_log
            where dt >= '$order_day'
            and substring(app_version,1,3) >= '4.4'
            and first_event_id in ('Home_ProductList', 'Home_Productid', 'Home_Shopid', 'Home_SimilarView')
 
            UNION ALL
            
            SELECT TRANSFORM(dt,app_version,os_plant,second_event_id,second_event_param,item_id,sale_ord_id,browser_uniq_id,user_log_acct) 
            using 'python event_udf.py order' as 
            (dt,app_version,os_plant,event_id,event_param,sid,clk_sku_id,order_sku_id,sale_ord_id,flow,source,browser_uniq_id,user_log_acct) 
            FROM gdm.gdm_m14_wireless_order_log
            where dt >= '$order_day'
            and substring(app_version,1,3) >= '4.4'
            and second_event_id in ('MyJD_GuessYouLike', 'Shopcart_GuessYouLike', 'MJingDouHome_ProductPic', 'Shopcart_Compare_Productid')

            UNION ALL
            
            SELECT TRANSFORM(dt,app_version,os_plant,fourth_event_id,fourth_event_param,item_id,sale_ord_id,browser_uniq_id,user_log_acct) 
            using 'python event_udf.py order' as 
            (dt,app_version,os_plant,event_id,event_param,sid,clk_sku_id,order_sku_id,sale_ord_id,flow,source,browser_uniq_id,user_log_acct) 
            FROM gdm.gdm_m14_wireless_order_log
            where dt >= '$order_day'
            and substring(app_version,1,3) >= '4.4'
            and fourth_event_id = 'Productdetail_Like'

            UNION ALL
            
            SELECT TRANSFORM(dt,app_version,os_plant,third_event_id,third_event_param,item_id,sale_ord_id,browser_uniq_id,user_log_acct) 
            using 'python event_udf.py order' as 
            (dt,app_version,os_plant,event_id,event_param,sid,clk_sku_id,order_sku_id,sale_ord_id,flow,source,browser_uniq_id,user_log_acct) 
            FROM gdm.gdm_m14_wireless_order_log
            where dt >= '$order_day'
            and substring(app_version,1,3) >= '4.4'
            and third_event_id in ('OrderDetail_ProductRecommend', 'MyFollow_RecommendProduct', 'OrderFinish_Recommend_Product')

            UNION ALL

            SELECT TRANSFORM(dt,app_version,os_plant,fifth_event_id,fifth_event_param,item_id,sale_ord_id,browser_uniq_id,user_log_acct) 
            using 'python event_udf.py order' as 
            (dt,app_version,os_plant,event_id,event_param,sid,clk_sku_id,order_sku_id,sale_ord_id,flow,source,browser_uniq_id,user_log_acct) 
            FROM gdm.gdm_m14_wireless_order_log
            where dt >= '$order_day'
            and substring(app_version,1,3) >= '4.4'
            and fifth_event_id = 'Shopcart_Compare_AddtoCart'
        ) w

          LEFT JOIN 
        (
            SELECT parent_sale_ord_id, item_sku_id, ord_status_cd_1, after_prefr_amount, unix_timestamp(sale_ord_tm) as sale_ord_time
            FROM gdm.gdm_m04_ord_det_sum 
            where dt >= '$order_day'
            and sale_ord_valid_flag = 1
            and parent_sale_ord_id is not null
            and after_prefr_amount  <= 50000
        ) d
            ON w.sale_ord_id = d.parent_sale_ord_id AND w.order_sku_id = d.item_sku_id;
"
}

#check upsteam data
test_file hdfs://ns1/user/dd_edw/gdm.db/gdm_m14_wireless_order_log/dt=$runday *.lzo hdfs
#wait for gdm.gdm_m04_ord_det_sum history data
time_now=`date +%H%M`
if [ $time_now -lt 0930 ];then
    sleep 90m
fi
#run
app_order
if [ $? -ne 0 ];then
    echo "gen order data failed."
    exit -1
fi
#check output data
hadoop fs -cat $order_output/* > data/${runday}.order
if [ $? -ne 0 ];then
    echo "hadoop cat failed. The disk is nospace? Or hafs disable?"
    exit -1
fi
order_num=$(grep $runday data/${runday}.order | wc -l)
if [ $order_num -le 350000 ];then
    echo "order data missing, please check upstream hdfs://ns1/user/dd_edw/gdm.db/gdm_m14_wireless_order_log."
    exit -1
fi
