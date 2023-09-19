import requests
from datetime import datetime
import pandas as pd
import boto3 
from io import StringIO

def get_facebook_data():
    url = 'https://graph.facebook.com/v17.0/me'
    params = {
        'fields': 'adaccounts{account_id,name,adsets{id,account_id,campaign_id,name,status,start_time,end_time,insights{date_start,date_stop,impressions,reach,spend,cpm,cpp,cpc,frequency,cost_per_inline_link_click,account_currency,clicks,ctr,engagement_rate_ranking,cost_per_conversion,purchase_roas}}}',
        'access_token': 'EABeF9y6z5iEBOz97AypnRvFGlsXYp8JvNdCOb682SZB3fg8cpAt76Ui7HZBzgZCvh4SJFvgeVUSCPMB9rxnEnXZCZAAj3q4lhivM7almbjCMGBUPcO1dIG8Uj2l8kfvW21nXCc0SNrXZCaclJOVIJX3XhxaWN1YV93WN4DAgVSXV34AjI3aKDXXZA9T3jUbZBVogfwZDZD'
    }

    response = requests.get(url, params=params)
    if response.status_code == 200: 
        data = response.json()
        return data
    else:
        print('Error:', response.status_code, response.text)
        return None
    
def transform_facebook_data(data):
    adaccounts = data['adaccounts']['data']
    meta_data_list = []
    for data in adaccounts:
        if data['account_id'] != '328273816321621':
            for adsets in data['adsets']['data']:
                if  adsets['status'] == 'ACTIVE':
                    meta_data_dict = {}
                    meta_data_dict['adsets_id'] = adsets['id']
                    meta_data_dict['account_id'] = data['account_id']
                    meta_data_dict['camp_id'] = adsets['campaign_id']
                    meta_data_dict['adset_name'] = adsets['name']
                    meta_data_dict['adset_status'] = adsets['status']
                    adset_start_datetime = adsets['start_time']
                    parsed_adset_start_datetime = datetime.strptime(adset_start_datetime, "%Y-%m-%dT%H:%M:%S%z")
                    meta_data_dict['adset_start_date'] = parsed_adset_start_datetime.date()
                    meta_data_dict['adset_start_time'] = parsed_adset_start_datetime.time()
                    adset_stop_datetime = adsets['end_time']
                    parsed_adset_stop_datetime = datetime.strptime(adset_stop_datetime, "%Y-%m-%dT%H:%M:%S%z")
                    meta_data_dict['adset_stop_date'] = parsed_adset_stop_datetime.date()
                    meta_data_dict['adset_stop_time'] = parsed_adset_stop_datetime.time()
                    if adsets.get('insights') is not None:
                        for insights in adsets['insights']['data']:
                            meta_data_dict['adset_impression'] = insights.get('impressions',0)
                            meta_data_dict['adset_reach'] = insights.get('reach',0)
                            meta_data_dict['adset_spend'] = insights.get('spend',0)
                            meta_data_dict['insight_start_date'] = insights.get('date_start')
                            meta_data_dict['insight_stop_date'] = insights.get('date_stop')
                            meta_data_dict['adset_cpm'] = insights.get('cpm',0)
                            meta_data_dict['adset_cpp'] = insights.get('cpp',0)
                            meta_data_dict['adset_cpc'] = insights.get('cpc',0)
                            meta_data_dict['adset_cpv'] = insights.get('cpv',0)
                            meta_data_dict['adset_cpi'] = insights.get('cpi',0)
                            meta_data_dict['adset_cpa'] = insights.get('cpa',0)
                            meta_data_dict['adset_frequency'] = insights.get('frequency',0)
                            meta_data_dict['adset_cost_per_inline_link_click'] = insights.get('cost_per_inline_link_click',0)
                            meta_data_dict['adset_account_currency'] = insights.get('account_currency',None)
                            meta_data_dict['adset_clicks'] = insights.get('clicks',0)
                            meta_data_dict['adset_ctr'] = insights.get('ctr',0)
                            meta_data_dict['adset_engagement_rate_ranking'] = insights.get('engagement_rate_ranking',None)
                            meta_data_dict['adset_cost_per_conversion'] = insights.get('cost_per_conversion',0)
                            meta_data_dict['adset_purchase_roas'] = insights.get('purchase_roas',0)
                            meta_data_dict['adset_cost_per_thruplay'] = insights.get('cost_per_thruplay',0)
                    else:
                            meta_data_dict['adset_impression'] = None
                            meta_data_dict['adset_reach'] = None
                            meta_data_dict['adset_spend'] = None
                            meta_data_dict['insight_start_date'] = None
                            meta_data_dict['insight_stop_date'] = None
                            meta_data_dict['adset_cpm'] = None
                            meta_data_dict['adset_cpp'] = None
                            meta_data_dict['adset_cpc'] = None
                            meta_data_dict['adset_cpv'] = None
                            meta_data_dict['adset_cpi'] = None
                            meta_data_dict['adset_cpa'] = None                            
                            meta_data_dict['adset_frequency'] = None
                            meta_data_dict['adset_cost_per_inline_link_click'] = None
                            meta_data_dict['adset_account_currency'] = None
                            meta_data_dict['adset_clicks'] = None
                            meta_data_dict['adset_ctr'] = None
                            meta_data_dict['adset_engagement_rate_ranking'] = None
                            meta_data_dict['adset_cost_per_conversion'] = None
                            meta_data_dict['adset_purchase_roas'] = None
                            meta_data_dict['adset_cost_per_thruplay'] = None

                    meta_data_list.append(meta_data_dict)  
    df = pd.DataFrame(meta_data_list)
    return df

def upload_to_s3(df, aws_access_key_id, aws_secret_access_key, region_name, bucket):
    s3 = boto3.client("s3",
                  region_name=region_name,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key='AdSets.csv')

def main():
    facebook_data = get_facebook_data()
    if facebook_data:
        transformed_data = transform_facebook_data(facebook_data)
        region_name = 'us-east-1'
        bucket = 'fb-meta.bucket-1'
        aws_access_key_id = 'AKIAQXDOKNAIBAVWWNVI'
        aws_secret_access_key = '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'
        upload_to_s3(transformed_data, aws_access_key_id, aws_secret_access_key, region_name, bucket)

if __name__ == "__main__":
    main()


