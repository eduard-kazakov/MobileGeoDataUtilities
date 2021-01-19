import pandas as pd
import numpy as np
from datetime import datetime
import os

def timestamp_to_correct_format(timestamp):
    ct = datetime.strptime(timestamp,'%d.%m.%Y %H:%M')
    return ct.strftime('%Y.%m.%d %H:%M')

# Основная фильтрация - по колонкам, времени, zid, home_zid
def basic_filtrator(csv_gz_files, output_csv_file=None, columns=None, time_stamps=None, zid_list=None, zid_max=None, home_zid_list=None, home_zid_max=None, chunk_size=10000, chunk_message_step=5000, start_chunk=0, stop_iteration=None, mode='unite'):
    start_time = datetime.now()
    if columns is None:
            for chunk in pd.read_csv(csv_gz_files[0], chunksize=1, delimiter=';'):
                columns = chunk.columns
                break

    new_df = pd.DataFrame(columns=columns)
    l = 0
    for csv_gz_file in csv_gz_files:
        print ('=== Start basic filtration for doc %s ===\n\n' % csv_gz_file)
        
        i = 0
        for chunk in pd.read_csv(csv_gz_file, chunksize=chunk_size, delimiter=';', usecols=columns):
            if i < start_chunk:
                i+=1
                continue
                
            if (i % chunk_message_step) == 0:
                print ('%s | PROCESSING CHUNK #%s (row #%s) | Current df rows number: %s \n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), i, i*chunk_size, new_df.shape[0]))

            filtered_chunk = chunk.copy()
            if not time_stamps is None:
                filtered_chunk = filtered_chunk[filtered_chunk.ts.isin(time_stamps)]
            if not zid_max is None:
                filtered_chunk = filtered_chunk[filtered_chunk.zid <= zid_max]
            elif not zid_list is None:
                filtered_chunk = filtered_chunk[filtered_chunk.zid.isin(zid_list)]
                
            if not home_zid_max is None:
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid <= home_zid_max]
            elif not home_zid_list is None:
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid.isin(home_zid_list)]
            
            if mode == 'unite':
                new_df = new_df.append(filtered_chunk)
            if mode == 'single_files':
                if filtered_chunk.shape[0] > 0:
                    current_name = 'current_work_%s.csv' % l
                    filtered_chunk.to_csv(current_name, sep=';', index=False)
                    l+=1
            i+=1
            
            if not stop_iteration is None:
                if i > stop_iteration:
                    break
            
    if mode == 'unite':
        if output_csv_file is None:
            end_time = datetime.now()
            delta = (end_time - start_time).seconds
            print ('ENDED IN %s seconds' % delta)
            return new_df
        else:
            new_df.to_csv(output_csv_file,sep=';',index=False)
            end_time = datetime.now()
            delta = (end_time - start_time).seconds
            print ('ENDED IN %s seconds' % delta)
            
# Получение таблицы по указанным zid и timestamps, с фильтрацией по указанным home_zid
def calculate_basic_sum_table(csv_gz_files, output_csv_file=None, statistics_column='customers_cnt_total', time_stamps=None, zid_list=None, zid_max=None, home_zid_list=None, home_zid_max=None, chunk_size=10000, chunk_message_step=5000, stop_iteration=None, mode='with_external'):
    # modes:
    # without_external - ignore home_zid=-1
    # with_external - include home_zid=-1
    # external_only - use only home_zid=-1
    
    start_time = datetime.now()
    basic_columns=['ts','zid','home_zid'], 

    # 1. Create empty structure table
    template_array = []
    for zid in zid_list:
        template_array.append([zid]+[0]*len(time_stamps))
    
    final_df = pd.DataFrame(template_array, columns=['zid']+time_stamps)
    
    # 2. go through data
    aggregated_rows = 0
    for csv_gz_file in csv_gz_files:
        print ('=== Start calculate_basic_sum_table for doc %s ===\n\n' % csv_gz_file)
        
        i = 0
        for chunk in pd.read_csv(csv_gz_file, chunksize=chunk_size, delimiter=';', usecols=['ts','zid','home_zid',statistics_column]):
            if (i % chunk_message_step) == 0:
                print ('%s | PROCESSING CHUNK #%s (row #%s) | Current aggregated rows number: %s \n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), i, i*chunk_size, aggregated_rows))

            filtered_chunk = chunk.copy()
            
            if mode == 'without_external':
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid != -1]
            if mode == 'external_only':
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid == -1]
            
            filtered_chunk = filtered_chunk[filtered_chunk.ts.isin(time_stamps)]
        
            if not zid_max is None:
                filtered_chunk = filtered_chunk[filtered_chunk.zid <= zid_max]
            elif not zid_list is None:
                filtered_chunk = filtered_chunk[filtered_chunk.zid.isin(zid_list)]
            
            if not home_zid_max is None:
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid <= home_zid_max]
            elif not home_zid_list is None:
                filtered_chunk = filtered_chunk[filtered_chunk.home_zid.isin(home_zid_list)]
            
            aggregated_rows += filtered_chunk.shape[0]
            # count statistics immediately?
            filtered_grouped_chunk = filtered_chunk[['ts','zid',statistics_column]].groupby(['ts','zid']).sum()
            
            for index, row in filtered_grouped_chunk.iterrows():
                final_df.loc[((final_df['zid'] == index[1])),index[0]] += row.values[0]
            
            i+=1
            
            if not stop_iteration is None:
                if i > stop_iteration:
                    break
    
    if not (output_csv_file is None):
        final_df.to_csv(output_csv_file,sep=';',index=False)
        end_time = datetime.now()
        delta = (end_time - start_time).seconds
        print ('ENDED IN %s seconds. Aggregated %s rows' % (delta, aggregated_rows))
    else:
        end_time = datetime.now()
        delta = (end_time - start_time).seconds
        print ('ENDED IN %s seconds. Aggregated %s rows' % (delta, aggregated_rows))
        return final_df