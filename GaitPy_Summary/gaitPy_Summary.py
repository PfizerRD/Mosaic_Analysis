import pandas as pd
import numpy as np
import os
from GaitPy_Summary.get_Mosic_files_AWS import *
import io
from tqdm import tqdm

def summarize_data(df):
    medians = pd.DataFrame([np.nanmedian(df, axis=0)])
    perc95 = pd.DataFrame([np.percentile(df, 95, axis=0)])

    medians.columns = [str(x)+'_median' for x in df.columns]
    perc95.columns = [str(x)+'_95perc' for x in df.columns]

    ret_df = pd.concat([medians, perc95], axis = 1)

    return ret_df

if __name__ == '__main__':
    filename_df = get_mosaic_files_AWS()
    results = []
    errored_files = []
    for index, row in tqdm(filename_df.iterrows()):
        try:
            df = pd.read_csv(io.BytesIO(row.S3_obj.get()['Body'].read()))
        except:
            errored_files.append(row)
            continue
        sub_df = df.iloc[:, 7:]
        sub_df_final = sub_df.iloc[:, ~sub_df.columns.str.contains('asymmetry')] ##Filter out asymmetry
        ret_df = summarize_data(sub_df_final)
        ret_df['subject'] = row.subject
        ret_df['task'] = row.task
        ret_df['device'] = row.device
        ret_df['file_type'] = row.file_type
        results.append(ret_df)

    results_df = pd.concat(results)
    results_df.to_csv('/Users/psaltd/Desktop/Mosaic/data/Mosaic_GaitPy_Summary_metrics.csv', index=False)

    errored_files_df = pd.DataFrame(errored_files)
    errored_files_df.to_csv('/Users/psaltd/Desktop/Mosaic/data/Mosaic_GaitPy_Summary_errors.csv', index=False)