#####################################################
##     Explore GAITRite Gait Data for MOSAIC HC    ##
##     Di, Junrui  - Dimitrios psaltos             ##
#####################################################

import pandas as pd
import numpy as np
import os
import glob

def Memo_check(df):
    memos = sorted(list(set(list(df.Memo))))
    wrong_memo_name = [len(str.strip(x)) != 13 for x in memos]

    memos_results = pd.DataFrame([memos, wrong_memo_name]).T
    memos_results.columns = ['Memo', 'check']

    return  memos_results

def Step_check(df):
    steps = df.groupby('Memo').count().Label
    m50_thresh = round(np.median(steps) * 0.5)
    m60_thresh = round(np.median(steps) * 0.6)
    m70_thresh = round(np.median(steps) * 0.7)

    steps_res = {'50perc_steps': steps < m50_thresh, '60perc_steps': steps < m60_thresh,
                 '70perc_steps': steps < m70_thresh}
    res_df = pd.DataFrame(steps_res)
    res_df['Memo'] = res_df.index

    return res_df.reset_index(drop = True)

def _check_step_labels(df):
    df = df.reset_index(drop=True)
    # split = [s.split(' ')[0] for s in steps]
    issues = []
    for i in range(1, len(df)):
        # if split[i] == 'Right':
        if 1 == df.Label[i]:
            if 0 == df.Label[i - 1]:
                pass
            else:
                issues.append(i)
        elif 0 == df.Label[i]:
            if 1 == df.Label[i - 1]:
                pass
            else:
                issues.append(i)
        else:
            issues.append(i)

    return issues

def Asym_check(df):
    step_errors = []
    lap_names = sorted(list(set(list(df.Memo))))
    for lap in lap_names:
        steps = df[df.Memo == lap]
        #print(lap)
        step_label_irreg = _check_step_labels(steps)
        #print(step_label_irreg)
        step_errors.append(step_label_irreg)

    obj = {'Memo': lap_names, 'asym_error': step_errors}

    return pd.DataFrame(obj)

def gr_check_file(gr_df, subjID, trailnum):
    '''
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    gr_df = pd.concat(li, axis=0, ignore_index=True)
    gr_df = []
    for file in all_files:
        filename = file.split('/')[-1]
        subjID, date, task, trailnum, lap_num, tot_laps, end = filename.split('_')
        df = pd.read_csv(file)
        df['subject'] = subjID
        df['trail'] = trailnum
        df['lap'] = lap_num
        gr_df.append(df)

    gr_df = pd.concat(gr_df)
    '''
    vars = [' Last Name', ' Step Length', ' Comments', " Left/Right Foot"]

    sub_df = gr_df.filter(vars)

    sub_df.columns = ["ID", "step_length", "Memo", "Label"]

    asym_df = Asym_check(sub_df)
    step_df = Step_check(sub_df)
    memo_df = Memo_check(sub_df)

    check_df = memo_df.merge(step_df)
    check_df = check_df.merge(asym_df)
    check_df['subject'] = subjID
    check_df['trail'] = trailnum

    return check_df

if __name__ == '__main__':
    path = '/Users/psaltd/Desktop/achondroplasia/devices/GaitRite/' \
          'GAITRite_Sample_data_Mosaic/'
    all_files = glob.glob(path + "/*GAITRite.csv")
    all_files = sorted(all_files)
    #csv = 'MOSAIC9398_20200221_2MWT_T1_01_07_GAITRite.csv'

    #TODO: Aquire files from S3 and load all the GAITRite files together
    gr_check_file(all_files)
