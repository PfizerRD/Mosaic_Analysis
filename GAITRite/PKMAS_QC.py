#####################################################
##     Explore PKMAS Gait Data for MOSAIC HC       ##
##     Di, Junrui  - Dimitrios psaltos             ##
#####################################################

import pandas as pd
import numpy as np

def Memo_check(df):
    memos = sorted(list(set(list(df.Memo))))
    wrong_memo_name = [len(str.strip(x)) != 13 for x in memos]

    memos_results = pd.DataFrame([memos, wrong_memo_name]).T
    memos_results.columns = ['Memo', 'memo_check']

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
        if 'Right' in df.Label[i]:
            if 'Left' in df.Label[i - 1]:
                pass
            else:
                issues.append(i)
        elif 'Left' in df.Label[i]:
            if 'Right' in df.Label[i - 1]:
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

def check_file(csv):
    csv = str(csv)
    file = csv.split('/')[-1]
    subjID, date, task, trailnum, end = file.split('_')
    pkmas_df = pd.read_csv(csv)

    vars = ['Unnamed: 1', "Memo", "Step Length (cm.)", "Stride Width (cm.)", "Integ. Pressure (p x sec.)",
            "Foot Area (cm. x cm.)", "Stance Time (sec.)", "Total D. Support (sec.)"]

    sub_df = pkmas_df.filter(vars)

    sub_df.columns = ["Label", "Memo", "step_length", "stride_width",
                          "integ_pressure", "foot_area",
                          "stance_time", "total_d_support"]

    asym_df = Asym_check(sub_df)
    step_df = Step_check(sub_df)
    memo_df = Memo_check(sub_df)

    check_df = memo_df.merge(step_df)
    check_df = check_df.merge(asym_df)

    check_df['subject'] = subjID
    check_df['trail'] = trailnum

    return check_df

if __name__ == '__main__':
    csv = '/Users/psaltd/Desktop/achondroplasia/devices/GaitRite/' \
          'GAITRite_Sample_data_Mosaic/MOSAIC9398_20200221_2MWT_T1_PKMAS.csv'

    #TODO: Aquire files from S3
    check_file(csv)
