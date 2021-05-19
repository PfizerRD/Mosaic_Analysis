from tqdm import tqdm
import os
import pandas as pd

import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/STEPP/src/GeneActiv/')
from GeneActiv_helpers import *

import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/Mosaic_Analysis/GaitPy_Summary/')
from get_Mosic_files_AWS import get_mosaic_files_AWS

def get_Mosaic_GA_files(macro):
    path = '/Users/psaltd/Desktop/Mosaic/data/Processed/'
    files = os.listdir(path)
    files.sort()
    res_df = []
    for f in files:
        [subID, wearpos, sec, end] = f.split('_')
        [subID, v] = subID.split('-')
        if v == '3':
            vis = 'home'
        else:
            vis = 'error'

        subjectID = pd.read_csv(path + f, nrows = 20).iloc[10,1]
        subjectID = subjectID.strip() ## This is from the epoch file to confirm the subID

        savename = f.split('.')[0]
        savename = ('%s_%s.csv') % (savename, macro)

        row = {'Study': 'Mosaic Healthy @Home',
               'Wear Location': wearpos,
               'Subject': subID,
               'visit': vis,
               'check_subID': subjectID,
               'filepath': path + f,
               'savepath': path + savename}
        res_df.append(row)

    res_df = pd.DataFrame(res_df)

    return res_df

if __name__ == '__main__':
    macro = 'Everyday_living_macro'
    df = get_Mosaic_GA_files(macro)

    for index, row in df.iterrows():
        if os.path.exists(row.savepath):
            continue
        else:
            if macro == 'Sleep_macro':
                geneactiv_sleep_macro_csv(row.filepath, row.savepath)
            if macro == 'Everyday_living_macro':
                geneactiv_everyday_living_macro_csv(row.filepath, row.savepath)
        time.sleep(3)

    print('Done!')