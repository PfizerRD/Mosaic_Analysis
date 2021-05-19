import os
import pandas as pd
from tqdm import tqdm
from pfawsaccess import *
import io
import boto3
from pathlib import Path
import h5py
import botocore
import argparse
from GAITRite.PKMAS_QC import *
from GAITRite.GAITRite_QC import *


def agg_gaitrite_data(f):
    '''
    Get file info together for GAITRite files
    :param f: filename of gaitrite file
    :return: object with the subject ID, task number, lap number, and filename
    '''
    filename = str(f).split('/')[-1]
    subj, date, t, tasknum, ending = filename.split('_', 4)
    lap_num = ending.partition('GAITRite')[0]

    obj = {'subject': subj,
           'task': tasknum,
           'lap': lap_num,
           'filename': f}

    return obj

def get_GAITRite_files_AWS(method, results_file):
    '''
    This function collects the filenames for APDM processing as a dataframe
    :param task: the type of task you are looking for:
        ['Walk', 'Sway', 'Sit_to_Stand', 'TUG', 'Analysis']
    :return: a dataframe with the filename info for S3
    '''

    from tempfile import TemporaryDirectory
    from importlib import import_module

    path = 's3://ecddmtimosaicamrasp70622'

    prefix = 'raw'
    extension = '.csv'
    exclude = '.h5', 'apdm', 'CORT', 'MOSAIC_STUDYVIDEOANNOTATIONS', \
              'video_annotations', 'stepwatch', 'geneactiv', 'earlysense', 'psg', 'TUG', 'test'
    include = 'GAITRite'

    s3 = boto3.Session(profile_name='saml').resource('s3')
    bucket = s3.Bucket(path[5:])
    files = [Path(i.key) for i in bucket.objects.filter(Prefix=prefix)]
    files = filter_files(files, include, exclude, extension)

    pkmas = [x for x in files if 'PKMAS' in str(x)] #To get PKMAS
    gaitrite = [x for x in files if not 'PKMAS' in str(x)]  # To get Gaitrite
    gaitrite_df = [agg_gaitrite_data(x) for x in gaitrite]
    gaitrite_df = pd.DataFrame(gaitrite_df)

    results = []
    if method == 'pkmas':
        with TemporaryDirectory() as tempdir:
            tdpath = Path(tempdir)

            #for file in files:
            for file in pkmas:
                file_size = round(bucket.Object(str(file)).content_length / 1000000, 3) # convert to megabyte
                dlp = tdpath / file.name  # create DownLoad Path
                s3.meta.client.download_file(path[5:], str(file), str(dlp))
                r_df = check_file(dlp)
                dlp.unlink()  # deletes the file
                results.append(r_df)

        results = pd.concat(results)
        results.to_csv(results_file, index=False)

    else:
        with TemporaryDirectory() as tempdir:
            tdpath = Path(tempdir)
            subs = sorted(list(set(list(gaitrite_df.subject))))
            task = sorted(list(set(list(gaitrite_df.task))))

            for s in subs:
                for t in task:
                    gt_sub_df = gaitrite_df[(gaitrite_df.subject == s) & (gaitrite_df.task == t)]
                    if gt_sub_df.empty:
                        print('s')
                        continue
                    else:
                        pass
                    ##TODO: Groupby subject and trail to run the rest
                    #file_size = round(bucket.Object(str(file)).content_length / 1000000, 3)  # convert to megabyte
                    #dlp = [str(tdpath) + '/' + str(x) for x in gt_sub_df.filename] # create DownLoad Path
                    # create DownLoad Path
                    gaitrite_subj_data = []
                    for file in list(gt_sub_df.filename):
                        #dlp = tdpath / file
                        #s3.meta.client.download_file(path[5:], str(file), str(dlp))
                        filename = str(file).split('/')[-1]
                        try:
                            subjID, date, Vtask, trailnum, lap_num, tot_laps, end = filename.split('_')
                        except ValueError:
                            #print('0')
                            subjID, date, Vtask, trailnum, lap_num, Ad, lapNum2, tot_laps, end = filename.split('_')
                            lap_num = '%s_%s_%s' % (lap_num, Ad, lapNum2)
                        df = read_file_aws(file)
                        df['subject'] = subjID
                        df['trail'] = trailnum
                        df['lap'] = lap_num
                        gaitrite_subj_data.append(df)

                    gaitrite_subj_data = pd.concat(gaitrite_subj_data)
                    r_df = gr_check_file(gaitrite_subj_data, subjID, trailnum)
                    #dlp.unlink()  # deletes the file
                    results.append(r_df)

        results = pd.concat(results)
        results.to_csv(results_file, index=False)

def read_file_aws(filename):
    '''

    :param filename: AWS filepath to file
    :return: Dataframe of data in file
    '''
    import boto3
    import pandas as pd
    import io

    session = boto3.Session(profile_name='saml')
    s3 = session.resource('s3')

    obj = s3.Object('ecddmtimosaicamrasp70622', str(filename))

    df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))

    return df

def filter_files(files, include, exclude, extension):
    '''

    :param files: The collection of files on AWS
    :param include: Inclusion criteria
    :param exclude: Exclusion criteria
    :param extension: Desired file type extension
    :return: List of filtered filenames
    '''
    if '*' in exclude:
        mask = [False] * len(files)

        for i, file in enumerate(files):
            mask[i] |= (file.suffix.lower() == extension.lower())
            mask[i] |= any([s.lower() in str(file).lower() for s in include]) or (include == [])
    elif '*' in include:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

    else:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= any([s.lower() in str(file).lower() for s in include]) or (include == [])
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

        for i in range(len(mask) - 1, -1, -1):
            if not mask[i]:
                del files[i]

    return files

if __name__ == '__main__':
    ## need PKMAS save path, Need GaitRite save path, bucket name,
    # ARGUMENT PARSING
    get_GAITRite_files_AWS('gaitrite', '_.csv')
    parser = argparse.ArgumentParser()
    parser.add_argument('sensor', choices=['gaitrite', 'pkmas'])
    parser.add_argument('--results_path')

    args = parser.parse_args()
    get_GAITRite_files_AWS(args.sensor, args.results_path)