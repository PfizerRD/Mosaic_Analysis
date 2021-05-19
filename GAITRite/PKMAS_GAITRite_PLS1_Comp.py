from GAITRite.PKMAS_QC import *
from GAITRite.GAITRite_QC import *
from GAITRite.get_files import *


def get_files_AWS(method, results_file):
    '''
    This function collects the filenames for APDM processing as a dataframe
    :param task: the type of task you are looking for:
        ['Walk', 'Sway', 'Sit_to_Stand', 'TUG', 'Analysis']
    :return: a dataframe with the filename info for S3
    '''

    from tempfile import TemporaryDirectory
    from importlib import import_module

    path = 's3://ecddmtipfire1amrasp66535'

    if method == 'gaitrite':
        prefix = 'raw'
        include = ['GAITRite', 'NoInsole']
    else:
        prefix = 'processed'
        include = ['pkmas', 'NoInsoles']
    extension = '.csv'
    exclude = '.h5', 'apdm', 'CORT', 'MOSAIC_STUDYVIDEOANNOTATIONS', \
              'video_annotations', 'stepwatch', 'geneactiv', 'earlysense', 'psg', 'TUG', 'test', \
              'Device_Apple_Watch_APPLWTCH', 'GaitGrid'

    s3 = boto3.Session(profile_name='saml').resource('s3')
    bucket = s3.Bucket(path[5:])
    files = [Path(i.key) for i in bucket.objects.filter(Prefix=prefix)]
    files = filter_files(files, include, exclude, extension)

    pkmas = [x for x in files if 'PKMAS' in str(x)]  # To get PKMAS
    gaitrite = [x for x in files if not 'PKMAS' in str(x)]  # To get Gaitrite
    gaitrite_df = [agg_gaitrite_data(x) for x in gaitrite]
    gaitrite_df = pd.DataFrame(gaitrite_df)

    results = []
    if method == 'pkmas':
        # for file in files:
        for file in pkmas:

            obj = s3.Object(bucket.name, str(file))
            df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
            row = pkmas_analysis(df)
            results.append(row)
    else:
        for file in gaitrite:
            WIP_path = '/Volumes/npru-bluesky/OtherProjects/PfiReLabStudy1/PfIRe_Lab_Study1_Raw_Data/'
            file_path = str(file).split('/', 2)[2]
            df = pd.read_csv(WIP_path + file_path)
            row = gaitrite_analysis(df)
            results.append(row)

    results = pd.concat(results)
    results.to_csv(results_file, index=False)
    #return results

def gaitrite_analysis(df):
    ##Aggregate the results by median of metrics per lap (remove na/0) and avg across laps
    idx_subID = ['Patient Name' in str(x) for x in df.iloc[:, 0]]
    subID = df.iloc[:, 0][idx_subID]
    subject = subID.values[0].split(',')[1].strip()
    new_cols = df.iloc[0, :]
    df.columns = [x.strip() for x in new_cols]
    sub_df = df.drop(index = 0)
    dat = sub_df.iloc[:, 6:44]
    dat = dat.dropna(axis = 0)
    dat = dat.drop(columns=['Yarray', 'RealFoot', 'PassNo'])
    steps = dat.shape[0]
    dat_df = dat.replace('0', np.nan)
    dat_df = dat_df.dropna(axis = 0)

    results = pd.DataFrame([np.nanmedian(dat_df.astype('float'), axis=0)])
    results.columns = dat_df.columns
    results['subject'] = subject
    results['steps'] = steps

    return results

def pkmas_analysis(df):
    ##Aggregate the results by median of metrics per lap (remove na/0) and avg across the laps
    subject = df.iloc[0, 1].split(',')[1].strip()
    header = df.iloc[11,:].values
    if 'Velocity (cm./sec.)' not in header:
        header = df.iloc[10,:].values
    else:
        pass

    sub_df = df.iloc[27:,:]
    sub_df.columns = header
    steps = sub_df.shape[0]
    dat = sub_df.iloc[:, 5:]
    results = pd.DataFrame([np.nanmedian(dat.astype('float'), axis=0)])
    results.columns = dat.columns
    results['subject'] = subject
    results['steps'] = steps

    return results

if __name__ == '__main__':
    gaitrite = get_files_AWS('gaitrite', '/Users/psaltd/Desktop/PLS1_analytics/X9001178_gaitrite_summary.csv')
    pkmas = get_files_AWS('pkmas', '/Users/psaltd/Desktop/PLS1_analytics/X9001178_pkmas_summary.csv')


