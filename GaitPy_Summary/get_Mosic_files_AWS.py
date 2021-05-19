import boto3
from pathlib import Path
import pandas as pd

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


def get_mosaic_files_AWS():
    '''
    This function collects the filenames for APDM processing as a dataframe
    :param task: the type of task you are looking for:
        ['Walk', 'Sway', 'Sit_to_Stand', 'TUG', 'Analysis']
    :return: a dataframe with the filename info for S3
    '''

    path = 's3://ecddmtimosaicamrasp70622'

    prefix = 'processed/healthy_gait/' #GaitPy/inlab/'
    extension = '.csv'

    exclude = ''
    include = ['GaitPy', 'inlab']

    s3 = boto3.Session(profile_name='saml').resource('s3')
    bucket = s3.Bucket(path[5:])
    files = [Path(i.key) for i in bucket.objects.filter(Prefix=prefix)]
    files = filter_files(files, include, exclude, extension)

    ## create the df of info
    filename_df = []
    for file_path in files:
        file = str(file_path)
        obj = s3.Object(bucket.name, file)
        [p, h, method, s, lab, filename] = file.split('/')
        if 'inlab' in file:
            try:
                [subject, dev, task, g, f] = filename.split('_')
                file_type = 'inlab'
            except ValueError:
                [subject, dev, task, toss, g, f] = filename.split('_')
                file_type = 'inlab'
        else:
            [subject, end] = filename.split('_')
            [task, end] = end.split('.')
            dev = 'GNACTV'
            file_type = 'home'

        ##check that the subject in filename matches the folder struct
        if not s == subject:
            raise KeyError

        ret_obj =  {'subject': subject,
                    'task': task,
                    'device': dev,
                    'S3_obj': obj,
                    'file_type': file_type,
                    'filename': filename}

        filename_df.append(ret_obj)

    filename_df = pd.DataFrame(filename_df)

    return filename_df

if __name__ == '__main__':
    filename_df = get_mosaic_files_AWS()
    filename_df.head()