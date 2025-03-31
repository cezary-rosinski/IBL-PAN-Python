import glob

#%%
path = r'C:\Users\Cezary\Downloads\sofair texts/'
folders = [f for f in glob.glob(path + '**', recursive=True)]
