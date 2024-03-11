# Luigi pipeline for processing genetic data from NCBI

## What is does:
1. Downloads a raw dataset from GEO (Gene Expression Omnibus) functional genomics data repository given the dataset name.
2. Extract files from tar package file
3. For each file inside creates a separates folder and unarchive the corresponding file there.
4. Process each file using pandas, saving five separate tsv-files
5. Remove the unzipped file in each folder (keeping the download and unpacked files in a root folder)

### The tasks in the script:
1. DownloadDataset
2. ExtractTarFile
3. UnzipGZFiles
4. SplitTSVFiles
5. Cleanup

## Script code:
[pipeline.py](pipeline.py)


## Usage:
python ./pipeline.py Cleanup --dataset-name &lt;dataset-name&gt; --local-scheduler

### Example:
python ./pipeline.py Cleanup --dataset-name GSE68849 --local-scheduler

## Results of script

[In folder](GSE68849) 

[As an archive](GSE68849.zip)