# Luigi pipeline for processing genetic data from NCBI

## Script:
[pipeline.py](pipeline.py)

### The tasks in the script:
1. DownloadDataset
2. ExtractTarFile
3. UnzipGZFiles
4. SplitTSVFiles
5. Cleanup

## How to Use:
python ./pipeline.py Cleanup --dataset-name <dataset-name> --local-scheduler

### Example:
python ./pipeline.py Cleanup --dataset-name GSE68849 --local-scheduler

## Results of script

[In folder](Data_Engineering_Assignment_1/GSE68849) 

[As an archive](Data_Engineering_Assignment_1/GSE68849.zip)