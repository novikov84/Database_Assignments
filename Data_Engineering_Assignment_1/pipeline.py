import os
import luigi
import pandas as pd
import tarfile
import gzip
import shutil
from luigi import LocalTarget, Task
from luigi.parameter import Parameter
import wget
import io

class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dataset_name, f'{self.dataset_name}.tar'))

    def run(self):
        os.makedirs(self.dataset_name, exist_ok=True)
        # No need to parse the page, all files in GEO can be accessed by the same link with only name of dataset being different:
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        tar_path = self.output().path
        wget.download(url, tar_path)

class ExtractTarFile(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return DownloadDataset(self.dataset_name)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dataset_name, 'extraction_complete'))

    def run(self):
        with tarfile.open(self.input().path) as tar:
            tar.extractall(path=self.dataset_name)
        with open(self.output().path, 'w') as marker_file:
            marker_file.write('Extraction complete')

class UnzipGZFiles(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return ExtractTarFile(self.dataset_name)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dataset_name, 'unzip_complete'))

    def run(self):
        base_path = self.dataset_name
        gz_files = [f for f in os.listdir(base_path) if f.endswith('.gz')]
        for gz in gz_files:
            gz_path = os.path.join(base_path, gz)
            extract_dir = os.path.splitext(gz_path)[0]
            os.makedirs(extract_dir, exist_ok=True)
            with gzip.open(gz_path, 'rb') as f_in:
                output_file_path = os.path.join(extract_dir, os.path.basename(os.path.splitext(gz_path)[0]))
                with open(output_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        with open(self.output().path, 'w') as marker_file:
            marker_file.write('Unzip complete')

class SplitTSVFiles(Task):
    dataset_name = Parameter()

    def requires(self):
        return UnzipGZFiles(self.dataset_name)

    def output(self):
        return LocalTarget(os.path.join(self.dataset_name, 'split_complete'))

    def run(self):
        base_path = self.dataset_name
        dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

        for dir in dirs:
            dir_path = os.path.join(base_path, dir)
            for file_name in os.listdir(dir_path):
                file_path = os.path.join(dir_path, file_name)
                self.split_tsv(file_path)  

        with open(self.output().path, 'w') as marker_file:
            marker_file.write('Split complete')


    def split_tsv(self, file_path):
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        dfs[write_key].to_csv(os.path.join(os.path.dirname(file_path), f'{write_key}.tsv'), sep='\t', index=False)
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue
                if write_key:
                    fio.write(l)
            if write_key:  
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
                dfs[write_key].to_csv(os.path.join(os.path.dirname(file_path), f'{write_key}.tsv'), sep='\t', index=False)

            # Special processing for 'Probes' table if exists
            if 'Probes' in dfs:
                probes_df = dfs['Probes'].drop(columns=['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])
                probes_df.to_csv(os.path.join(os.path.dirname(file_path), 'Probes_cleaned.tsv'), sep='\t', index=False)

            

class Cleanup(Task):
    dataset_name = Parameter()

    def requires(self):
        return SplitTSVFiles(self.dataset_name)

    def output(self):
        return LocalTarget(os.path.join(self.dataset_name, 'cleanup_complete'))

    def run(self):
        base_path = self.dataset_name
        dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

        for dir in dirs:
            dir_path = os.path.join(base_path, dir)
            for file_name in os.listdir(dir_path):
                if file_name.endswith('.txt'):
                    file_path = os.path.join(dir_path, file_name)
                    os.remove(file_path)  # Remove the original text file

        with open(self.output().path, 'w') as marker_file:
            marker_file.write('Cleanup complete')



if __name__ == '__main__':
    luigi.run()