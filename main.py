from patho_sys_preprocess.process import PathoSysPreprocess as TestOperator
import pandas as pd
from pathlib import Path

def get_samples(dataset_root="./dataset", source_type = "csv", target_type="csv"):
    dataset_path = Path(dataset_root).absolute()
    if not dataset_path.exists():
        print(f"Dataset root {dataset_root} does not exist.")
        return []
    
    dataset_id_dir = dataset_path / "source"

    samples = []
    if dataset_id_dir.is_dir():
        target_files = list(dataset_id_dir.glob(f"*.{source_type}"))

        for sample_file in target_files:
            sample_file: Path = sample_file
            sample = {
                "text": "",
                "data": "",
                "fileName": sample_file.name,
                "fileType": sample_file.suffix.lstrip('.'),
                "fileId"  : "1234567890",
                "filePath": str(sample_file),
                "fileSize": str(sample_file.stat().st_size),
                "export_path": str(dataset_path / "output"),
                "ext_params": "",
                "target_type": target_type
            }
            samples.append(sample)
    return samples

def test_operator():
    # Parameters from metadata.yml
    params = {
        'mountPoint': '/mnt/ruipath/hospital_data/',
        'ignoreSdpc': False
    }
    
    op = TestOperator(**params)
    
    samples = get_samples(source_type="csv", target_type="csv")
    print(f"Found {len(samples)} samples to process.")
    
    for sample in samples:
        try:
            result = op.execute(sample)
            print(f"Successfully processed {sample['fileName']}")
        except Exception as e:
            print(f"Failed to process {sample['fileName']}: {e}")


if __name__ == "__main__":
    test_operator()