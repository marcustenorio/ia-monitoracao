import os
from infrastructure.preprocessing_service import PreprocessingService
from infrastructure.labeling_service import LabelingService

def main():
    input_file = os.getenv("INPUT_FILE", "/data/processed/anomalies_dataset.csv")
    processed_file = os.getenv("PROCESSED_FILE", "/data/processed/dataset_ready.csv")
    labeled_file = os.getenv("LABELED_FILE", "/data/processed/dataset_labeled.csv")

    # 1 - Pré-processamento (ETL)
    preprocessing = PreprocessingService(input_file, processed_file)
    preprocessing.run()

    # 2 - Criação dos labels
    labeling = LabelingService(processed_file, labeled_file)
    labeling.run()

if __name__ == "__main__":
    main()
