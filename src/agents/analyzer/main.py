import os
from infrastructure.preprocessing_service import PreprocessingService

def main():
    input_file = os.getenv("INPUT_FILE", "/data/processed/anomalies_dataset.csv")
    output_file = os.getenv("OUTPUT_FILE", "/data/processed/dataset_ready.csv")

    service = PreprocessingService(input_file, output_file)
    service.run()

if __name__ == "__main__":
    main()

