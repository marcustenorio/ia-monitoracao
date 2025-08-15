import os
import pandas as pd
from infrastructure.preprocessing_service import PreprocessingService

def main():
    # Caminhos de entrada e saída
    input_file = os.getenv("INPUT_FILE", "/data/processed/anomalies_dataset.csv")
    output_file = os.getenv("OUTPUT_FILE", "/data/processed/dataset_ready.csv")

    # Executa o pré-processamento
    service = PreprocessingService(input_file, output_file)
    service.run()

    # Adiciona a coluna 'label'
    df = pd.read_csv(output_file)

    # Ajuste a lógica da criação de 'label' conforme a sua realidade
    if "priority" in df.columns:
        df["label"] = df["priority"].apply(lambda x: 1 if x > 0.5 else 0)
    else:
        raise ValueError("Coluna 'priority' não encontrada no dataset.")

    # Salva com o label incluído
    labeled_output = output_file.replace(".csv", "_labeled.csv")
    df.to_csv(labeled_output, index=False)

    print(f"Arquivo final salvo em: {labeled_output}")
    print(df.head())

if __name__ == "__main__":
    main()
