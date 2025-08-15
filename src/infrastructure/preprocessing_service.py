import pandas as pd
import os

class PreprocessingService:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

    def run(self):
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Arquivo {self.input_path} não encontrado.")

        print(f"[PreprocessingService] Lendo dados de {self.input_path}...")
        df = pd.read_csv(self.input_path)

        # Remover duplicatas
        df.drop_duplicates(inplace=True)

        # Tratar valores ausentes (exemplo: preencher com 0)
        df.fillna(0, inplace=True)

        # Normalizar colunas numéricas
        num_cols = df.select_dtypes(include=["int64", "float64"]).columns
        for col in num_cols:
            max_val = df[col].max()
            min_val = df[col].min()
            if max_val != min_val:
                df[col] = (df[col] - min_val) / (max_val - min_val)

        # Salvar dataset pronto
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)

        print(f"[PreprocessingService] Dataset pronto salvo em {self.output_path}")

