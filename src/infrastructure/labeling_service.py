import pandas as pd

class LabelingService:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def run(self):
        # Carrega o dataset
        df = pd.read_csv(self.input_file)

        # Cria a coluna label (priority > 0.5 = 1, caso contrário 0)
        df["label"] = df["priority"].apply(lambda x: 1 if x > 0.5 else 0)

        # Salva no novo arquivo
        df.to_csv(self.output_file, index=False)
        print(f"Arquivo com labels salvo em: {self.output_file}")

