# Autor: Marcus Tenório
# Script principal para execução do treinamento

import os
from infrastructure.ml_training_service import MLTrainingService


def main():
    service = MLTrainingService()

    input_path = os.getenv("TRAIN_INPUT", "data/processed/dataset_labeled.csv")
    model_path = os.getenv("MODEL_PATH", "data/models/anomaly_model.pkl")
    metrics_path = os.getenv("METRICS_PATH", "data/reports/metrics.json")
    feature_imp_path = os.getenv("FEATURE_IMP_PATH", "data/reports/feature_importances.csv")

    result = service.train_and_save(
        input_path=input_path,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_imp_path=feature_imp_path,
    )

    print("✅ Treinamento concluído.")
    print("📌 Métricas salvas em:", metrics_path)
    print("📌 Modelo salvo em:", model_path)
    print("📌 Importância das features em:", feature_imp_path)


if __name__ == "__main__":
    main()
