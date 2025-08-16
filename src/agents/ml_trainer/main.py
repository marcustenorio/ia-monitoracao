import os
import json
from infrastructure.ml_training_service import MLTrainingService

def main():
    input_csv = os.getenv("TRAIN_INPUT", "/data/processed/dataset_labeled.csv")
    model_path = os.getenv("MODEL_PATH", "/data/models/anomaly_model.pkl")
    metrics_path = os.getenv("METRICS_PATH", "/data/models/metrics.json")
    feat_imp_path = os.getenv("FEATURE_IMP_PATH", "/data/models/feature_importances.csv")

    # Se quiser fixar features, descomente e ajuste:
    # service = MLTrainingService(label_column="label", feature_columns=["priority", "triggerid"])
    service = MLTrainingService(label_column="label")

    result = service.train_and_save(
        input_csv=input_csv,
        model_output_path=model_path,
        metrics_output_path=metrics_path,
        feature_importances_path=feat_imp_path
    )

    print("Treino finalizado.")
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()

