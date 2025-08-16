import os
import joblib
import pandas as pd
from typing import Dict, List

class ModelInferenceService:
    """
    Carrega o modelo treinado (joblib) e faz predições em lote.
    Espera um dict com {"model": sklearn_estimator, "features": [..]}.
    """

    def __init__(self, model_path: str):
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Modelo não encontrado em: {model_path}")
        bundle = joblib.load(model_path)
        self.model = bundle["model"]
        self.features: List[str] = bundle["features"]

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # Garante as colunas esperadas pelo modelo; preenche ausentes com 0
        X = pd.DataFrame()
        for col in self.features:
            X[col] = df[col] if col in df.columns else 0
        return X

    def predict_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retorna DataFrame com colunas:
          - pred_label
          - pred_score (probabilidade da classe 1, se disponível)
        """
        X = self.prepare_features(df)
        result = pd.DataFrame(index=df.index)
        if hasattr(self.model, "predict_proba"):
            proba = self.model.predict_proba(X)
            result["pred_score"] = proba[:, 1]
        else:
            result["pred_score"] = 0.0
        result["pred_label"] = self.model.predict(X)
        return result

