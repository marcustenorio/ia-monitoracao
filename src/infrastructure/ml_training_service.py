import json
import os
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

class MLTrainingService:
    """
    Adaptador (infraestrutura) responsável por treinar e persistir o modelo.
    O agente (ml_trainer) apenas chama este serviço.
    """

    def __init__(self,
                 label_column: str = "label",
                 feature_columns: Optional[List[str]] = None,
                 test_size: float = 0.2,
                 random_state: int = 42):
        self.label_column = label_column
        self.feature_columns = feature_columns
        self.test_size = test_size
        self.random_state = random_state

    def _select_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # Caso não especificado, escolhe features numéricas automaticamente (exclui a label)
        if self.feature_columns is not None:
            cols = [c for c in self.feature_columns if c in df.columns]
            return df[cols]
        else:
            numeric = df.select_dtypes(include=[np.number]).columns.tolist()
            numeric = [c for c in numeric if c != self.label_column]
            return df[numeric]

    def train_and_save(self,
                       input_csv: str,
                       model_output_path: str,
                       metrics_output_path: Optional[str] = None,
                       feature_importances_path: Optional[str] = None) -> Dict:
        if not os.path.exists(input_csv):
            raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_csv}")

        df = pd.read_csv(input_csv)
        if self.label_column not in df.columns:
            raise ValueError(f"Coluna de label '{self.label_column}' não encontrada em {input_csv}")

        X = self._select_features(df)
        y = df[self.label_column].astype(int)

        if X.empty:
            raise ValueError("Nenhuma feature numérica disponível para treino. Verifique o dataset.")

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.random_state, stratify=y
        )

        # Baseline simples
        clf = RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            random_state=self.random_state,
            n_jobs=-1,
        )
        clf.fit(X_train, y_train)

        # Avaliação
        y_pred = clf.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True)

        # Persistência
        os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
        joblib.dump({"model": clf, "features": list(X.columns)}, model_output_path)

        # Métricas
        metrics = {
            "accuracy": report.get("accuracy"),
            "precision_0": report.get("0", {}).get("precision"),
            "recall_0": report.get("0", {}).get("recall"),
            "precision_1": report.get("1", {}).get("precision"),
            "recall_1": report.get("1", {}).get("recall"),
            "macro_avg_f1": report.get("macro avg", {}).get("f1-score"),
            "weighted_avg_f1": report.get("weighted avg", {}).get("f1-score"),
        }

        if metrics_output_path:
            with open(metrics_output_path, "w") as f:
                json.dump(metrics, f, indent=2)

        # Importâncias (se disponíveis)
        if hasattr(clf, "feature_importances_") and feature_importances_path:
            imp_df = pd.DataFrame({
                "feature": X.columns,
                "importance": clf.feature_importances_
            }).sort_values("importance", ascending=False)
            imp_df.to_csv(feature_importances_path, index=False)

        return {
            "features_used": list(X.columns),
            "n_train": len(X_train),
            "n_test": len(X_test),
            "metrics": metrics
        }

