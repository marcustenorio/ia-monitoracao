# src/infrastructure/ml_training_service.py
# Serviço de treinamento com engenharia de features, métricas e gráficos
# Compatível com seu main.py atual (train_and_save)

import os
import json
import ast
from typing import Optional, Tuple, List

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    precision_recall_fscore_support,
    roc_auc_score,
    roc_curve,
    confusion_matrix,
    ConfusionMatrixDisplay,
)
from sklearn.model_selection import train_test_split

# backend headless para salvar imagens em container/servidor
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ----------------------------
# Helpers de engenharia de features
# ----------------------------

def _parse_hosts_len(x: str) -> int:
    """
    Converte a string do tipo "[{'hostid': '10084', 'name': 'Zabbix server'}]"
    no tamanho da lista (quantos hosts). Em caso de erro, retorna 1.
    """
    try:
        v = ast.literal_eval(str(x))
        return len(v) if isinstance(v, list) else 1
    except Exception:
        return 1


def _build_numeric_view(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Cria uma visão NUMÉRICA do dataset:
      - desc_len: len(description)
      - host_count: len(lista de hosts)
      - priority, triggerid, lastchange convertidos para float
    """
    work = df.copy()

    work["desc_len"] = work["description"].astype(str).str.len()
    work["host_count"] = work["hosts"].astype(str).apply(_parse_hosts_len)

    for col in ["priority", "triggerid", "lastchange", "desc_len", "host_count"]:
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0.0)

    features = ["priority", "triggerid", "lastchange", "desc_len", "host_count"]
    X = work[features]
    return X, features


# ----------------------------
# Serviço principal
# ----------------------------

class MLTrainingService:
    def __init__(self):
        """Serviço de treinamento com features numéricas embutidas."""
        pass

    def train_and_save(
        self,
        input_path: str,
        model_path: str,
        metrics_path: str,
        feature_imp_path: Optional[str] = None,
        *,
        test_size: float = 0.3,
        random_state: int = 42,
        n_estimators: int = 200,
        max_depth: Optional[int] = None,
        generate_plots: bool = True,            # <-- habilita gráficos por padrão
        plots_dir: Optional[str] = None,        # ex.: "/data/reports"
    ):
        """
        Treina RandomForest e salva artefatos (modelo, métricas, importâncias e gráficos).

        Args (compatíveis com seu main.py):
          - input_path: CSV de entrada (DEVE conter 'label' 0/1)
          - model_path: caminho para salvar o modelo .pkl
          - metrics_path: caminho para salvar métricas em JSON
          - feature_imp_path: caminho para salvar importâncias (CSV)

        Kwargs úteis:
          - test_size, random_state, n_estimators, max_depth
          - generate_plots: se True, salva ROC e Matriz de Confusão
          - plots_dir: diretório para gráficos (obrigatório se generate_plots=True)
        """
        # 1) Carregar dataset
        df = pd.read_csv(input_path)
        if "label" not in df.columns:
            raise ValueError("O dataset precisa conter a coluna 'label' (0/1).")

        y = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)

        # 2) Features numéricas
        X, feat_names = _build_numeric_view(df)

        # 3) Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )

        # 4) Modelo
        clf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            class_weight="balanced_subsample",
            random_state=random_state,
            n_jobs=-1,
        )
        clf.fit(X_train, y_train)

        # 5) Avaliação
        # Relatório padrão (usa threshold interno do RF) e evita warnings nas métricas
        y_pred = clf.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)

        # Probabilidade para ROC e threshold alternativo
        diagnostics = {"features": feat_names}
        try:
            y_proba = clf.predict_proba(X_test)[:, 1]

            # Métricas com threshold alternativo 0.4 (diagnóstico)
            y_pred_alt = (y_proba >= 0.4).astype(int)
            p, r, f1, _ = precision_recall_fscore_support(
                y_test, y_pred_alt, average="binary", zero_division=0
            )
            try:
                auc = roc_auc_score(y_test, y_proba)
            except Exception:
                auc = None

            diagnostics["alt_threshold_metrics"] = {
                "thr": 0.4,
                "precision": float(p),
                "recall": float(r),
                "f1": float(f1),
                "roc_auc": (float(auc) if auc is not None else None),
            }
        except Exception:
            y_proba = None  # modelo sem predict_proba

        report["_diagnostics_"] = diagnostics

        # 6) Persistência (modelo/métricas/importâncias)
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
        if feature_imp_path:
            os.makedirs(os.path.dirname(feature_imp_path), exist_ok=True)

        joblib.dump(clf, model_path)

        with open(metrics_path, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        if feature_imp_path and hasattr(clf, "feature_importances_"):
            fi = pd.DataFrame(
                {"feature": feat_names, "importance": clf.feature_importances_}
            ).sort_values("importance", ascending=False)
            fi.to_csv(feature_imp_path, index=False)

        # 7) Gráficos (opcional)
        if generate_plots:
            if not plots_dir:
                # usa o diretório das métricas por padrão
                plots_dir = os.path.dirname(metrics_path)
            os.makedirs(plots_dir, exist_ok=True)

            # ROC (se houver proba)
            if y_proba is not None:
                try:
                    fpr, tpr, _ = roc_curve(y_test, y_proba)
                    plt.figure()
                    plt.plot(fpr, tpr, linewidth=2)
                    plt.plot([0, 1], [0, 1], "--")
                    plt.xlabel("FPR")
                    plt.ylabel("TPR")
                    plt.title("ROC Curve")
                    plt.savefig(os.path.join(plots_dir, "roc_curve.png"), bbox_inches="tight")
                    plt.close()
                except Exception as e:
                    print(f"[ml_trainer] aviso: falha ao salvar ROC: {e}")

            # Matriz de confusão (threshold 0.5)
            try:
                cm = confusion_matrix(y_test, y_pred)
                disp = ConfusionMatrixDisplay(confusion_matrix=cm)
                disp.plot()
                plt.title("Confusion Matrix (thr=0.5)")
                plt.savefig(os.path.join(plots_dir, "confusion_matrix.png"), bbox_inches="tight")
                plt.close()
            except Exception as e:
                print(f"[ml_trainer] aviso: falha ao salvar matriz de confusão: {e}")

        # Logs úteis
        print("[ml_trainer] features usadas:", feat_names)
        print("[ml_trainer] modelo salvo em:", model_path)
        print("[ml_trainer] métricas em:", metrics_path)
        if feature_imp_path:
            print("[ml_trainer] importâncias em:", feature_imp_path)

        return {
            "model_path": model_path,
            "metrics_path": metrics_path,
            "feature_importances_path": feature_imp_path,
            "features": feat_names,
            "report": report,
        }
