import json
import pickle
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import shap
import torch
import torch.nn as nn
from sentence_transformers import SentenceTransformer
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, f1_score, precision_score,
                             recall_score)
from sklearn.metrics.pairwise import cosine_similarity

import classifier_utils
from config_loader import load_config

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
OUTPUT_DIR = DATA_DIR / "shap_analysis"


NEURAL_MODEL_FILE = TRAINED_BC_DIR / "neural_classifier.pth"
NEURAL_SCALER_FILE = TRAINED_BC_DIR / "neural_scaler.pkl"

SRC_DIR = BASE_DIR / "src"
EDITS_HISTORY_FILE = SRC_DIR / "501_manual_edits_history.json"

CONFIG = load_config()
MODEL_NAME = CONFIG['embedding']['model_name']
VECTOR_DIM = CONFIG['embedding']['dimension']


MAX_SHAP_SAMPLES = 50  


class VandalismClassifier(nn.Module):
    def __init__(self, input_dim):
        super(VandalismClassifier, self).__init__()
        self.fc1 = nn.Linear(input_dim, 256)
        self.bn1 = nn.BatchNorm1d(256)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(256, 128)
        self.bn2 = nn.BatchNorm1d(128)
        self.dropout2 = nn.Dropout(0.5)
        self.fc3 = nn.Linear(128, 64)
        self.dropout3 = nn.Dropout(0.4)
        self.fc4 = nn.Linear(64, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.dropout1(x)
        x = self.fc2(x)
        x = self.bn2(x)
        x = self.relu(x)
        x = self.dropout2(x)
        x = self.fc3(x)
        x = self.relu(x)
        x = self.dropout3(x)
        x = self.fc4(x)
        x = self.sigmoid(x)
        return x


def get_raw_features(edit, embedder, driver):
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    comment = edit.get('comment', '')
    
    new_emb = embedder.encode(new_text, convert_to_numpy=True) if new_text else np.zeros(VECTOR_DIM)
    old_emb = embedder.encode(original_text, convert_to_numpy=True) if original_text else np.zeros(VECTOR_DIM)
    comment_emb = embedder.encode(comment, convert_to_numpy=True) if comment else np.zeros(VECTOR_DIM)
    
    old_len = len(original_text)
    new_len = len(new_text)
    length_ratio = new_len / old_len if old_len > 0 else (1.0 if new_len == 0 else 10.0)
    
    if np.all(old_emb == 0) or np.all(new_emb == 0):
        semantic_similarity = 0.0
    else:
        semantic_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    if np.all(new_emb == 0):
        score_new_wiki = 0.0
        score_new_trusted = 0.0
    else:
        _, score_new_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, new_emb)
        _, score_new_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, new_emb)
    
    if np.all(old_emb == 0):
        score_old_wiki = 0.0
        score_old_trusted = 0.0
    else:
        _, score_old_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, old_emb)
        _, score_old_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, old_emb)
    
    features = np.concatenate([
        old_emb, new_emb, comment_emb,
        [semantic_similarity], [length_ratio], 
        [score_new_wiki], [score_new_trusted],
        [score_old_wiki], [score_old_trusted]
    ])
    
    return features


def load_edits(filepath):
    if not filepath.exists():
        print(f"⚠️  File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def create_feature_groups():
    return {
        "Old Text Embedding": (0, 384),
        "New Text Embedding": (384, 768),
        "Comment Embedding": (768, 1152),
        "Semantic Similarity": (1152, 1153),
        "Length Ratio": (1153, 1154),
        "RAG: New→Wiki": (1154, 1155),
        "RAG: New→Trusted": (1155, 1156),
        "RAG: Old→Wiki": (1156, 1157),
        "RAG: Old→Trusted": (1157, 1158),
    }


def plot_confusion_matrix(y_true, y_pred, output_path):
    cm = confusion_matrix(y_true, y_pred)
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=['Legitimate', 'Vandalism'],
                yticklabels=['Legitimate', 'Vandalism'])
    plt.xlabel('Predicted Label', fontsize=12)
    plt.ylabel('True Label', fontsize=12)
    plt.title('Confusion Matrix - Neural Classifier', fontsize=14)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"   ✅ Confusion Matrix salvata: {output_path.name}")
    
    return cm


def plot_shap_summary(shap_values, X, feature_names, output_path):
    plt.figure(figsize=(12, 8))
    shap.summary_plot(shap_values, X, feature_names=feature_names, 
                      show=False, max_display=20)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"   ✅ SHAP Summary salvato: {output_path.name}")


def plot_grouped_importance(shap_values, feature_groups, output_path):
    group_importance = {}
    
    for group_name, (start, end) in feature_groups.items():
        group_shap = np.abs(shap_values[:, start:end]).mean()
        group_importance[group_name] = group_shap
    
    sorted_groups = sorted(group_importance.items(), key=lambda x: x[1], reverse=True)
    groups = [g[0] for g in sorted_groups]
    values = [g[1] for g in sorted_groups]
    
    plt.figure(figsize=(10, 6))
    colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(groups)))
    bars = plt.barh(range(len(groups)), values, color=colors)
    plt.yticks(range(len(groups)), groups)
    plt.xlabel('Mean |SHAP Value|', fontsize=12)
    plt.title('Feature Group Importance (Aggregated SHAP)', fontsize=14)
    plt.gca().invert_yaxis()
    
    for bar, val in zip(bars, values):
        plt.text(val + 0.001, bar.get_y() + bar.get_height()/2, 
                 f'{val:.4f}', va='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"   ✅ Grouped Importance salvato: {output_path.name}")
    
    return sorted_groups


def plot_rag_features_detail(shap_values, feature_groups, output_path):
    engineered_features = [
        ("Semantic Similarity", 1152),
        ("Length Ratio", 1153),
        ("RAG: New→Wiki", 1154),
        ("RAG: New→Trusted", 1155),
        ("RAG: Old→Wiki", 1156),
        ("RAG: Old→Trusted", 1157),
    ]
    
    feature_names = [f[0] for f in engineered_features]
    feature_indices = [f[1] for f in engineered_features]
    
    shap_subset = shap_values[:, feature_indices]
    
    mean_shap = np.mean(shap_subset, axis=0).flatten()
    std_shap = np.std(shap_subset, axis=0).flatten()
    
    mean_shap_list = mean_shap.tolist()
    std_shap_list = std_shap.tolist()
    
    plt.figure(figsize=(10, 6))
    colors = ['green' if m < 0 else 'red' for m in mean_shap_list]
    
    x = np.arange(len(feature_names))
    bars = plt.bar(x, mean_shap_list, yerr=std_shap_list, capsize=5, color=colors, alpha=0.7, edgecolor='black', linewidth=0.5)
    plt.xticks(x, feature_names, rotation=45, ha='right')
    plt.ylabel('Mean SHAP Value', fontsize=12)
    plt.xlabel('Feature', fontsize=12)
    plt.title('Impact of Engineered Features on Prediction\n(Red = pushes towards Vandalism, Green = pushes towards Legitimate)', 
              fontsize=12)
    plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"   ✅ RAG Features Detail salvato: {output_path.name}")
    
    return dict(zip(feature_names, mean_shap_list))


def main():
    print("=" * 70)
    print("📊 SHAP ANALYSIS - Neural Classifier Explainability")
    print("=" * 70)
    
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("\n🔧 Caricamento risorse...")
    
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        print("❌ Impossibile connettersi a Neo4j")
        return
    
    if not NEURAL_MODEL_FILE.exists():
        print(f"❌ Modello non trovato: {NEURAL_MODEL_FILE}")
        print("   Esegui prima: python 13_train_neural_complete.py")
        driver.close()
        return
    
    with open(NEURAL_SCALER_FILE, "rb") as f:
        scaler = pickle.load(f)
    
    input_dim = scaler.n_features_in_
    model = VandalismClassifier(input_dim)
    model.load_state_dict(torch.load(NEURAL_MODEL_FILE, map_location='cpu'))
    model.eval()
    
    print(f"   ✅ Modello caricato (input_dim: {input_dim})")
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"   ✅ Embedder caricato: {MODEL_NAME}")
    
    print("\n📂 Caricamento dataset di test...")
    
    if not EDITS_HISTORY_FILE.exists():
        print(f"❌ File non trovato: {EDITS_HISTORY_FILE}")
        driver.close()
        return
    
    all_edits = load_edits(EDITS_HISTORY_FILE)
    
    # Separa legit e vandal in base a is_vandalism
    legit_edits = [e for e in all_edits if not e.get('is_vandalism', False)]
    vandal_edits = [e for e in all_edits if e.get('is_vandalism', False)]
    
    print(f"   Usando: {EDITS_HISTORY_FILE.name}")
    print(f"   Legit: {len(legit_edits)}, Vandal: {len(vandal_edits)}")
    
    print("\n⚙️  Estrazione features...")
    X = []
    y = []
    
    total = len(legit_edits) + len(vandal_edits)
    for i, edit in enumerate(legit_edits):
        feat = get_raw_features(edit, embedder, driver)
        X.append(feat)
        y.append(0)
        if (i + 1) % 20 == 0:
            sys.stdout.write(f"\r   Processati {i+1}/{total}")
            sys.stdout.flush()
    
    for i, edit in enumerate(vandal_edits):
        feat = get_raw_features(edit, embedder, driver)
        X.append(feat)
        y.append(1)
        if (len(legit_edits) + i + 1) % 20 == 0:
            sys.stdout.write(f"\r   Processati {len(legit_edits) + i + 1}/{total}")
            sys.stdout.flush()
    
    X = np.array(X)
    y = np.array(y)
    print(f"\n   ✅ Features shape: {X.shape}")
    
    X_scaled = scaler.transform(X)
    
    print("\n📈 Calcolo predizioni...")
    
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X_scaled)
        outputs = model(X_tensor)
        y_pred = (outputs.numpy() > 0.5).astype(int).flatten()
        y_proba = outputs.numpy().flatten()
    
    accuracy = accuracy_score(y, y_pred)
    f1 = f1_score(y, y_pred)
    precision = precision_score(y, y_pred)
    recall = recall_score(y, y_pred)
    
    print(f"\n   📊 METRICHE:")
    print(f"   {'Accuracy':<15}: {accuracy*100:.2f}%")
    print(f"   {'F1 Score':<15}: {f1*100:.2f}%")
    print(f"   {'Precision':<15}: {precision*100:.2f}%")
    print(f"   {'Recall':<15}: {recall*100:.2f}%")
    
    print("\n   📋 CLASSIFICATION REPORT:")
    print(classification_report(y, y_pred, target_names=['Legitimate', 'Vandalism']))
    
    print("\n🎨 Generazione visualizzazioni...")
    cm = plot_confusion_matrix(y, y_pred, OUTPUT_DIR / "confusion_matrix.png")
    
    print(f"\n   CONFUSION MATRIX:")
    print(f"   {'':>15} Pred_Legit  Pred_Vandal")
    print(f"   {'True_Legit':>15}     {cm[0,0]:4d}        {cm[0,1]:4d}")
    print(f"   {'True_Vandal':>15}     {cm[1,0]:4d}        {cm[1,1]:4d}")
    
    print("\n🔬 Calcolo SHAP values...")
    print(f"   (usando {min(MAX_SHAP_SAMPLES, len(X_scaled))} samples come background)")
    
    def model_predict(x):
        model.eval()
        with torch.no_grad():
            tensor = torch.FloatTensor(x)
            return model(tensor).numpy()
    
    background_size = min(MAX_SHAP_SAMPLES, len(X_scaled))
    background_idx = np.random.choice(len(X_scaled), background_size, replace=False)
    background = X_scaled[background_idx]
    
    explainer = shap.KernelExplainer(model_predict, background)
    
    shap_sample_size = min(MAX_SHAP_SAMPLES, len(X_scaled))
    sample_idx = np.random.choice(len(X_scaled), shap_sample_size, replace=False)
    X_sample = X_scaled[sample_idx]
    
    print(f"   Calcolo SHAP su {shap_sample_size} samples...")
    shap_values = explainer.shap_values(X_sample)
    
    if isinstance(shap_values, list):
        shap_values = shap_values[0]
    
    print("   ✅ SHAP values calcolati")
    
    feature_groups = create_feature_groups()
    
    sorted_groups = plot_grouped_importance(
        shap_values, feature_groups, 
        OUTPUT_DIR / "grouped_feature_importance.png"
    )
    
    print("\n   📊 FEATURE GROUP IMPORTANCE:")
    for group, importance in sorted_groups:
        bar = "█" * int(importance * 100)
        print(f"   {group:<25}: {importance:.4f} {bar}")
    
    rag_impact = plot_rag_features_detail(
        shap_values, feature_groups,
        OUTPUT_DIR / "rag_features_impact.png"
    )
    
    print("\n   📊 ENGINEERED FEATURES IMPACT (mean SHAP):")
    for feat, impact in rag_impact.items():
        direction = "→ VANDAL" if impact > 0 else "→ LEGIT"
        print(f"   {feat:<25}: {impact:+.4f} {direction}")
    
    report = {
        "model": "neural_classifier (VandalismClassifier)",
        "dataset": {
            "legit_samples": len(legit_edits),
            "vandal_samples": len(vandal_edits),
            "total": len(y)
        },
        "metrics": {
            "accuracy": float(accuracy),
            "f1_score": float(f1),
            "precision": float(precision),
            "recall": float(recall)
        },
        "confusion_matrix": {
            "true_legit_pred_legit": int(cm[0, 0]),
            "true_legit_pred_vandal": int(cm[0, 1]),
            "true_vandal_pred_legit": int(cm[1, 0]),
            "true_vandal_pred_vandal": int(cm[1, 1])
        },
        "feature_group_importance": {g: float(v) for g, v in sorted_groups},
        "engineered_features_impact": {k: float(v) for k, v in rag_impact.items()}
    }
    
    report_file = OUTPUT_DIR / "shap_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
    
    print(f"\n   ✅ Report JSON salvato: {report_file.name}")
    
    driver.close()
    
    print("\n" + "=" * 70)
    print("✨ ANALISI COMPLETATA!")
    print(f"   Output salvati in: {OUTPUT_DIR}")
    print("   Files generati:")
    print("   • confusion_matrix.png")
    print("   • grouped_feature_importance.png")
    print("   • rag_features_impact.png")
    print("   • shap_report.json")
    print("=" * 70)


if __name__ == "__main__":
    main()
