import json
import pickle
import sys
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score, StratifiedKFold, train_test_split
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sentence_transformers import SentenceTransformer
import classifier_utils


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
MODEL_FILE = TRAINED_BC_DIR / "binary_classifier.pkl"
SCALER_FILE = TRAINED_BC_DIR / "scaler.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

from config_loader import load_config
CONFIG = load_config()
MODEL_NAME = CONFIG['embedding']['model_name']


TRAIN_SPLIT = 0.5  

def load_all_edits(filepath):
    """Carica tutti gli edit dal file"""
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def evaluate_models(X, y):
    """Confronta diversi modelli con cross-validation e restituisce il migliore."""
    print("\n🔬 CONFRONTO MODELLI (5-Fold Cross-Validation):")
    print("-" * 60)
    
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    models = {
        "Logistic Regression (C=0.1)": LogisticRegression(C=0.1, max_iter=1000, random_state=42),
        "Logistic Regression (C=1.0)": LogisticRegression(C=1.0, max_iter=1000, random_state=42),
        "Logistic Regression (C=0.01)": LogisticRegression(C=0.01, max_iter=1000, random_state=42),
        "Random Forest (n=50)": RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42),
        "Random Forest (n=100)": RandomForestClassifier(n_estimators=100, max_depth=3, random_state=42),
        "SVM (RBF, C=1)": SVC(kernel='rbf', C=1.0, probability=True, random_state=42),
        "SVM (RBF, C=0.1)": SVC(kernel='rbf', C=0.1, probability=True, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=42),
    }
    
    best_model = None
    best_score = 0
    best_name = ""
    results = []
    
    for name, model in models.items():
        scores = cross_val_score(model, X, y, cv=cv, scoring='f1')
        mean_score = scores.mean()
        std_score = scores.std()
        results.append((name, mean_score, std_score))
        
        if mean_score > best_score:
            best_score = mean_score
            best_model = model
            best_name = name
    
    # Ordina per score
    results.sort(key=lambda x: x[1], reverse=True)
    
    for name, mean, std in results:
        marker = "🏆" if name == best_name else "  "
        print(f"   {marker} {name:<35} F1: {mean:.2%} ± {std:.2%}")
    
    print("-" * 60)
    print(f"   ✅ Miglior modello: {best_name}")
    
    return best_model, best_name

def analyze_weights(clf, model_name):
    """Stampa un'analisi leggibile dei pesi del modello."""
    print(f"\n🔍 ANALISI MODELLO ({model_name}):")
    
    if hasattr(clf, 'coef_'):
        weights = clf.coef_[0]

        delta_weights = weights[0:384]
        comment_weights = weights[384:768]
        text_sim_weight = weights[768] if len(weights) > 768 else 0
        length_ratio_weight = weights[769] if len(weights) > 769 else 0
        truth_new_wiki_weight = weights[770] if len(weights) > 770 else 0
        truth_new_trusted_weight = weights[771] if len(weights) > 771 else 0
        truth_old_wiki_weight = weights[772] if len(weights) > 772 else 0
        truth_old_trusted_weight = weights[773] if len(weights) > 773 else 0
        
        avg_delta_impact = np.mean(np.abs(delta_weights))
        avg_comment_impact = np.mean(np.abs(comment_weights))
        
        print(f"   📊 Impatto 'Semantic Delta': {avg_delta_impact:.4f}")
        print(f"   📊 Impatto 'Comment Intent': {avg_comment_impact:.4f}")
        print(f"   🔥 Peso 'Text Similarity': {text_sim_weight:.4f}")
        print(f"   📏 Peso 'Length Ratio': {length_ratio_weight:.4f}")
        print(f"   ✅ Peso 'Truth Score (New Wiki)': {truth_new_wiki_weight:.4f}")
        print(f"   ✅ Peso 'Truth Score (New Trusted)': {truth_new_trusted_weight:.4f}")
        print(f"   📚 Peso 'Truth Score (Old Wiki)': {truth_old_wiki_weight:.4f}")
        print(f"   📚 Peso 'Truth Score (Old Trusted)': {truth_old_trusted_weight:.4f}")
        
        print("\n   💡 INTERPRETAZIONE:")
        if text_sim_weight > 0:
            print(f"   • Text Similarity POSITIVO ({text_sim_weight:.3f}): alta similarità → più probabile VANDALISMO (strano!)")
        else:
            print(f"   • Text Similarity NEGATIVO ({text_sim_weight:.3f}): alta similarità → più probabile LEGITTIMO ✅")
        
        if truth_new_wiki_weight < 0:
            print(f"   • Truth New Wiki NEGATIVO ({truth_new_wiki_weight:.3f}): nuovo testo simile a Wiki → LEGITTIMO ✅")
        
    elif hasattr(clf, 'feature_importances_'):
        importances = clf.feature_importances_
        delta_imp = np.mean(importances[0:384])
        comment_imp = np.mean(importances[384:768])
        text_sim_imp = importances[768] if len(importances) > 768 else 0
        length_ratio_imp = importances[769] if len(importances) > 769 else 0
        truth_new_wiki_imp = importances[770] if len(importances) > 770 else 0
        truth_new_trusted_imp = importances[771] if len(importances) > 771 else 0
        truth_old_wiki_imp = importances[772] if len(importances) > 772 else 0
        truth_old_trusted_imp = importances[773] if len(importances) > 773 else 0
        
        print(f"   📊 Importanza 'Semantic Delta': {delta_imp:.4f}")
        print(f"   📊 Importanza 'Comment Intent': {comment_imp:.4f}")
        print(f"   🔥 Importanza 'Text Similarity': {text_sim_imp:.4f}")
        print(f"   📏 Importanza 'Length Ratio': {length_ratio_imp:.4f}")
        print(f"   ✅ Importanza 'Truth Score (New Wiki)': {truth_new_wiki_imp:.4f}")
        print(f"   ✅ Importanza 'Truth Score (New Trusted)': {truth_new_trusted_imp:.4f}")
        print(f"   📚 Importanza 'Truth Score (Old Wiki)': {truth_old_wiki_imp:.4f}")
        print(f"   📚 Importanza 'Truth Score (Old Trusted)': {truth_old_trusted_imp:.4f}")
    else:
        print("   (Modello senza pesi interpretabili)")

def main():
    print("=" * 60)
    print("🧠 TRAINING BINARY CLASSIFIER - VANDALISM DETECTION")
    print(f"   Train/Test Split: {TRAIN_SPLIT*100:.0f}% / {(1-TRAIN_SPLIT)*100:.0f}%")
    print("=" * 60)
    
    # 0. Setup Directory
    if not TRAINED_BC_DIR.exists():
        TRAINED_BC_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Connect Neo4j
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        return
    
    # 2. Load Embedding Model
    print(f"\n🚀 Caricamento Modello Embeddings: {MODEL_NAME}...")
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 3. Load ALL Data
    legit_edits = load_all_edits(LEGIT_FILE)
    vandal_edits = load_all_edits(VANDAL_FILE)
    
    all_edits = legit_edits + vandal_edits
    labels = [0] * len(legit_edits) + [1] * len(vandal_edits)
    
    print(f"\n📊 Dataset Totale: {len(legit_edits)} Legit + {len(vandal_edits)} Vandal = {len(all_edits)}")
    
    if not all_edits:
        print("❌ Nessun dato per il training.")
        driver.close()
        return

    # 4. Feature Engineering
    print("\n⚙️  Generazione Features...")
    X = []
    for i, edit in enumerate(all_edits):
        feat = classifier_utils.get_features(edit, embedder, driver)
        X.append(feat)
        if (i + 1) % 20 == 0 or i == len(all_edits) - 1:
            sys.stdout.write(f"\r   Processati {i+1}/{len(all_edits)}")
            sys.stdout.flush()
            
    X = np.array(X)
    y = np.array(labels)
    print(f"\n   ✅ Features shape: {X.shape}")
    
    # 5. TRAIN/TEST SPLIT
    print(f"\n📂 Split Dataset ({TRAIN_SPLIT*100:.0f}% train / {(1-TRAIN_SPLIT)*100:.0f}% test)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=(1 - TRAIN_SPLIT), 
        random_state=42, 
        stratify=y  # Mantiene proporzioni classi
    )
    print(f"   Train: {len(X_train)} samples")
    print(f"   Test:  {len(X_test)} samples")
    
    # 6. Scaling (fit solo su train!)
    print("\n⚖️  Normalizzazione Features (StandardScaler)...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)  # Fit su train
    X_test_scaled = scaler.transform(X_test)        # Transform su test
    
    with open(SCALER_FILE, "wb") as f:
        pickle.dump(scaler, f)
    print(f"   ✅ Scaler salvato: {SCALER_FILE}")

    # 7. Model Selection con Cross-Validation sul TRAIN set
    best_model, best_name = evaluate_models(X_train_scaled, y_train)
    
    # 8. Train finale sul train set
    print(f"\n🏋️  Training finale con {best_name}...")
    best_model.fit(X_train_scaled, y_train)
    
    # 9. Valutazione su TRAIN e TEST set
    train_acc = best_model.score(X_train_scaled, y_train)
    test_acc = best_model.score(X_test_scaled, y_test)
    
    y_pred_train = best_model.predict(X_train_scaled)
    y_pred_test = best_model.predict(X_test_scaled)
    
    train_f1 = f1_score(y_train, y_pred_train)
    test_f1 = f1_score(y_test, y_pred_test)
    
    print(f"\n📈 PERFORMANCE:")
    print(f"   {'Metrica':<15} {'Train':<12} {'Test':<12}")
    print(f"   {'-'*39}")
    print(f"   {'Accuracy':<15} {train_acc:<12.2%} {test_acc:<12.2%}")
    print(f"   {'F1-Score':<15} {train_f1:<12.2%} {test_f1:<12.2%}")
    
    # Avviso overfitting
    if train_acc - test_acc > 0.15:
        print(f"\n   ⚠️  ATTENZIONE: Possibile overfitting (gap train-test > 15%)")
    
    # 10. Classification Report e Confusion Matrix su TEST
    print(f"\n📊 CLASSIFICATION REPORT (Test Set):")
    print(classification_report(y_test, y_pred_test, target_names=['LEGIT', 'VANDAL']))
    
    cm = confusion_matrix(y_test, y_pred_test)
    print(f"   CONFUSION MATRIX (Test Set):")
    print(f"                 Predicted")
    print(f"                 LEGIT  VANDAL")
    print(f"   Actual LEGIT   {cm[0,0]:3d}    {cm[0,1]:3d}")
    print(f"   Actual VANDAL  {cm[1,0]:3d}    {cm[1,1]:3d}")
    
    # 11. Save Model
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(best_model, f)
    print(f"\n✅ Modello salvato: {MODEL_FILE}")
    
    # 12. Analisi pesi
    analyze_weights(best_model, best_name)
    
    driver.close()
    print("\n" + "=" * 60)
    print("✨ Training completato!")
    print("=" * 60)

if __name__ == "__main__":
    main()