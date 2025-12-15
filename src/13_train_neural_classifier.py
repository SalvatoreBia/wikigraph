"""
13_train_neural_classifier.py
Training di un classificatore neurale PyTorch per rilevamento vandalismo.
Usa embedding grezzi invece di feature ingegnerizzate per gestire meglio i sinonimi.
"""

import json
import pickle
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, f1_score, accuracy_score
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader

# Import shared utils
import classifier_utils

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
MODEL_FILE = TRAINED_BC_DIR / "neural_classifier.pth"
SCALER_FILE = TRAINED_BC_DIR / "neural_scaler.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

from config_loader import load_config
CONFIG = load_config()
MODEL_NAME = CONFIG['embedding']['model_name']

# --- HYPERPARAMETERS ---
TRAIN_SPLIT = 0.8
EPOCHS = 100
BATCH_SIZE = 8
LEARNING_RATE = 0.001
WEIGHT_DECAY = 0.01  # L2 regularization
PATIENCE = 15  # Early stopping


class VandalismClassifier(nn.Module):
    """Rete neurale per classificazione vandalismo"""
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
    """
    Estrae feature grezze: embedding completi invece di delta semantico.
    Questo permette alla rete di imparare pattern più complessi,
    inclusa la gestione dei sinonimi.
    
    AGGIORNAMENTO RAG: Include score di triangolazione (Wiki + Trusted)
    """
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    comment = edit.get('comment', '')
    
    # Embedding del nuovo testo
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
    
    # Embedding del testo originale
    if original_text:
        old_emb = embedder.encode(original_text, convert_to_numpy=True)
    else:
        old_emb = np.zeros(384)
    
    # Embedding del commento
    if comment:
        comment_emb = embedder.encode(comment, convert_to_numpy=True)
    else:
        comment_emb = np.zeros(384)
    
    # Feature aggiuntive
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    
    # FEATURE CHIAVE: Similarità semantica tra vecchio e nuovo testo
    # Alta similarità = probabilmente sinonimi/riformulazione legittima
    if np.all(old_emb == 0) or np.all(new_emb == 0):
        semantic_similarity = 0.0
    else:
        semantic_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    # Truth scores da Neo4j (TRIANGOLAZIONE)
    # 1. NEW TEXT vs WIKI & TRUSTED
    if np.all(new_emb == 0):
        score_new_wiki = 0.0
        score_new_trusted = 0.0
    else:
        _, score_new_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, new_emb)
        _, score_new_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, new_emb)
    
    # 2. OLD TEXT vs WIKI & TRUSTED
    if np.all(old_emb == 0):
        score_old_wiki = 0.0
        score_old_trusted = 0.0
    else:
        _, score_old_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, old_emb)
        _, score_old_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, old_emb)
    
    # Concatena tutto: 384*3 + 2 + 4 = 1158 features
    # (OldEmb, NewEmb, CommentEmb, SemSim, LenRatio, NewWiki, NewTrusted, OldWiki, OldTrusted)
    features = np.concatenate([
        old_emb, new_emb, comment_emb,
        [semantic_similarity], [length_ratio], 
        [score_new_wiki], [score_new_trusted],
        [score_old_wiki], [score_old_trusted]
    ])
    
    return features


def load_data():
    """Carica i dati di training dai file JSON"""
    print("📂 Caricamento dati...")
    
    if not LEGIT_FILE.exists() or not VANDAL_FILE.exists():
        print("❌ File mock non trovati!")
        return None, None
    
    with open(LEGIT_FILE, 'r', encoding='utf-8') as f:
        legit_edits = json.load(f)
    
    with open(VANDAL_FILE, 'r', encoding='utf-8') as f:
        vandal_edits = json.load(f)
    
    print(f"   ✅ Legit: {len(legit_edits)}, Vandal: {len(vandal_edits)}")
    return legit_edits, vandal_edits


def extract_features(edits, embedder, driver, label):
    """Estrae feature per una lista di edit"""
    X = []
    y = []
    
    for edit in edits:
        feat = get_raw_features(edit, embedder, driver)
        X.append(feat)
        y.append(label)
    
    return X, y


def train_model(X_train, y_train, X_val, y_val, input_dim, device):
    """Training del modello PyTorch con early stopping"""
    
    # Prepara DataLoader
    train_dataset = TensorDataset(
        torch.FloatTensor(X_train),
        torch.FloatTensor(y_train).unsqueeze(1)
    )
    val_dataset = TensorDataset(
        torch.FloatTensor(X_val),
        torch.FloatTensor(y_val).unsqueeze(1)
    )
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    
    # Modello
    model = VandalismClassifier(input_dim).to(device)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', patience=5, factor=0.5)
    
    # Early stopping
    best_val_loss = float('inf')
    patience_counter = 0
    best_model_state = None
    
    print(f"\n🏋️ Training per {EPOCHS} epochs (early stopping patience={PATIENCE})...")
    print("-" * 60)
    
    for epoch in range(EPOCHS):
        # Training
        model.train()
        train_loss = 0.0
        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            
            optimizer.zero_grad()
            outputs = model(X_batch)
            loss = criterion(outputs, y_batch)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        train_loss /= len(train_loader)
        
        # Validation
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for X_batch, y_batch in val_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                outputs = model(X_batch)
                loss = criterion(outputs, y_batch)
                val_loss += loss.item()
        
        val_loss /= len(val_loader)
        scheduler.step(val_loss)
        
        # Early stopping check
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_model_state = model.state_dict().copy()
            patience_counter = 0
        else:
            patience_counter += 1
        
        # Log ogni 10 epochs
        if (epoch + 1) % 10 == 0 or patience_counter == 0:
            current_lr = optimizer.param_groups[0]['lr']
            status = "⭐" if patience_counter == 0 else ""
            print(f"  Epoch {epoch+1:3d}: train_loss={train_loss:.4f}, val_loss={val_loss:.4f}, lr={current_lr:.6f} {status}")
        
        if patience_counter >= PATIENCE:
            print(f"\n⏹️ Early stopping at epoch {epoch+1}")
            break
    
    # Ripristina best model
    if best_model_state:
        model.load_state_dict(best_model_state)
    
    return model


def evaluate_model(model, X_test, y_test, device):
    """Valuta il modello sul test set"""
    model.eval()
    
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X_test).to(device)
        outputs = model(X_tensor)
        predictions = (outputs.cpu().numpy() > 0.5).astype(int).flatten()
    
    y_test_arr = np.array(y_test)
    
    print("\n" + "=" * 60)
    print("📊 RISULTATI SUL TEST SET")
    print("=" * 60)
    
    print(f"\nAccuracy: {accuracy_score(y_test_arr, predictions)*100:.2f}%")
    print(f"F1 Score: {f1_score(y_test_arr, predictions)*100:.2f}%")
    
    print("\nClassification Report:")
    print(classification_report(y_test_arr, predictions, 
                                target_names=['Legit', 'Vandal']))
    
    print("\nConfusion Matrix:")
    cm = confusion_matrix(y_test_arr, predictions)
    print(f"  {'':>10} Pred_Legit  Pred_Vandal")
    print(f"  {'True_Legit':>10}     {cm[0,0]:4d}        {cm[0,1]:4d}")
    print(f"  {'True_Vandal':>10}     {cm[1,0]:4d}        {cm[1,1]:4d}")
    
    return predictions


def main():
    print("=" * 60)
    print("🧠 NEURAL CLASSIFIER TRAINING (PyTorch)")
    print("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n💻 Device: {device}")
    
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        print("❌ Impossibile connettersi a Neo4j")
        return
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"✅ Embedder caricato: {MODEL_NAME}")
    
    legit_edits, vandal_edits = load_data()
    if legit_edits is None:
        driver.close()
        return
    
    # Estrai feature
    print("\n🔄 Estrazione feature (raw embeddings)...")
    X_legit, y_legit = extract_features(legit_edits, embedder, driver, 0)
    X_vandal, y_vandal = extract_features(vandal_edits, embedder, driver, 1)
    
    X = np.array(X_legit + X_vandal)
    y = np.array(y_legit + y_vandal)
    
    print(f"   Feature shape: {X.shape}")
    print(f"   Labels: {len(y)} (Legit: {sum(y==0)}, Vandal: {sum(y==1)})")
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=(1-TRAIN_SPLIT), random_state=42, stratify=y
    )
    
    print(f"\n📊 Split: Train={len(X_train)}, Test={len(X_test)}")
    
    # Normalizzazione
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Training
    input_dim = X_train_scaled.shape[1]
    model = train_model(X_train_scaled, y_train, X_test_scaled, y_test, input_dim, device)
    
    # Valutazione
    evaluate_model(model, X_test_scaled, y_test, device)
    
    # Salva modello e scaler
    if not TRAINED_BC_DIR.exists():
        TRAINED_BC_DIR.mkdir(parents=True, exist_ok=True)
    
    torch.save(model.state_dict(), MODEL_FILE)
    with open(SCALER_FILE, 'wb') as f:
        pickle.dump(scaler, f)
    
    print(f"\n💾 Modello salvato: {MODEL_FILE}")
    print(f"💾 Scaler salvato: {SCALER_FILE}")
    
    driver.close()
    print("\n✅ Training completato!")


if __name__ == "__main__":
    main()
