"""
16_train_neural_only_new.py
Training di un classificatore neurale PyTorch per rilevamento vandalismo.
VERSIONE SOLO NEW TEXT: Usa solo embedding del nuovo testo + length_ratio.
Features: new_emb + length_ratio = 385
"""

import json
import pickle
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, f1_score, accuracy_score
from sentence_transformers import SentenceTransformer
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"

MODEL_FILE = TRAINED_BC_DIR / "neural_classifier_only_new.pth"
SCALER_FILE = TRAINED_BC_DIR / "neural_scaler_only_new.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

from config_loader import load_config
CONFIG = load_config()
MODEL_NAME = CONFIG['embedding']['model_name']


TRAIN_SPLIT = CONFIG['dataset']['training'].get('train_split', 0.8)
EPOCHS = 100
BATCH_SIZE = 8
LEARNING_RATE = 0.001
WEIGHT_DECAY = 0.01 
PATIENCE = 15  


class VandalismClassifierOnlyNew(nn.Module):
    """
    Rete neurale per classificazione vandalismo
    VERSIONE SOLO NEW TEXT: input_dim = 384 + 1 = 385 features
    """
    def __init__(self, input_dim):
        super(VandalismClassifierOnlyNew, self).__init__()
        # Rete più piccola per meno features
        self.fc1 = nn.Linear(input_dim, 128)
        self.bn1 = nn.BatchNorm1d(128)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(128, 64)
        self.bn2 = nn.BatchNorm1d(64)
        self.dropout2 = nn.Dropout(0.4)
        self.fc3 = nn.Linear(64, 1)
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
        x = self.sigmoid(x)
        return x


def get_raw_features_only_new(edit, embedder):
    """
    Estrae feature grezze SOLO NEW TEXT.
    
    Features:
    - new_emb (384)
    - length_ratio (1)
    
    Totale: 385 features
    """
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    
    # Embedding del nuovo testo
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
    
    # Feature aggiuntive
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    
    # SOLO NEW TEXT + RATIO!
    # Concatena: 384 + 1 = 385 features
    features = np.concatenate([
        new_emb,
        [length_ratio]
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


def extract_features(edits, embedder, label):
    """Estrae feature per una lista di edit"""
    X = []
    y = []
    
    for edit in edits:
        feat = get_raw_features_only_new(edit, embedder)
        X.append(feat)
        y.append(label)
    
    return X, y


def train_model(X_train, y_train, X_val, y_val, input_dim, device):
    """Training del modello PyTorch con early stopping"""
    
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
    
    model = VandalismClassifierOnlyNew(input_dim).to(device)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', patience=5, factor=0.5)
    
    best_val_loss = float('inf')
    patience_counter = 0
    best_model_state = None
    
    print(f"\n🏋️ Training per {EPOCHS} epochs (early stopping patience={PATIENCE})...")
    print("-" * 60)
    
    for epoch in range(EPOCHS):
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
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_model_state = model.state_dict().copy()
            patience_counter = 0
        else:
            patience_counter += 1
        
        if (epoch + 1) % 10 == 0 or patience_counter == 0:
            current_lr = optimizer.param_groups[0]['lr']
            status = "⭐" if patience_counter == 0 else ""
            print(f"  Epoch {epoch+1:3d}: train_loss={train_loss:.4f}, val_loss={val_loss:.4f}, lr={current_lr:.6f} {status}")
        
        if patience_counter >= PATIENCE:
            print(f"\n⏹️ Early stopping at epoch {epoch+1}")
            break
    
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
    print("📊 RISULTATI SUL TEST SET (ONLY NEW)")
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
    print("🧠 NEURAL CLASSIFIER TRAINING (PyTorch) - ONLY NEW TEXT")
    print("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n💻 Device: {device}")
    
    print("ℹ️  Modo ONLY NEW: Usa solo embedding del nuovo testo")
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"✅ Embedder caricato: {MODEL_NAME}")
    
    legit_edits, vandal_edits = load_data()
    if legit_edits is None:
        return

    target_legit = CONFIG['dataset']['training']['legit_count']
    target_vandal = CONFIG['dataset']['training']['vandal_count']
    
    if len(legit_edits) > target_legit:
        print(f"✂️  Limito Legit a {target_legit} (da {len(legit_edits)})")
        legit_edits = legit_edits[:target_legit]
        
    if len(vandal_edits) > target_vandal:
        print(f"✂️  Limito Vandal a {target_vandal} (da {len(vandal_edits)})")
        vandal_edits = vandal_edits[:target_vandal]
    
    print("\n🔄 Estrazione feature (ONLY NEW TEXT)...")
    X_legit, y_legit = extract_features(legit_edits, embedder, 0)
    X_vandal, y_vandal = extract_features(vandal_edits, embedder, 1)
    
    X = np.array(X_legit + X_vandal)
    y = np.array(y_legit + y_vandal)
    
    print(f"   Feature shape: {X.shape}")
    print(f"   Labels: {len(y)} (Legit: {sum(y==0)}, Vandal: {sum(y==1)})")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=(1-TRAIN_SPLIT), random_state=42, stratify=y
    )
    
    print(f"\n📊 Split: Train={len(X_train)}, Test={len(X_test)}")
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    input_dim = X_train_scaled.shape[1]
    model = train_model(X_train_scaled, y_train, X_test_scaled, y_test, input_dim, device)
    
    evaluate_model(model, X_test_scaled, y_test, device)
    
    if not TRAINED_BC_DIR.exists():
        TRAINED_BC_DIR.mkdir(parents=True, exist_ok=True)
    
    torch.save(model.state_dict(), MODEL_FILE)
    with open(SCALER_FILE, 'wb') as f:
        pickle.dump(scaler, f)
    
    print(f"\n💾 Modello salvato: {MODEL_FILE}")
    print(f"💾 Scaler salvato: {SCALER_FILE}")
    
    print("\n✅ Training completato (ONLY NEW TEXT)!")


if __name__ == "__main__":
    main()
