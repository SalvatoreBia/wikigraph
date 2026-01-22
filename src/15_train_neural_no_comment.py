import json
import pickle
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sentence_transformers import SentenceTransformer
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, f1_score)
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"

MODEL_FILE = TRAINED_BC_DIR / "neural_classifier_no_comment.pth"
SCALER_FILE = TRAINED_BC_DIR / "neural_scaler_no_comment.pkl"

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

class VandalismClassifierNoComment(nn.Module):
    def __init__(self, input_dim):
        super(VandalismClassifierNoComment, self).__init__()
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

def get_raw_features_no_comment(edit, embedder):
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
    
    if original_text:
        old_emb = embedder.encode(original_text, convert_to_numpy=True)
    else:
        old_emb = np.zeros(384)
    
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    
    if np.all(old_emb == 0) or np.all(new_emb == 0):
        semantic_similarity = 0.0
    else:
        semantic_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    features = np.concatenate([
        old_emb, new_emb,
        [semantic_similarity], [length_ratio]
    ])
    
    return features

def load_data():
    print("- Caricamento dati...")
    
    if not LEGIT_FILE.exists() or not VANDAL_FILE.exists():
        print("! File mock non trovati!")
        return None, None
    
    with open(LEGIT_FILE, 'r', encoding='utf-8') as f:
        legit_edits = json.load(f)
    
    with open(VANDAL_FILE, 'r', encoding='utf-8') as f:
        vandal_edits = json.load(f)
    
    print(f"   - Legit: {len(legit_edits)}, Vandal: {len(vandal_edits)}")
    return legit_edits, vandal_edits

def extract_features(edits, embedder, label):
    X = []
    y = []
    
    for edit in edits:
        feat = get_raw_features_no_comment(edit, embedder)
        X.append(feat)
        y.append(label)
    
    return X, y

def train_model(X_train, y_train, X_val, y_val, input_dim, device):
    train_dataset = TensorDataset(
        torch.FloatTensor(X_train),
        torch.FloatTensor(y_train).unsqueeze(1)
    )
    val_dataset = TensorDataset(
        torch.FloatTensor(X_val),
        torch.FloatTensor(y_val).unsqueeze(1)
    )
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True, drop_last=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    
    model = VandalismClassifierNoComment(input_dim).to(device)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', patience=5, factor=0.5)
    
    best_val_loss = float('inf')
    patience_counter = 0
    best_model_state = None
    
    print(f"\n- Training per {EPOCHS} epochs (patience={PATIENCE})...")
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
            status = "!" if patience_counter == 0 else ""
            print(f"  Epoch {epoch+1:3d}: train_loss={train_loss:.4f}, val_loss={val_loss:.4f}, lr={current_lr:.6f} {status}")
        
        if patience_counter >= PATIENCE:
            print(f"\n- Early stopping alla epoch {epoch+1}")
            break
    
    if best_model_state:
        model.load_state_dict(best_model_state)
    
    return model

def evaluate_model(model, X_test, y_test, device):
    model.eval()
    
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X_test).to(device)
        outputs = model(X_tensor)
        predictions = (outputs.cpu().numpy() > 0.5).astype(int).flatten()
    
    y_test_arr = np.array(y_test)
    
    print("\n" + "=" * 60)
    print("- RISULTATI TEST (NO COMMENT)")
    print("=" * 60)
    
    print(f"\nAccuratezza: {accuracy_score(y_test_arr, predictions)*100:.2f}%")
    print(f"F1 Score: {f1_score(y_test_arr, predictions)*100:.2f}%")
    
    print("\nReport Classificazione:")
    print(classification_report(y_test_arr, predictions, 
                                target_names=['Legit', 'Vandal']))
    
    print("\nMatrice di Confusione:")
    cm = confusion_matrix(y_test_arr, predictions)
    print(f"  {'':>10} Pred_Legit  Pred_Vandal")
    print(f"  {'True_Legit':>10}     {cm[0,0]:4d}        {cm[0,1]:4d}")
    print(f"  {'True_Vandal':>10}     {cm[1,0]:4d}        {cm[1,1]:4d}")
    
    return predictions

def main():
    print("=" * 60)
    print("- TRAINING CLASSIFICATORE NEURALE (PyTorch) - NO COMMENT")
    print("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n- Dispositivo: {device}")
    
    print("- Modo NO COMMENT: Non usa embedding del commento")
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"- Embedder caricato: {MODEL_NAME}")
    
    legit_edits, vandal_edits = load_data()
    if legit_edits is None:
        return

    target_legit = CONFIG['dataset']['training']['legit_count']
    target_vandal = CONFIG['dataset']['training']['vandal_count']
    
    if len(legit_edits) > target_legit:
        print(f"- Limito Legit a {target_legit} (da {len(legit_edits)})")
        legit_edits = legit_edits[:target_legit]
        
    if len(vandal_edits) > target_vandal:
        print(f"- Limito Vandal a {target_vandal} (da {len(vandal_edits)})")
        vandal_edits = vandal_edits[:target_vandal]
    
    print("\n- Estrazione feature (raw embeddings, NO COMMENT)...")
    X_legit, y_legit = extract_features(legit_edits, embedder, 0)
    X_vandal, y_vandal = extract_features(vandal_edits, embedder, 1)
    
    X = np.array(X_legit + X_vandal)
    y = np.array(y_legit + y_vandal)
    
    print(f"   - Feature shape: {X.shape}")
    print(f"   - Labels: {len(y)} (Legit: {sum(y==0)}, Vandal: {sum(y==1)})")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=(1-TRAIN_SPLIT), random_state=42, stratify=y
    )
    
    print(f"\n- Split: Train={len(X_train)}, Test={len(X_test)}")
    
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
    
    print(f"\n- Modello salvato: {MODEL_FILE}")
    print(f"- Scaler salvato: {SCALER_FILE}")
    
    print("\n- Training completato (NO COMMENT)!")

if __name__ == "__main__":
    main()
