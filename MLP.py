import torch
import torch.nn as nn
import torch.optim as optim
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from torch.utils.data import TensorDataset, DataLoader
import pandas as pd 
import numpy as np
from Data_Analysis import Data_Analysis, Security

# ----------------------------
# Simple MLP
# ----------------------------
class MLP(nn.Module):
    def __init__(self, input_dim):

        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, x):
        return self.model(x)
    
    def print_last_test_prediction(self, X_test, y_test, device):
        self.eval()

        with torch.no_grad():
            # Get last sample
            X_last = X_test[-1].unsqueeze(0).float().to(device)  # shape (1, d)
            y_last = y_test[-1].item()

            pred = self(X_last).item()

            residual = y_last - pred

            print("\n--- Last Test Observation ---")
            print(f"Prediction : {pred:.6f}")
            print(f"Target     : {y_last:.6f}")
            print(f"Residual   : {residual:.6f}")

# ----------------------------
# Training function
# ----------------------------
def train_model(model, train_loader, val_loader, test_loader, epochs=20):

    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=1e-3)

    train_losses = []
    val_losses = []
    test_losses = []

    for epoch in range(epochs):

        # ---- TRAIN ----
        model.train()
        train_loss = 0

        for X, y in train_loader:

            X = X.to(device)
            y = y.to(device)

            optimizer.zero_grad()

            preds = model(X)
            loss = criterion(preds, y)

            loss.backward()
            optimizer.step()

            train_loss += loss.item()

        train_loss /= len(train_loader)
        train_losses.append(train_loss)

        # ---- VALIDATION ----
        model.eval()
        val_loss = 0

        with torch.no_grad():
            for X, y in val_loader:

                X = X.to(device)
                y = y.to(device)

                preds = model(X)
                loss = criterion(preds, y)
                val_loss += loss.item()

        val_loss /= len(val_loader)
        val_losses.append(val_loss)

        # ---- TEST ----
        test_loss = 0

        with torch.no_grad():
            for X, y in test_loader:

                X = X.to(device)
                y = y.to(device)

                preds = model(X)
                loss = criterion(preds, y)
                test_loss += loss.item()

        test_loss /= len(test_loader)
        test_losses.append(test_loss)

        print(f"Epoch {epoch+1}: Train={train_loss:.4f}, Val={val_loss:.4f}, Test={test_loss:.4f}")

    return train_losses, val_losses, test_losses


# ----------------------------
# Plot function
# ----------------------------
def plot_losses(train_losses, val_losses, test_losses):

    plt.figure(figsize=(10,6))

    plt.plot(train_losses, label="Train Loss")
    plt.plot(val_losses, label="Validation Loss")
    plt.plot(test_losses, label="Test Loss")

    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title("Training vs Validation vs Test Loss")
    plt.legend()

    plt.show()

if __name__ == '__main__': 

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    device = "cpu"          
    print(device)
    
    df = pd.read_csv("/Users/arcwilbo/Desktop/HFT Research/Will_Branch/X.csv", index_col=0)

    target_col = "Target"

    X = df.drop(columns=[target_col])
    y = df[target_col]

    X = X.values.astype('float32')
    y = y.values.astype('float32').reshape(-1, 1)

    n = len(X)

    train_end = int(0.7 * n)
    val_end = int(0.85 * n)

    X_train, y_train = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_val = scaler.transform(X_val)
    X_test = scaler.transform(X_test)

    X_train = torch.tensor(X_train)
    y_train = torch.tensor(y_train)

    X_val = torch.tensor(X_val)
    y_val = torch.tensor(y_val)

    X_test = torch.tensor(X_test)
    y_test = torch.tensor(y_test)

    train_loader = DataLoader(TensorDataset(X_train, y_train), batch_size=256, shuffle=True)
    val_loader = DataLoader(TensorDataset(X_val, y_val), batch_size=256, shuffle=False)
    test_loader = DataLoader(TensorDataset(X_test, y_test), batch_size=256, shuffle=False)

    # Train 

    model = MLP(input_dim=X_train.shape[1]).to(device)

    train_losses, val_losses, test_losses = train_model(
        model,
        train_loader,
        val_loader,
        test_loader,
        epochs=30
    )


    model.print_last_test_prediction(X_test, y_test, device)

    plot_losses(train_losses, val_losses, test_losses)