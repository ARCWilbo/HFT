import torch
import torch.nn as nn
import torch.optim as optim
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
from itertools import product
from torch.utils.data import TensorDataset, DataLoader
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy.stats import norm


class MLP(nn.Module):
    def __init__(self, input_dim, hidden_layers, dropout):
        super().__init__()
        layers = []
        in_dim = input_dim
        for h in hidden_layers:
            layers.extend([nn.Linear(in_dim, h), nn.ReLU(), nn.Dropout(dropout)])
            in_dim = h
        layers.append(nn.Linear(in_dim, 1))
        self.model = nn.Sequential(*layers)
        self._init_weights()

    def _init_weights(self):
        for layer in self.model:
            if isinstance(layer, nn.Linear):
                nn.init.kaiming_uniform_(layer.weight, nonlinearity='relu')
                nn.init.zeros_(layer.bias)

    def forward(self, x):
        return self.model(x)


def _bs_price_vec(S, K, T, r, sigma, is_call):
    T     = np.maximum(T,     1e-9)
    sigma = np.maximum(sigma, 1e-9)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    call = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    put  = K * np.exp(-r * T) * norm.cdf(-d2)  - S * norm.cdf(-d1)
    return np.where(is_call, call, put).astype(np.float32)


def time_series_cv_splits(X, y, k):
    n = len(X)
    fold_size = n // k
    splits = []
    for i in range(k):
        val_start = i * fold_size
        val_end = (i + 1) * fold_size if i < k - 1 else n
        X_val = X[val_start:val_end]
        y_val = y[val_start:val_end]
        X_train = np.concatenate([X[:val_start], X[val_end:]], axis=0)
        y_train = np.concatenate([y[:val_start], y[val_end:]], axis=0)
        splits.append((X_train, y_train, X_val, y_val))
    return splits


def train_one(model, train_loader, val_loader, epochs, lr, device, verbose=False):
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    train_losses, val_losses = [], []
    best_val_loss = float('inf')
    best_state = None

    for epoch in range(epochs):
        model.train()
        batch_losses = []
        for X, y in train_loader:
            X, y = X.to(device), y.to(device)
            optimizer.zero_grad()
            loss = criterion(model(X), y)
            loss.backward()
            optimizer.step()
            batch_losses.append(loss.item())

        train_loss = np.mean(batch_losses)
        train_losses.append(train_loss)

        model.eval()
        running_val = 0.0
        with torch.no_grad():
            for X, y in val_loader:
                X, y = X.to(device), y.to(device)
                running_val += criterion(model(X), y).item()
        val_loss = running_val / len(val_loader)
        val_losses.append(val_loss)

        if verbose:
            print(f"[Epoch {epoch+1:03d}] Train: {train_loss:.6f}  Val: {val_loss:.6f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = {k: v.clone() for k, v in model.state_dict().items()}

    model.load_state_dict(best_state)
    return train_losses, val_losses, best_val_loss


def grid_search(X, y, config, device):
    k = config['data']['k_folds']
    splits = time_series_cv_splits(X, y, k)

    best_score = float('inf')
    best_params = None

    param_grid = product(
        config['model']['hidden_layers'],
        config['model']['dropout'],
        config['training']['lr'],
        config['training']['batch_size']
    )

    for hidden_layers, dropout, lr, batch_size in param_grid:
        scores = []

        for X_train, y_train, X_val, y_val in splits:
            train_loader = DataLoader(
                TensorDataset(
                    torch.tensor(X_train, dtype=torch.float32),
                    torch.tensor(y_train, dtype=torch.float32)
                ),
                batch_size=batch_size, shuffle=False
            )
            val_loader = DataLoader(
                TensorDataset(
                    torch.tensor(X_val, dtype=torch.float32),
                    torch.tensor(y_val, dtype=torch.float32)
                ),
                batch_size=batch_size, shuffle=False
            )

            model = MLP(
                input_dim=X.shape[1],
                hidden_layers=hidden_layers,
                dropout=dropout
            ).to(device)

            _, _, val_loss = train_one(
                model, train_loader, val_loader,
                config['training']['epochs'], lr, device,
                verbose=True
            )
            scores.append(val_loss)

        avg_score = np.mean(scores)
        print(f"Params: hidden={hidden_layers}, dropout={dropout}, lr={lr}, batch={batch_size} → {avg_score:.6f}")

        if avg_score < best_score:
            best_score = avg_score
            best_params = (hidden_layers, dropout, lr, batch_size)

    return best_params, best_score


def final_train_and_evaluate(X_trainval, y_trainval, X_test, y_test, best_params, config, device, df_test_raw):
    if isinstance(best_params, dict):
        hidden_layers = best_params["hidden_layers"]
        dropout       = best_params["dropout"]
        lr            = best_params.get("lr", best_params.get("learning_rate"))
        batch_size    = best_params["batch_size"]
    else:
        hidden_layers, dropout, lr, batch_size = best_params
    epochs = 25

    split = int(len(X_trainval) * 0.8)
    X_tr, y_tr = X_trainval[:split], y_trainval[:split]
    X_val, y_val = X_trainval[split:], y_trainval[split:]

    train_loader = DataLoader(
        TensorDataset(
            torch.tensor(X_tr, dtype=torch.float32),
            torch.tensor(y_tr, dtype=torch.float32)
        ),
        batch_size=batch_size, shuffle=False
    )
    val_loader = DataLoader(
        TensorDataset(
            torch.tensor(X_val, dtype=torch.float32),
            torch.tensor(y_val, dtype=torch.float32)
        ),
        batch_size=batch_size, shuffle=False
    )

    model = MLP(
        input_dim=X_trainval.shape[1],
        hidden_layers=hidden_layers,
        dropout=dropout
    ).to(device)

    print("\n--- Final Training (best params, verbose) ---")
    train_losses, val_losses, best_val = train_one(
        model, train_loader, val_loader, epochs, lr, device, verbose=True
    )
    # print(f"\nBest val loss achieved: {best_val:.6f}")

    loss_df = pd.DataFrame({
        'epoch':      range(1, len(train_losses) + 1),
        'train_loss': train_losses,
        'val_loss':   val_losses
    })
    print("\nEpoch Loss Summary:")
    print(loss_df.to_string(index=False))

    plt.figure(figsize=(10, 5))
    plt.plot(loss_df['epoch'], loss_df['train_loss'], label='Train Loss')
    plt.plot(loss_df['epoch'], loss_df['val_loss'],   label='Val Loss')
    plt.xlabel('Epoch')
    plt.ylabel('MSE Loss')
    plt.title('MLP Training vs Validation Loss')
    plt.legend()
    plt.tight_layout()
    plt.savefig('../Graphs/mlp_loss_curve.png')
    plt.show()

    # model weights already restored to best val epoch — evaluate on test
    test_loader = DataLoader(
        TensorDataset(
            torch.tensor(X_test, dtype=torch.float32),
            torch.tensor(y_test, dtype=torch.float32)
        ),
        batch_size=batch_size, shuffle=False
    )

    model.eval()
    preds, actuals = [], []
    with torch.no_grad():
        for X, y in test_loader:
            X, y = X.to(device), y.to(device)
            preds.extend(model(X).cpu().numpy().flatten())
            actuals.extend(y.cpu().numpy().flatten())

    preds   = np.array(preds)
    actuals = np.array(actuals)

    mse  = np.mean((preds - actuals) ** 2)
    rmse = np.sqrt(mse)
    mae  = np.mean(np.abs(preds - actuals))
    ss_res = np.sum((actuals - preds) ** 2)
    ss_tot = np.sum((actuals - actuals.mean()) ** 2)
    r2   = 1 - ss_res / ss_tot if ss_tot > 0 else float('nan')

    print("\n--- Test Metrics (Predicted IV) ---")
    metrics = pd.DataFrame({
        'metric': ['MSE', 'RMSE', 'MAE', 'R²'],
        'value':  [mse, rmse, mae, r2]
    })
    print(metrics.to_string(index=False))

    # ── BS price from predicted IV → residual histogram ───────────────────────
    R = 0.0426
    df_te = df_test_raw.reset_index(drop=True)

    S            = df_te["STK_mid_price"].values.astype(np.float32)
    K            = (S + df_te["strike_STK_residual"].values).astype(np.float32)
    T            = (df_te["min_to_exp"].values / (390 * 252)).astype(np.float32)
    is_call      = df_te["isCall"].values.astype(bool)
    opt_actual   = df_te["Target"].values.astype(np.float32)

    bs_price_pred = _bs_price_vec(S, K, T, R, preds, is_call)
    residuals     = opt_actual - bs_price_pred

    print(f"\nResidual stats:  mean={residuals.mean():.4f}  std={residuals.std():.4f}"
          f"  min={residuals.min():.4f}  max={residuals.max():.4f}")

    mu, std = norm.fit(residuals)
    clip_lo, clip_hi = np.percentile(residuals, 0.5), np.percentile(residuals, 99.5)
    x_fit = np.linspace(clip_lo, clip_hi, 400)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(residuals.clip(clip_lo, clip_hi), bins=100, density=True,
            alpha=0.55, color="steelblue", label="Residuals")
    ax.plot(x_fit, norm.pdf(x_fit, mu, std), "r-", lw=2,
            label=f"Gaussian fit  μ={mu:.4f}  σ={std:.4f}")
    ax.set_xlabel("OPT_Price  −  BS(Predicted IV)")
    ax.set_ylabel("Density")
    ax.set_title("Residuals: Actual OPT Price vs BS Price Using MLP-Predicted IV")
    ax.legend()
    plt.tight_layout()
    plt.savefig("../Graphs/residual_histogram.png", dpi=150)
    plt.show()


if __name__ == '__main__':

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Device: {device}")

    with open("../Source/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    df = pd.read_csv("../Data/0dteX.csv")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    df["event_ts_ns"] = df["event_timestamp"].astype("int64")
    
    X_raw = df.drop(columns=["iv", "event_timestamp"]).values.astype("float32")
    y     = df["iv"].values.astype("float32").reshape(-1, 1)

    heatmap_df = df.drop(columns=["event_timestamp", "event_ts_ns"]).rename(columns={"Target": "OPT_Price"})
    cols = [c for c in heatmap_df.columns if c != "iv"] + ["iv"]
    heatmap_df = heatmap_df[cols]

    plt.figure(figsize=(12, 10))
    sns.heatmap(heatmap_df.corr(), annot=True, fmt=".2f", cmap='coolwarm')
    plt.title("Feature Correlation Matrix  (target = iv)")
    plt.tight_layout()
    plt.savefig("../Graphs/feature_correlation_matrix.png")
    # plt.show()

    scaler = StandardScaler()
    X = scaler.fit_transform(X_raw)

    # hold out last 20% as test set (time-ordered — no shuffle)
    split_idx = int(len(X) * 0.8)
    X_trainval, y_trainval = X[:split_idx], y[:split_idx]
    X_test,     y_test     = X[split_idx:], y[split_idx:]

    best_params, best_score = grid_search(X_trainval, y_trainval, config, device)

    print("\nBest Params:", best_params)
    print("Best CV Loss:", f"{best_score:.6f}")

    # best_params = {
    #     "hidden_layers": [256, 128, 64],
    #     "dropout": 0.0,
    #     "learning_rate": 0.001,
    #     "batch_size": 128
    # }

    df_test_raw = df.iloc[split_idx:].reset_index(drop=True)
    final_train_and_evaluate(X_trainval, y_trainval, X_test, y_test, best_params, config, device, df_test_raw)
