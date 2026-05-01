ENV_NAME  = Research
CONDA     = conda
RUN       = $(CONDA) run --no-capture-output -n $(ENV_NAME)
SRC_DIR   = Source
GRAPH_DIR = Graphs

.PHONY: env install pull_data feature_engineer graphs run_model clean remove

# ── Create env + install all packages ─────────────────────────────────────────
# Strip conda-specific "@ file://..." URLs so pip can parse the file cleanly.
env:
	$(CONDA) create -n $(ENV_NAME) python=3.11 -y
	$(MAKE) install

install:
	sed 's| @ file://[^ ]*||g' txt/requirements.txt \
	    | grep -v '^#' | grep -v '^$$' > /tmp/hft_reqs.txt
	$(RUN) pip install -r /tmp/hft_reqs.txt
	@echo "\nDone — '$(ENV_NAME)' is ready."

# ── Pull live data from IBKR TWS ──────────────────────────────────────────────
pull_data:
	cd "$(SRC_DIR)" && $(RUN) python -u IBKR_Pulling_Data.py

# ── Clean raw DB data → 0dteX.csv feature matrix ─────────────────────────────
feature_engineer:
	cd "$(SRC_DIR)" && $(RUN) python -u Cleaning_and_Feature_Engineering.py

# ── Exploratory visualisations ────────────────────────────────────────────────
graphs:
	cd "$(GRAPH_DIR)" && $(RUN) python -u Graph_visualizations.py

# ── Train and evaluate MLP ────────────────────────────────────────────────────
run_model:
	cd "$(SRC_DIR)" && $(RUN) python -u MLP.py

# ── Delete compiled artefacts ─────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	@echo "Cleaned."

# ── Delete the conda environment entirely ─────────────────────────────────────
remove:
	$(CONDA) env remove -n $(ENV_NAME) -y
	@echo "Environment '$(ENV_NAME)' removed."
