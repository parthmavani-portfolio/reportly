# Reportly

An NLP-powered annual report analyser for FMCG and manufacturing companies. Upload a PDF, get a professional dashboard.

## What it does

- **6-Theme Analysis** — Volume, Raw Material, Brand, Margin, Growth, Risk
- **VADER Sentiment** — Per-section sentiment scoring
- **TF-IDF Keywords** — Distinctive keyword extraction
- **Management Guidance** — Forward-looking statement extraction
- **Text-Derived PMI** — Purchase Manager Index from report language
- **Narrative Classification** — Aggressive Growth to Defensive Stress

## Project structure

```
reportly/
├── app.py              ← Flask backend (serves HTML + API)
├── decoder.py          ← FMCG analysis pipeline
├── templates/
│   └── index.html      ← Reportly frontend
├── requirements.txt    ← Python dependencies
├── Procfile            ← Gunicorn start command
├── render.yaml         ← Render blueprint (one-click deploy)
├── setup_nltk.py       ← Downloads NLTK data on build
└── .gitignore
```

## Run locally

```bash
# 1. Clone and enter the directory
git clone https://github.com/YOUR_USERNAME/reportly.git
cd reportly

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate        # macOS/Linux
# venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
python setup_nltk.py

# 4. Install poppler (needed for logo extraction)
# macOS:   brew install poppler
# Ubuntu:  sudo apt-get install poppler-utils
# Windows: download from https://github.com/ospalh/poppler-windows

# 5. Run the app
python app.py
```

Open http://localhost:5000 in your browser.

## Deploy to Render (recommended)

1. Push this repo to GitHub
2. Go to https://render.com → New → Web Service
3. Connect your GitHub repo
4. Render auto-detects `render.yaml` and configures everything
5. Set build command: `pip install -r requirements.txt && python setup_nltk.py`
6. Set start command: `gunicorn app:app --bind 0.0.0.0:$PORT --timeout 300 --workers 2`
7. Deploy

**Important**: The analysis takes 30-90 seconds per report. Set the gunicorn timeout to at least 300 seconds.

## Deploy to Railway

1. Push to GitHub
2. Go to https://railway.app → New Project → Deploy from GitHub
3. Railway auto-detects the `Procfile`
4. Add build command in settings: `pip install -r requirements.txt && python setup_nltk.py`
5. Deploy

## API reference

### POST /api/analyse
Upload a PDF and start analysis.

```
Content-Type: multipart/form-data
Body: file (PDF), label (string)
Response: { "job_id": "abc12345" }
```

### GET /api/status/{job_id}
Poll for analysis progress.

```json
{
  "status": "processing",
  "step": "Calculating text-derived PMI...",
  "progress": 65
}
```

When complete:
```json
{
  "status": "complete",
  "pdf_url": "/api/download/abc12345/pdf",
  "excel_url": "/api/download/abc12345/excel",
  "narrative": "Aggressive Growth Narrative",
  "pmi": 77.5,
  "sentiment": 0.778
}
```

### GET /api/download/{job_id}/{pdf|excel}
Download the generated file.

## Tech stack

- **Backend**: Flask + Gunicorn
- **NLP**: VADER, TF-IDF (scikit-learn), pdfplumber
- **Charts**: matplotlib, wordcloud
- **PDF Output**: reportlab
- **Frontend**: Vanilla HTML/CSS/JS, DM Sans + DM Mono
