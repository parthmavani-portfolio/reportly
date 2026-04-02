"""
Run this before starting the app to download required NLTK data.
On Render/Railway, add this as a build command:
    pip install -r requirements.txt && python setup_nltk.py
"""
import os
import nltk

# Set a writable path for NLTK data
nltk_data_dir = os.path.join(os.path.dirname(__file__), "nltk_data")
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.data.path.insert(0, nltk_data_dir)

print("Downloading NLTK stopwords...")
nltk.download("stopwords", download_dir=nltk_data_dir, quiet=False)
print("Done.")
