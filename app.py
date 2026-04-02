"""
Reportly — Flask backend
Serves the frontend and handles PDF analysis via the FMCG decoder.
"""

import os
import uuid
import shutil
import threading
import time
from flask import Flask, request, jsonify, send_from_directory, render_template

# ── Setup ──
app = Flask(__name__, static_folder="static", template_folder="templates")

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Max upload size: 50 MB
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# In-memory job tracking (for production, use Redis or a DB)
jobs = {}


# ═══════════════════════════════════════════════════════════════════
# ROUTES — Frontend
# ═══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Serve the main Reportly page."""
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════════
# ROUTES — API
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/analyse", methods=["POST"])
def analyse():
    """
    Accept a PDF upload, start analysis in background, return a job ID.
    The frontend polls /api/status/<job_id> until complete.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "" or not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Please upload a PDF file"}), 400

    label = request.form.get("label", "").strip()
    if not label:
        label = os.path.splitext(file.filename)[0]

    # Save uploaded file with unique ID
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}.pdf")
    file.save(pdf_path)

    # Track job
    jobs[job_id] = {
        "status": "processing",
        "step": "Uploading...",
        "progress": 5,
        "label": label,
        "pdf_path": pdf_path,
        "output_dir": job_dir,
        "result": None,
        "error": None,
    }

    # Run analysis in background thread
    thread = threading.Thread(target=_run_analysis, args=(job_id,))
    thread.daemon = True
    thread.start()

    return jsonify({"job_id": job_id}), 202


@app.route("/api/status/<job_id>")
def job_status(job_id):
    """Poll this to check analysis progress."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    response = {
        "status": job["status"],
        "step": job["step"],
        "progress": job["progress"],
        "label": job["label"],
    }

    if job["status"] == "complete":
        response["pdf_url"] = f"/api/download/{job_id}/pdf"
        response["excel_url"] = f"/api/download/{job_id}/excel"
        # Include key metrics for the results screen
        rd = job.get("result") or {}
        response["narrative"] = rd.get("narrative", "")
        response["pmi"] = rd.get("pmi_score", "")
        response["sentiment"] = rd.get("avg_sentiment", "")

    if job["status"] == "error":
        response["error"] = job["error"]

    return jsonify(response)


@app.route("/api/download/<job_id>/<file_type>")
def download(job_id, file_type):
    """Serve the generated PDF or Excel file."""
    job = jobs.get(job_id)
    if not job or job["status"] != "complete":
        return jsonify({"error": "File not ready"}), 404

    job_dir = job["output_dir"]
    result = job.get("result", {})

    if file_type == "pdf":
        filename = result.get("pdf_filename", "dashboard.pdf")
    elif file_type == "excel":
        filename = result.get("excel_filename", "analysis.xlsx")
    else:
        return jsonify({"error": "Invalid file type"}), 400

    return send_from_directory(
        job_dir, filename, as_attachment=True
    )


# ═══════════════════════════════════════════════════════════════════
# BACKGROUND ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def _update_job(job_id, step, progress):
    """Update job status for frontend polling."""
    if job_id in jobs:
        jobs[job_id]["step"] = step
        jobs[job_id]["progress"] = progress


def _run_analysis(job_id):
    """Run the decoder in a background thread."""
    job = jobs[job_id]
    pdf_path = job["pdf_path"]
    label = job["label"]
    output_dir = job["output_dir"]

    try:
        _update_job(job_id, "Extracting sections from PDF...", 10)

        # Import the decoder here (heavy imports stay out of the main thread)
        from decoder import process_report

        # Monkey-patch the print function to capture progress
        import builtins
        original_print = builtins.print

        def progress_print(*args, **kwargs):
            msg = " ".join(str(a) for a in args)
            original_print(*args, **kwargs)
            # Update job step based on decoder output
            if "Extracting" in msg or "✓" in msg:
                _update_job(job_id, "Extracting sections from PDF...", 20)
            elif "guidance" in msg.lower():
                _update_job(job_id, "Extracting management guidance...", 55)
            elif "PMI" in msg or "pmi" in msg.lower():
                _update_job(job_id, "Calculating text-derived PMI...", 65)
            elif "logo" in msg.lower():
                _update_job(job_id, "Extracting company logo...", 75)
            elif "Building PDF" in msg or "PDF dashboard" in msg:
                _update_job(job_id, "Building PDF dashboard...", 85)
            elif "Excel" in msg:
                _update_job(job_id, "Generating Excel workbook...", 92)
            elif "DONE" in msg:
                _update_job(job_id, "Finalising...", 98)

        builtins.print = progress_print

        # Run the decoder
        result = process_report(pdf_path, output_dir, label)

        builtins.print = original_print

        if result is None:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = (
                "Could not find any sections in this PDF. "
                "The tool works best with FMCG/manufacturing annual reports "
                "that have a standard table of contents."
            )
            return

        # Extract filenames from paths
        pdf_out = os.path.basename(result.get("pdf", "dashboard.pdf"))
        excel_out = os.path.basename(result.get("excel", "analysis.xlsx"))

        report_data = result.get("report", {})
        jobs[job_id]["result"] = {
            "pdf_filename": pdf_out,
            "excel_filename": excel_out,
            "narrative": report_data.get("narrative", ""),
            "pmi_score": report_data.get("pmi", {}).get("pmi_score", ""),
            "avg_sentiment": round(report_data.get("avg_sentiment", 0), 3),
        }
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["step"] = "Done"
        jobs[job_id]["progress"] = 100

    except Exception as e:
        import traceback
        traceback.print_exc()
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = f"Analysis failed: {str(e)}"

    finally:
        # Clean up uploaded file (keep outputs)
        try:
            os.remove(pdf_path)
        except OSError:
            pass


# ═══════════════════════════════════════════════════════════════════
# CLEANUP — remove old jobs periodically (simple TTL)
# ═══════════════════════════════════════════════════════════════════

def _cleanup_old_jobs():
    """Run every 30 minutes. Remove jobs older than 2 hours."""
    while True:
        time.sleep(1800)
        cutoff = time.time() - 7200  # 2 hours
        to_remove = []
        for jid, job in jobs.items():
            job_dir = job.get("output_dir", "")
            if os.path.exists(job_dir):
                dir_age = os.path.getmtime(job_dir)
                if dir_age < cutoff:
                    to_remove.append(jid)

        for jid in to_remove:
            job = jobs.pop(jid, None)
            if job:
                try:
                    shutil.rmtree(job["output_dir"], ignore_errors=True)
                except Exception:
                    pass


cleanup_thread = threading.Thread(target=_cleanup_old_jobs)
cleanup_thread.daemon = True
cleanup_thread.start()


# ═══════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
