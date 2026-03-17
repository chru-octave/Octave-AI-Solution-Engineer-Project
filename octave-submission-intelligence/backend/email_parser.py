import base64
import email
import os
import tempfile
from email import policy

# Lazy import — pdfminer takes ~400ms to load, defer until first actual use
_pdf_extract_text = None


def _get_pdf_extractor():
    global _pdf_extract_text
    if _pdf_extract_text is None:
        from pdfminer.high_level import extract_text

        _pdf_extract_text = extract_text
    return _pdf_extract_text


# PDFs we really want even if scanned
PRIORITY_KEYWORDS = [
    "loss run",
    "loss_run",
    "acord",
    "application",
    "app",
    "vehicle schedule",
    "mvr",
    "driver",
    "supplement",
    "supp",
]

# PDFs we can skip — not useful for underwriting extraction
SKIP_KEYWORDS = [
    "resume",
    "registration",
    "corporation doc",
    "fein doc",
    "corp doc",
    "image",
    "photo",
]

MAX_TOTAL_CHARS = 60_000
MAX_PER_PDF = 8_000
MAX_VISION_IMAGES = 4


def _is_priority(fname):
    fl = fname.lower()
    return any(k in fl for k in PRIORITY_KEYWORDS)


def _should_skip(fname):
    fl = fname.lower()
    return any(k in fl for k in SKIP_KEYWORDS)


def _pdf_to_base64_image(pdf_bytes):
    """Convert first page of PDF to base64 PNG via pdftoppm or ImageMagick."""
    pdf_path = None
    try:
        import subprocess

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            pdf_path = tmp.name

        # Try pdftoppm (poppler)
        result = subprocess.run(
            ["pdftoppm", "-r", "150", "-l", "1", "-png", pdf_path, pdf_path.replace(".pdf", "")],
            capture_output=True,
            timeout=15,
        )
        candidate = pdf_path.replace(".pdf", "-1.png")
        if result.returncode == 0 and os.path.exists(candidate):
            with open(candidate, "rb") as f:
                data = base64.standard_b64encode(f.read()).decode()
            os.unlink(candidate)
            return data

        # Fallback: ImageMagick
        png_path = pdf_path.replace(".pdf", ".png")
        result2 = subprocess.run(
            ["convert", "-density", "150", f"{pdf_path}[0]", png_path], capture_output=True, timeout=15
        )
        if result2.returncode == 0 and os.path.exists(png_path):
            with open(png_path, "rb") as f:
                data = base64.standard_b64encode(f.read()).decode()
            os.unlink(png_path)
            return data

    except Exception:
        pass
    finally:
        if pdf_path and os.path.exists(pdf_path):
            try:
                os.unlink(pdf_path)
            except Exception:
                pass
    return None


def parse_eml(file_path, save_dir=None):
    """
    Parse .eml file. Returns body text + text-extracted PDFs + vision images
    for scanned PDFs that couldn't be text-extracted.
    """
    with open(file_path, "rb") as f:
        msg = email.message_from_bytes(f.read(), policy=policy.default)

    result = {
        "filename": os.path.basename(file_path),
        "subject": str(msg.get("Subject", "")),
        "from": str(msg.get("From", "")),
        "to": str(msg.get("To", "")),
        "date": str(msg.get("Date", "")),
        "body_text": "",
        "attachments": [],
        "vision_attachments": [],
        "saved_files": [],
    }

    pdf_parts = []
    all_pdf_parts = []  # every PDF, for saving to disk
    for part in msg.walk():
        ct = part.get_content_type()
        cd = part.get_content_disposition() or ""
        fname = part.get_filename() or ""

        if ct == "text/plain" and "attachment" not in cd:
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                text = payload.decode(charset, errors="replace")
                if len(text.strip()) > 20:
                    result["body_text"] += text[:3000]

        elif "pdf" in ct.lower() or fname.lower().endswith(".pdf"):
            payload = part.get_payload(decode=True)
            if payload:
                all_pdf_parts.append((fname, payload))
                if not _should_skip(fname):
                    pdf_parts.append((fname, payload, _is_priority(fname)))

    # Save ALL PDFs to disk if save_dir provided
    if save_dir:
        import hashlib
        import shutil

        # Write to a temp dir first — swap atomically on success so a
        # failed mid-parse never leaves the submission with no documents
        tmp_save_dir = save_dir + "._tmp"
        if os.path.exists(tmp_save_dir):
            shutil.rmtree(tmp_save_dir)
        os.makedirs(tmp_save_dir, exist_ok=True)
        seen_hashes = set()  # deduplicate by content hash
        for fname, payload in all_pdf_parts:
            # Skip exact content duplicates (same bytes, different MIME parts)
            content_hash = hashlib.md5(payload).hexdigest()
            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            # Sanitize filename — keep readable characters
            safe_name = "".join(c for c in fname if c.isalnum() or c in " ._-()").strip()
            if not safe_name:
                safe_name = "attachment.pdf"
            # Ensure .pdf extension
            if not safe_name.lower().endswith(".pdf"):
                safe_name += ".pdf"

            dest = os.path.join(tmp_save_dir, safe_name)
            # Handle filename collisions (different content, same name)
            if os.path.exists(dest):
                base, ext = os.path.splitext(safe_name)
                counter = 2
                while os.path.exists(os.path.join(tmp_save_dir, f"{base} ({counter}){ext}")):
                    counter += 1
                dest = os.path.join(tmp_save_dir, f"{base} ({counter}){ext}")
                safe_name = os.path.basename(dest)

            with open(dest, "wb") as f_out:
                f_out.write(payload)
            result["saved_files"].append(
                {
                    "filename": safe_name,
                    "original_filename": fname,
                    "size": len(payload),
                }
            )

        # Atomic swap — only replace old docs if we wrote at least one file
        if result["saved_files"]:
            if os.path.exists(save_dir):
                shutil.rmtree(save_dir)
            shutil.move(tmp_save_dir, save_dir)
        else:
            shutil.rmtree(tmp_save_dir)  # nothing written, clean up

    # Priority PDFs first, then smaller ones first within each group
    pdf_parts.sort(key=lambda x: (0 if x[2] else 1, len(x[1])))

    total_chars = 0
    vision_count = 0

    for fname, payload, is_priority in pdf_parts:
        if total_chars >= MAX_TOTAL_CHARS and vision_count >= MAX_VISION_IMAGES:
            break

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(payload)
            tmp_path = tmp.name

        try:
            text = _get_pdf_extractor()(tmp_path)
        except Exception:
            text = ""
        finally:
            os.unlink(tmp_path)

        if text and len(text.strip()) > 100:
            if total_chars < MAX_TOTAL_CHARS:
                chunk = text.strip()[:MAX_PER_PDF]
                result["attachments"].append(
                    {
                        "filename": fname,
                        "type": "pdf",
                        "text": chunk,
                    }
                )
                total_chars += len(chunk)
        elif is_priority and vision_count < MAX_VISION_IMAGES:
            img_b64 = _pdf_to_base64_image(payload)
            if img_b64:
                result["vision_attachments"].append(
                    {
                        "filename": fname,
                        "base64": img_b64,
                    }
                )
                vision_count += 1

    return result
