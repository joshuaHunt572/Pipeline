#!/usr/bin/env python3
"""
Whisper Module (Faster-Whisper Engine)
Converts audio files to transcripts with metadata.
"""

import os
import sys
import json
import time
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add project root to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    import yaml
    from pydantic import BaseModel, Field
    from faster_whisper import WhisperModel
except ImportError:
    print("Missing dependencies. Install with:")
    print("pip install pydantic pyyaml faster-whisper")
    sys.exit(1)


class WhisperOutput(BaseModel):
    """Structured output schema for Whisper transcriptions."""
    transcript: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WhisperModule:
    """Whisper → transcription engine using Faster-Whisper."""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        module_cfg = self.config.get("whisper", {})

        # Paths
        self.inbox_dir = ROOT / module_cfg.get("inbox", "pipeline/1_whisper/Whisper_Inbox")
        self.output_dir = ROOT / module_cfg.get("output", "pipeline/1_whisper/Whisper_Output")
        self.archive_dir = self.inbox_dir / "archive"

        # Ensure dirs exist
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Logging
        self._setup_logging()

        # Poll rate for checking inbox
        self.poll_interval = module_cfg.get("poll_interval", 5)

        # Initialize model placeholder (lazy load)
        self.model = None

        self.logger.info(f"Whisper module ready. Watching inbox: {self.inbox_dir}")

    # -------------------------------------------------------------
    # CONFIG
    # -------------------------------------------------------------
    def _load_config(self, path: str) -> Dict:
        try:
            with open(ROOT / path, "r") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"Config not found: {path}")
            return {}

    # -------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------
    def _setup_logging(self):
        log_level_name = self.config.get("logging", {}).get("level", "INFO")
        log_level = getattr(logging, log_level_name, logging.INFO)

        log_path = ROOT / "pipeline/1_whisper/whisper.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger("Whisper")
        self.logger.setLevel(log_level)

        fh = logging.FileHandler(log_path)
        ch = logging.StreamHandler()

        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        ch.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    # -------------------------------------------------------------
    # MODEL
    # -------------------------------------------------------------
    def _load_model(self):
        """Load Faster-Whisper model only once."""
        if self.model is None:
            self.logger.info("Loading Faster-Whisper model: base.en (float16)")
            self.model = WhisperModel(
                "base.en",
                device="cuda",
                compute_type="float16"
            )

    # -------------------------------------------------------------
    # TRANSCRIPTION
    # -------------------------------------------------------------
    def process_audio(self, file_path: Path) -> WhisperOutput:
        """Run real transcription using Faster-Whisper."""
        self._load_model()

        self.logger.info(f"Transcribing: {file_path.name}")

        segments, info = self.model.transcribe(str(file_path), beam_size=1)

        transcript = "".join(seg.text for seg in segments).strip()

        metadata = {
            "source_file": file_path.name,
            "timestamp": datetime.now().isoformat(),
            "file_size": file_path.stat().st_size,
            "engine": "faster-whisper",
            "model": "base.en",
            "language": info.language,
            "duration": info.duration,
            "processing_module": "whisper",
        }

        return WhisperOutput(transcript=transcript, metadata=metadata)

    # -------------------------------------------------------------
    # FILE OPERATIONS
    # -------------------------------------------------------------
    def _write_output(self, data: WhisperOutput, src: Path):
        out_file = src.stem + "_transcript.json"
        tmp = (self.output_dir / out_file).with_suffix(".tmp")
        final = self.output_dir / out_file

        try:
            with open(tmp, "w") as f:
                json.dump(data.model_dump(), f, indent=2)
            tmp.rename(final)
            self.logger.info(f"Wrote transcript → {final.name}")
        except Exception as e:
            self.logger.error(f"Write failed: {e}")
            if tmp.exists():
                tmp.unlink()

    def _archive(self, file_path: Path):
        dest = self.archive_dir / file_path.name
        try:
            shutil.move(str(file_path), str(dest))
            self.logger.info(f"Archived: {file_path.name}")
        except Exception as e:
            self.logger.error(f"Failed archive: {e}")

    # -------------------------------------------------------------
    # MAIN LOOP
    # -------------------------------------------------------------
    def _process_inbox(self):
        audio_exts = {".m4a", ".mp3", ".wav", ".aac", ".ogg", ".flac"}

        files = [f for f in self.inbox_dir.iterdir() if f.suffix.lower() in audio_exts]

        if not files:
            return

        self.logger.info(f"Found {len(files)} audio file(s). Starting transcription...")

        for file_path in files:
            try:
                out = self.process_audio(file_path)
                self._write_output(out, file_path)
                self._archive(file_path)
            except Exception as e:
                self.logger.error(f"Error: {e}", exc_info=True)

    def run(self):
        """Loop forever polling for new audio files."""
        self.logger.info("Whisper module running.")

        try:
            while True:
                self._process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Whisper module stopped.")
        except Exception as e:
            self.logger.error(f"Fatal: {e}", exc_info=True)


def main():
    WhisperModule().run()


if __name__ == "__main__":
    main()