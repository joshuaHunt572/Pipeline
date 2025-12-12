#!/usr/bin/env python3
"""
Whisper Module (Faster-Whisper)
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

# Add parent directory to path for config access
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import yaml
    from pydantic import BaseModel, Field
    from faster_whisper import WhisperModel
except ImportError as e:
    print("Missing dependencies.")
    print("Install with: pip install faster-whisper pydantic pyyaml")
    raise e


class WhisperOutput(BaseModel):
    transcript: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WhisperModule:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get("whisper", {})

        # Paths
        self.inbox_dir = Path(self.module_config.get("inbox", "pipeline/1_whisper/Whisper_Inbox"))
        self.output_dir = Path(self.module_config.get("output", "pipeline/1_whisper/Whisper_Output"))
        self.archive_dir = self.inbox_dir / "archive"

        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        self.poll_interval = self.module_config.get("poll_interval", 5)

        self._setup_logging()

        # Whisper model config
        self.model_size = self.module_config.get("model_size", "base")
        self.device = self.module_config.get("device", "auto")
        self.compute_type = self.module_config.get("compute_type", "auto")

        self.logger.info(
            f"Loading Faster-Whisper model: {self.model_size} | device={self.device}"
        )

        self.model = WhisperModel(
            self.model_size,
            device=self.device,
            compute_type=self.compute_type
        )

        self.logger.info(f"Whisper module initialized. Watching {self.inbox_dir}")

    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            return {}

    def _setup_logging(self):
        log_level = self.config.get("logging", {}).get("level", "INFO")
        log_file = Path(self.module_config.get("log_file", "pipeline/1_whisper/whisper.log"))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger("Whisper")
        self.logger.setLevel(getattr(logging, log_level))

        if not self.logger.handlers:
            fh = logging.FileHandler(log_file)
            ch = logging.StreamHandler()

            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def process_audio_file(self, file_path: Path) -> WhisperOutput:
        self.logger.info(f"Transcribing {file_path.name}")

        segments, info = self.model.transcribe(
            str(file_path),
            beam_size=5,
            vad_filter=True
        )

        transcript_parts = []
        for segment in segments:
            transcript_parts.append(segment.text)

        transcript = " ".join(transcript_parts).strip()

        metadata = {
            "source_file": file_path.name,
            "timestamp": datetime.now().isoformat(),
            "language": info.language,
            "duration": info.duration,
            "model": self.model_size,
            "processing_module": "whisper"
        }

        return WhisperOutput(
            transcript=transcript,
            metadata=metadata
        )

    def safe_write_output(self, output_data: WhisperOutput, source_file: Path):
        output_filename = source_file.stem + "_transcript.json"
        output_path = self.output_dir / output_filename
        temp_path = output_path.with_suffix(".tmp")

        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(output_data.model_dump(), f, indent=2, ensure_ascii=False)

        temp_path.rename(output_path)
        self.logger.info(f"Wrote transcript → {output_path.name}")

    def archive_file(self, file_path: Path):
        shutil.move(str(file_path), self.archive_dir / file_path.name)

    def process_inbox(self):
        audio_exts = {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac"}

        files = [
            f for f in self.inbox_dir.iterdir()
            if f.is_file() and f.suffix.lower() in audio_exts
        ]

        for file_path in files:
            try:
                output = self.process_audio_file(file_path)
                self.safe_write_output(output, file_path)
                self.archive_file(file_path)
            except Exception as e:
                self.logger.error(f"Failed {file_path.name}: {e}", exc_info=True)

    def run(self):
        self.logger.info("Whisper polling loop started")
        while True:
            self.process_inbox()
            time.sleep(self.poll_interval)


def main():
    WhisperModule().run()


if __name__ == "__main__":
    main()