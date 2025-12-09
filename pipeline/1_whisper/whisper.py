#!/usr/bin/env python3
"""
Whisper Module
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
except ImportError:
    print("Missing dependencies. Install: pip install pydantic pyyaml")
    sys.exit(1)


class WhisperOutput(BaseModel):
    """Schema for Whisper module output"""
    transcript: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WhisperModule:
    """Whisper processing module - converts audio to text"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('whisper', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/1_whisper/Whisper_Inbox'))
        self.output_dir = Path(self.module_config.get('output', 'pipeline/1_whisper/Whisper_Output'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Whisper module initialized. Watching: {self.inbox_dir}")

    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"Config file not found: {config_path}")
            return {}

    def _setup_logging(self):
        """Configure logging for this module"""
        log_level = self.config.get('logging', {}).get('level', 'INFO')
        log_file = Path(self.module_config.get('log_file', 'pipeline/1_whisper/whisper.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('Whisper')
        self.logger.setLevel(getattr(logging, log_level))

        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(getattr(logging, log_level))

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, log_level))

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def process_audio_file(self, file_path: Path) -> WhisperOutput:
        """
        Placeholder for Whisper audio-to-text conversion.
        Replace this with actual Whisper model logic.
        """
        self.logger.info(f"Processing audio file: {file_path.name}")

        # PLACEHOLDER: Replace with actual Whisper transcription
        transcript = f"[PLACEHOLDER TRANSCRIPT] Audio file '{file_path.name}' would be transcribed here."

        metadata = {
            "source_file": file_path.name,
            "timestamp": datetime.now().isoformat(),
            "file_size": file_path.stat().st_size,
            "processing_module": "whisper",
            "version": "1.0"
        }

        return WhisperOutput(
            transcript=transcript,
            metadata=metadata
        )

    def safe_write_output(self, output_data: WhisperOutput, source_file: Path):
        """Write output JSON safely with atomic operation"""
        output_filename = source_file.stem + '_transcript.json'
        output_path = self.output_dir / output_filename
        temp_path = output_path.with_suffix('.tmp')

        try:
            # Write to temp file first
            with open(temp_path, 'w') as f:
                json.dump(output_data.model_dump(), f, indent=2)

            # Atomic rename
            temp_path.rename(output_path)
            self.logger.info(f"Output written: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to write output: {e}")
            if temp_path.exists():
                temp_path.unlink()
            raise

    def archive_file(self, file_path: Path):
        """Move processed file to archive"""
        try:
            archive_path = self.archive_dir / file_path.name
            shutil.move(str(file_path), str(archive_path))
            self.logger.info(f"Archived: {file_path.name}")
        except Exception as e:
            self.logger.error(f"Failed to archive {file_path.name}: {e}")

    def process_inbox(self):
        """Process all files in inbox directory"""
        # Look for audio files (placeholder: process any file)
        audio_extensions = ['.mp3', '.wav', '.m4a', '.flac', '.ogg', '.aac']

        files = [
            f for f in self.inbox_dir.iterdir()
            if f.is_file() and f.suffix.lower() in audio_extensions
        ]

        if not files:
            return

        self.logger.info(f"Found {len(files)} file(s) to process")

        for file_path in files:
            try:
                self.logger.info(f"Processing: {file_path.name}")

                # Process the audio file
                output = self.process_audio_file(file_path)

                # Write output
                self.safe_write_output(output, file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Whisper module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Whisper module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Whisper module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = WhisperModule()
    module.run()


if __name__ == "__main__":
    main()
