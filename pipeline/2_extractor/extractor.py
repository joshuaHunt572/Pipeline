#!/usr/bin/env python3
"""
Extractor Module
Extracts facts and unknown items from transcripts.
"""

import os
import sys
import json
import time
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add parent directory to path for config access
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import yaml
    from pydantic import BaseModel, Field
except ImportError:
    print("Missing dependencies. Install: pip install pydantic pyyaml")
    sys.exit(1)


class ExtractorOutput(BaseModel):
    """Schema for Extractor module output"""
    facts: List[str] = Field(default_factory=list)
    unknown: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExtractorModule:
    """Extractor processing module - extracts facts and unknowns from transcripts"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('extractor', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/2_extractor/Extractor_Inbox'))
        self.output_dir = Path(self.module_config.get('output', 'pipeline/2_extractor/Extractor_Output'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Extractor module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/2_extractor/extractor.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('Extractor')
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

    def extract_facts(self, transcript: str) -> ExtractorOutput:
        """
        Placeholder for fact extraction logic.
        Replace this with actual LLM-based extraction.
        """
        self.logger.info("Extracting facts from transcript")

        # PLACEHOLDER: Replace with actual extraction logic
        facts = [
            f"[PLACEHOLDER FACT 1] Extracted from transcript",
            f"[PLACEHOLDER FACT 2] Another extracted fact",
            f"[PLACEHOLDER FACT 3] Third extracted fact"
        ]

        unknown = [
            "[PLACEHOLDER UNKNOWN 1] Item requiring clarification",
            "[PLACEHOLDER UNKNOWN 2] Another uncertain item"
        ]

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "processing_module": "extractor",
            "version": "1.0",
            "facts_count": len(facts),
            "unknown_count": len(unknown)
        }

        return ExtractorOutput(
            facts=facts,
            unknown=unknown,
            metadata=metadata
        )

    def process_transcript_file(self, file_path: Path) -> ExtractorOutput:
        """Process a transcript JSON file"""
        self.logger.info(f"Processing transcript file: {file_path.name}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            transcript = data.get('transcript', '')

            if not transcript:
                self.logger.warning(f"No transcript found in {file_path.name}")
                return ExtractorOutput(metadata={"error": "No transcript found"})

            # Extract facts and unknowns
            output = self.extract_facts(transcript)

            # Add source metadata
            output.metadata['source_file'] = file_path.name
            if 'metadata' in data:
                output.metadata['source_metadata'] = data['metadata']

            return output

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path.name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            raise

    def safe_write_output(self, output_data: ExtractorOutput, source_file: Path):
        """Write output JSON safely with atomic operation"""
        output_filename = source_file.stem.replace('_transcript', '') + '_extracted.json'
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
        """Process all JSON files in inbox directory"""
        files = [
            f for f in self.inbox_dir.iterdir()
            if f.is_file() and f.suffix == '.json'
        ]

        if not files:
            return

        self.logger.info(f"Found {len(files)} file(s) to process")

        for file_path in files:
            try:
                self.logger.info(f"Processing: {file_path.name}")

                # Process the transcript file
                output = self.process_transcript_file(file_path)

                # Write output
                self.safe_write_output(output, file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Extractor module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Extractor module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Extractor module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = ExtractorModule()
    module.run()


if __name__ == "__main__":
    main()
