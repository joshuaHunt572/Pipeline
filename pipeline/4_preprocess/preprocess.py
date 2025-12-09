#!/usr/bin/env python3
"""
Preprocess Module
Normalizes fields and enforces schemas on categorized data.
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
    from pydantic import BaseModel, Field, validator
except ImportError:
    print("Missing dependencies. Install: pip install pydantic pyyaml")
    sys.exit(1)


class NormalizedTask(BaseModel):
    """Normalized task schema"""
    task: str
    priority: str = "medium"
    status: str = "pending"
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class NormalizedEvent(BaseModel):
    """Normalized event schema"""
    event: str
    date: str = "TBD"
    type: str = "general"
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class PreprocessOutput(BaseModel):
    """Schema for Preprocess module output"""
    tasks: List[NormalizedTask] = Field(default_factory=list)
    events: List[NormalizedEvent] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PreprocessModule:
    """Preprocess module - normalizes fields and enforces schemas"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('preprocess', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/4_preprocess/Preprocess_Inbox'))
        self.output_dir = Path(self.module_config.get('output', 'pipeline/4_preprocess/Preprocessing_Output'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Preprocess module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/4_preprocess/preprocess.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('Preprocess')
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

    def normalize_data(self, tasks: List[Dict], events: List[Dict], notes: List[str]) -> PreprocessOutput:
        """
        Normalize and validate data according to schemas.
        """
        self.logger.info(f"Normalizing {len(tasks)} tasks, {len(events)} events, {len(notes)} notes")

        normalized_tasks = []
        normalized_events = []

        # Normalize tasks
        for task_data in tasks:
            try:
                task = NormalizedTask(**task_data)
                normalized_tasks.append(task)
            except Exception as e:
                self.logger.warning(f"Failed to normalize task: {task_data}. Error: {e}")

        # Normalize events
        for event_data in events:
            try:
                event = NormalizedEvent(**event_data)
                normalized_events.append(event)
            except Exception as e:
                self.logger.warning(f"Failed to normalize event: {event_data}. Error: {e}")

        # Notes are already strings, just clean them
        normalized_notes = [str(note).strip() for note in notes if note]

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "processing_module": "preprocess",
            "version": "1.0",
            "normalized_tasks": len(normalized_tasks),
            "normalized_events": len(normalized_events),
            "normalized_notes": len(normalized_notes),
            "validation_errors": (len(tasks) - len(normalized_tasks)) + (len(events) - len(normalized_events))
        }

        return PreprocessOutput(
            tasks=normalized_tasks,
            events=normalized_events,
            notes=normalized_notes,
            metadata=metadata
        )

    def process_categorized_file(self, file_path: Path) -> PreprocessOutput:
        """Process a categorized JSON file"""
        self.logger.info(f"Processing categorized file: {file_path.name}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            tasks = data.get('tasks', [])
            events = data.get('events', [])
            notes = data.get('notes', [])

            # Normalize the data
            output = self.normalize_data(tasks, events, notes)

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

    def safe_write_output(self, output_data: PreprocessOutput, source_file: Path):
        """Write output JSON safely with atomic operation"""
        output_filename = source_file.stem.replace('_categorized', '') + '_preprocessed.json'
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

                # Process the categorized file
                output = self.process_categorized_file(file_path)

                # Write output
                self.safe_write_output(output, file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Preprocess module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Preprocess module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Preprocess module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = PreprocessModule()
    module.run()


if __name__ == "__main__":
    main()
