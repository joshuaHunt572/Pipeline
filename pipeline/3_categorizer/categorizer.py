#!/usr/bin/env python3
"""
Categorizer Module
Categorizes extracted facts into tasks, events, and notes.
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


class CategorizerOutput(BaseModel):
    """Schema for Categorizer module output"""
    tasks: List[Dict[str, Any]] = Field(default_factory=list)
    events: List[Dict[str, Any]] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CategorizerModule:
    """Categorizer processing module - categorizes facts into tasks, events, notes"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('categorizer', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/3_categorizer/Categorizer_Inbox'))
        self.output_dir = Path(self.module_config.get('output', 'pipeline/3_categorizer/Categorizer_Output'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Categorizer module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/3_categorizer/categorizer.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('Categorizer')
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

    def categorize_facts(self, facts: List[str], unknown: List[str]) -> CategorizerOutput:
        """
        Placeholder for categorization logic.
        Replace this with actual LLM-based categorization.
        """
        self.logger.info(f"Categorizing {len(facts)} facts")

        # PLACEHOLDER: Replace with actual categorization logic
        tasks = [
            {
                "task": "[PLACEHOLDER TASK] Action item from facts",
                "priority": "medium",
                "status": "pending"
            },
            {
                "task": "[PLACEHOLDER TASK] Another action item",
                "priority": "low",
                "status": "pending"
            }
        ]

        events = [
            {
                "event": "[PLACEHOLDER EVENT] Meeting or deadline",
                "date": "TBD",
                "type": "meeting"
            },
            {
                "event": "[PLACEHOLDER EVENT] Another scheduled item",
                "date": "TBD",
                "type": "deadline"
            }
        ]

        notes = [
            "[PLACEHOLDER NOTE] General information from facts",
            "[PLACEHOLDER NOTE] Additional context or observation"
        ]

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "processing_module": "categorizer",
            "version": "1.0",
            "tasks_count": len(tasks),
            "events_count": len(events),
            "notes_count": len(notes),
            "unknown_items": len(unknown)
        }

        return CategorizerOutput(
            tasks=tasks,
            events=events,
            notes=notes,
            metadata=metadata
        )

    def process_extracted_file(self, file_path: Path) -> CategorizerOutput:
        """Process an extracted facts JSON file"""
        self.logger.info(f"Processing extracted file: {file_path.name}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            facts = data.get('facts', [])
            unknown = data.get('unknown', [])

            if not facts and not unknown:
                self.logger.warning(f"No facts or unknowns found in {file_path.name}")

            # Categorize the facts
            output = self.categorize_facts(facts, unknown)

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

    def safe_write_output(self, output_data: CategorizerOutput, source_file: Path):
        """Write output JSON safely with atomic operation"""
        output_filename = source_file.stem.replace('_extracted', '') + '_categorized.json'
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

                # Process the extracted file
                output = self.process_extracted_file(file_path)

                # Write output
                self.safe_write_output(output, file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Categorizer module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Categorizer module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Categorizer module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = CategorizerModule()
    module.run()


if __name__ == "__main__":
    main()
