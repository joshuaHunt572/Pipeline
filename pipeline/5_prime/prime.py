#!/usr/bin/env python3
"""
Prime Module
Produces analysis and actionable items from preprocessed data.
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


class PrimeOutput(BaseModel):
    """Schema for Prime module output"""
    analysis: str
    actionable: List[Dict[str, Any]] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PrimeModule:
    """Prime module - produces analysis and actionable items"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('prime', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/5_prime/Prime_Inbox'))
        self.output_dir = Path(self.module_config.get('output', 'pipeline/5_prime/Prime_Output'))
        self.final_output_dir = Path(self.module_config.get('final_output', 'pipeline/5_prime/Final_Output/prime'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.final_output_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Prime module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/5_prime/prime.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('Prime')
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

    def generate_analysis(self, tasks: List[Dict], events: List[Dict], notes: List[str]) -> PrimeOutput:
        """
        Placeholder for analysis generation logic.
        Replace this with actual LLM-based analysis.
        """
        self.logger.info(f"Generating analysis for {len(tasks)} tasks, {len(events)} events")

        # PLACEHOLDER: Replace with actual analysis logic
        analysis = f"""[PLACEHOLDER ANALYSIS]
This is a summary analysis of the preprocessed data.
- Total tasks identified: {len(tasks)}
- Total events identified: {len(events)}
- Total notes captured: {len(notes)}

Key insights would be generated here by an LLM."""

        actionable = [
            {
                "action": "[PLACEHOLDER ACTION 1] High-priority action item",
                "source": "tasks",
                "priority": "high",
                "deadline": "TBD"
            },
            {
                "action": "[PLACEHOLDER ACTION 2] Follow-up required",
                "source": "events",
                "priority": "medium",
                "deadline": "TBD"
            }
        ]

        context = {
            "total_tasks": len(tasks),
            "total_events": len(events),
            "total_notes": len(notes),
            "priority_breakdown": {
                "high": 1,
                "medium": 1,
                "low": 0
            }
        }

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "processing_module": "prime",
            "version": "1.0",
            "actionable_count": len(actionable)
        }

        return PrimeOutput(
            analysis=analysis,
            actionable=actionable,
            context=context,
            metadata=metadata
        )

    def process_preprocessed_file(self, file_path: Path) -> PrimeOutput:
        """Process a preprocessed JSON file"""
        self.logger.info(f"Processing preprocessed file: {file_path.name}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            tasks = data.get('tasks', [])
            events = data.get('events', [])
            notes = data.get('notes', [])

            # Generate analysis and actionable items
            output = self.generate_analysis(tasks, events, notes)

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

    def safe_write_output(self, output_data: PrimeOutput, source_file: Path):
        """Write output JSON safely with atomic operation to both output and final directories"""
        output_filename = source_file.stem.replace('_preprocessed', '') + '_primed.json'

        # Write to Prime_Output
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

        # Also write to Final_Output/prime
        final_path = self.final_output_dir / output_filename
        try:
            shutil.copy2(output_path, final_path)
            self.logger.info(f"Final output written: {final_path}")
        except Exception as e:
            self.logger.error(f"Failed to write final output: {e}")

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

                # Process the preprocessed file
                output = self.process_preprocessed_file(file_path)

                # Write output
                self.safe_write_output(output, file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Prime module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Prime module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Prime module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = PrimeModule()
    module.run()


if __name__ == "__main__":
    main()
