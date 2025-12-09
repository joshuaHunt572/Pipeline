#!/usr/bin/env python3
"""
Output Engine Module
Formats JSON into structured versions (tables, lists, summaries).
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


class OutputEngineModule:
    """Output Engine module - formats JSON into structured versions"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('output_engine', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/6_output_engine/Output_Inbox'))
        self.final_output_base = Path(self.module_config.get('final_output', 'pipeline/6_output_engine/Final_Output'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.final_output_base.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Output Engine module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/6_output_engine/output_engine.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('OutputEngine')
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

    def format_as_table(self, actionable: List[Dict[str, Any]]) -> str:
        """Format actionable items as a text table"""
        if not actionable:
            return "No actionable items.\n"

        table = "ACTIONABLE ITEMS TABLE\n"
        table += "=" * 80 + "\n"
        table += f"{'Action':<50} {'Priority':<15} {'Deadline':<15}\n"
        table += "-" * 80 + "\n"

        for item in actionable:
            action = item.get('action', 'N/A')[:48]
            priority = item.get('priority', 'N/A')
            deadline = item.get('deadline', 'TBD')
            table += f"{action:<50} {priority:<15} {deadline:<15}\n"

        table += "=" * 80 + "\n"
        return table

    def format_as_list(self, actionable: List[Dict[str, Any]]) -> str:
        """Format actionable items as a bulleted list"""
        if not actionable:
            return "No actionable items.\n"

        list_output = "ACTIONABLE ITEMS LIST\n"
        list_output += "=" * 60 + "\n\n"

        for i, item in enumerate(actionable, 1):
            action = item.get('action', 'N/A')
            priority = item.get('priority', 'N/A')
            deadline = item.get('deadline', 'TBD')
            list_output += f"{i}. {action}\n"
            list_output += f"   Priority: {priority} | Deadline: {deadline}\n\n"

        return list_output

    def format_as_summary(self, data: Dict[str, Any]) -> str:
        """Format the complete data as a summary"""
        summary = "PIPELINE PROCESSING SUMMARY\n"
        summary += "=" * 80 + "\n\n"

        # Analysis section
        if 'analysis' in data:
            summary += "ANALYSIS:\n"
            summary += "-" * 80 + "\n"
            summary += data['analysis'] + "\n\n"

        # Context section
        if 'context' in data:
            summary += "CONTEXT:\n"
            summary += "-" * 80 + "\n"
            context = data['context']
            for key, value in context.items():
                summary += f"  {key}: {value}\n"
            summary += "\n"

        # Metadata section
        if 'metadata' in data:
            summary += "METADATA:\n"
            summary += "-" * 80 + "\n"
            metadata = data['metadata']
            for key, value in metadata.items():
                if key not in ['source_metadata']:
                    summary += f"  {key}: {value}\n"
            summary += "\n"

        summary += "=" * 80 + "\n"
        return summary

    def process_primed_file(self, file_path: Path):
        """Process a primed JSON file and generate structured outputs"""
        self.logger.info(f"Processing primed file: {file_path.name}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            actionable = data.get('actionable', [])

            # Generate base filename
            base_name = file_path.stem.replace('_primed', '')

            # Create structured output directories
            structured_dir = self.final_output_base / f"structured_{base_name}"
            structured_dir.mkdir(parents=True, exist_ok=True)

            # Generate table format
            table_output = self.format_as_table(actionable)
            table_path = structured_dir / f"{base_name}_table.txt"
            with open(table_path, 'w') as f:
                f.write(table_output)
            self.logger.info(f"Table format written: {table_path}")

            # Generate list format
            list_output = self.format_as_list(actionable)
            list_path = structured_dir / f"{base_name}_list.txt"
            with open(list_path, 'w') as f:
                f.write(list_output)
            self.logger.info(f"List format written: {list_path}")

            # Generate summary format
            summary_output = self.format_as_summary(data)
            summary_path = structured_dir / f"{base_name}_summary.txt"
            with open(summary_path, 'w') as f:
                f.write(summary_output)
            self.logger.info(f"Summary format written: {summary_path}")

            # Also keep the original JSON
            json_path = structured_dir / f"{base_name}_data.json"
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"JSON data written: {json_path}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path.name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
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

                # Process the primed file
                self.process_primed_file(file_path)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully processed: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Output Engine module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Output Engine module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Output Engine module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = OutputEngineModule()
    module.run()


if __name__ == "__main__":
    main()
