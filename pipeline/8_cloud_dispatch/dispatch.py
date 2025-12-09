#!/usr/bin/env python3
"""
Cloud Dispatch Module
Moves deliverables to a final folder (cloud-simulated).
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


class DispatchRecord(BaseModel):
    """Schema for dispatch tracking"""
    file_name: str
    file_type: str
    dispatched_at: str
    source_path: str
    destination_path: str
    file_size: int
    checksum: str = ""


class CloudDispatchModule:
    """Cloud Dispatch module - moves deliverables to cloud storage (simulated)"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('cloud_dispatch', {})

        # Set up paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/8_cloud_dispatch/Dispatch_Inbox'))
        self.cloud_results_dir = Path(self.module_config.get('cloud_results', 'pipeline/8_cloud_dispatch/Cloud_Results'))
        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.cloud_results_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Cloud Dispatch module initialized. Watching: {self.inbox_dir}")

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
        log_file = Path(self.module_config.get('log_file', 'pipeline/8_cloud_dispatch/dispatch.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('CloudDispatch')
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

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate simple checksum for file (placeholder for actual hash)"""
        # PLACEHOLDER: Replace with actual hash calculation (e.g., SHA256)
        try:
            file_size = file_path.stat().st_size
            return f"CHECKSUM_{file_size}_{file_path.name}"
        except Exception as e:
            self.logger.warning(f"Could not calculate checksum: {e}")
            return "CHECKSUM_UNKNOWN"

    def dispatch_file(self, file_path: Path) -> DispatchRecord:
        """
        Dispatch a file to cloud storage (simulated).
        In production, this would upload to S3, GCS, Azure, etc.
        """
        self.logger.info(f"Dispatching file: {file_path.name}")

        # Determine file type
        file_type = file_path.suffix[1:] if file_path.suffix else "unknown"

        # Create organized directory structure in cloud results
        timestamp = datetime.now()
        date_dir = self.cloud_results_dir / timestamp.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        # Destination path
        dest_path = date_dir / file_path.name

        # Copy file to cloud results directory
        try:
            shutil.copy2(file_path, dest_path)
            self.logger.info(f"File copied to: {dest_path}")
        except Exception as e:
            self.logger.error(f"Failed to copy file: {e}")
            raise

        # Create dispatch record
        record = DispatchRecord(
            file_name=file_path.name,
            file_type=file_type,
            dispatched_at=timestamp.isoformat(),
            source_path=str(file_path),
            destination_path=str(dest_path),
            file_size=file_path.stat().st_size,
            checksum=self.calculate_checksum(file_path)
        )

        return record

    def save_dispatch_record(self, record: DispatchRecord):
        """Save dispatch record to a manifest file"""
        manifest_path = self.cloud_results_dir / "dispatch_manifest.jsonl"

        try:
            with open(manifest_path, 'a') as f:
                f.write(json.dumps(record.model_dump()) + '\n')
            self.logger.info(f"Dispatch record saved to manifest")
        except Exception as e:
            self.logger.error(f"Failed to save dispatch record: {e}")

    def archive_file(self, file_path: Path):
        """Move dispatched file to archive"""
        try:
            archive_path = self.archive_dir / file_path.name
            shutil.move(str(file_path), str(archive_path))
            self.logger.info(f"Archived: {file_path.name}")
        except Exception as e:
            self.logger.error(f"Failed to archive {file_path.name}: {e}")

    def process_inbox(self):
        """Process all files in inbox directory"""
        # Process all file types
        files = [
            f for f in self.inbox_dir.iterdir()
            if f.is_file()
        ]

        if not files:
            return

        self.logger.info(f"Found {len(files)} file(s) to dispatch")

        for file_path in files:
            try:
                self.logger.info(f"Processing: {file_path.name}")

                # Dispatch the file
                record = self.dispatch_file(file_path)

                # Save dispatch record
                self.save_dispatch_record(record)

                # Archive the source file
                self.archive_file(file_path)

                self.logger.info(f"Successfully dispatched: {file_path.name}")

            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        """Main loop - poll inbox and process files"""
        self.logger.info("Starting Cloud Dispatch module polling loop")

        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Cloud Dispatch module stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Cloud Dispatch module: {e}", exc_info=True)
            raise


def main():
    """Entry point"""
    module = CloudDispatchModule()
    module.run()


if __name__ == "__main__":
    main()
