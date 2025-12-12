#!/usr/bin/env python3
"""
Output Engine Module
Formats JSON into structured versions (tables, lists, summaries),
AND emits a handoff JSON for the next stage (Synthesis).
"""

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
except ImportError:
    print("Missing dependencies. Install: pip install pyyaml")
    sys.exit(1)


class OutputEngineModule:
    """Output Engine module - formats JSON into structured versions"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.module_config = self.config.get('output_engine', {})

        # Paths
        self.inbox_dir = Path(self.module_config.get('inbox', 'pipeline/6_output_engine/Output_Inbox'))

        # NEW: standard output dir for router/supervisor handoff
        self.output_dir = Path(self.module_config.get('output', 'pipeline/6_output_engine/Output_Output'))

        # Existing final output area
        self.final_output_base = Path(self.module_config.get('final_output', 'pipeline/6_output_engine/Final_Output'))

        self.archive_dir = self.inbox_dir / 'archive'

        # Create directories
        for d in [self.inbox_dir, self.output_dir, self.final_output_base, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Logging
        self._setup_logging()

        self.poll_interval = self.module_config.get('poll_interval', 5)
        self.logger.info(f"Output Engine initialized. Watching: {self.inbox_dir}")
        self.logger.info(f"Output Engine handoff output: {self.output_dir}")
        self.logger.info(f"Output Engine final outputs: {self.final_output_base}")

    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"Config file not found: {config_path}")
            return {}

    def _setup_logging(self):
        log_level = self.config.get('logging', {}).get('level', 'INFO')
        log_file = Path(self.module_config.get('log_file', 'pipeline/6_output_engine/output_engine.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger('OutputEngine')
        self.logger.setLevel(getattr(logging, log_level))

        # Avoid duplicate handlers if module is restarted
        if self.logger.handlers:
            return

        fh = logging.FileHandler(log_file)
        fh.setLevel(getattr(logging, log_level))

        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, log_level))

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def format_as_table(self, actionable: List[Dict[str, Any]]) -> str:
        if not actionable:
            return "No actionable items.\n"

        table = "ACTIONABLE ITEMS TABLE\n"
        table += "=" * 80 + "\n"
        table += f"{'Action':<50} {'Priority':<15} {'Deadline':<15}\n"
        table += "-" * 80 + "\n"

        for item in actionable:
            action = str(item.get('action', 'N/A'))[:48]
            priority = str(item.get('priority', 'N/A'))
            deadline = str(item.get('deadline', 'TBD'))
            table += f"{action:<50} {priority:<15} {deadline:<15}\n"

        table += "=" * 80 + "\n"
        return table

    def format_as_list(self, actionable: List[Dict[str, Any]]) -> str:
        if not actionable:
            return "No actionable items.\n"

        out = "ACTIONABLE ITEMS LIST\n"
        out += "=" * 60 + "\n\n"

        for i, item in enumerate(actionable, 1):
            action = item.get('action', 'N/A')
            priority = item.get('priority', 'N/A')
            deadline = item.get('deadline', 'TBD')
            out += f"{i}. {action}\n"
            out += f"   Priority: {priority} | Deadline: {deadline}\n\n"

        return out

    def format_as_summary(self, data: Dict[str, Any]) -> str:
        summary = "PIPELINE PROCESSING SUMMARY\n"
        summary += "=" * 80 + "\n\n"

        if 'analysis' in data:
            summary += "ANALYSIS:\n"
            summary += "-" * 80 + "\n"
            summary += str(data['analysis']) + "\n\n"

        if 'context' in data and isinstance(data['context'], dict):
            summary += "CONTEXT:\n"
            summary += "-" * 80 + "\n"
            for k, v in data['context'].items():
                summary += f"  {k}: {v}\n"
            summary += "\n"

        if 'metadata' in data and isinstance(data['metadata'], dict):
            summary += "METADATA:\n"
            summary += "-" * 80 + "\n"
            for k, v in data['metadata'].items():
                if k != 'source_metadata':
                    summary += f"  {k}: {v}\n"
            summary += "\n"

        summary += "=" * 80 + "\n"
        return summary

    def _safe_write_json(self, path: Path, payload: Dict[str, Any]):
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        tmp.replace(path)

    def process_primed_file(self, file_path: Path):
        """Process a primed JSON file and generate structured outputs + handoff JSON."""
        self.logger.info(f"Processing primed file: {file_path.name}")

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        actionable = data.get('actionable', [])
        base_name = file_path.stem.replace('_primed', '')

        # --- Final structured artifacts (human friendly) ---
        structured_dir = self.final_output_base / f"structured_{base_name}"
        structured_dir.mkdir(parents=True, exist_ok=True)

        table_path = structured_dir / f"{base_name}_table.txt"
        list_path = structured_dir / f"{base_name}_list.txt"
        summary_path = structured_dir / f"{base_name}_summary.txt"
        json_copy_path = structured_dir / f"{base_name}_data.json"

        with open(table_path, 'w', encoding='utf-8') as f:
            f.write(self.format_as_table(actionable))

        with open(list_path, 'w', encoding='utf-8') as f:
            f.write(self.format_as_list(actionable))

        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(self.format_as_summary(data))

        with open(json_copy_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Structured outputs written under: {structured_dir}")

        # --- NEW: Handoff JSON (machine friendly) for next stage ---
        handoff = {
            "actionable": actionable,
            "analysis": data.get("analysis", ""),
            "context": data.get("context", {}),
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "processing_module": "output_engine",
                "version": "1.1",
                "source_file": file_path.name,
                "structured_dir": str(structured_dir),
                "artifacts": {
                    "table_txt": str(table_path),
                    "list_txt": str(list_path),
                    "summary_txt": str(summary_path),
                    "json_copy": str(json_copy_path),
                }
            }
        }

        out_path = self.output_dir / f"{base_name}_formatted.json"
        self._safe_write_json(out_path, handoff)
        self.logger.info(f"Handoff JSON written: {out_path}")

    def archive_file(self, file_path: Path):
        try:
            archive_path = self.archive_dir / file_path.name
            shutil.move(str(file_path), str(archive_path))
            self.logger.info(f"Archived: {file_path.name}")
        except Exception as e:
            self.logger.error(f"Failed to archive {file_path.name}: {e}")

    def process_inbox(self):
        files = [f for f in self.inbox_dir.iterdir() if f.is_file() and f.suffix.lower() == '.json']
        if not files:
            return

        self.logger.info(f"Found {len(files)} file(s) to process")

        for file_path in files:
            try:
                self.logger.info(f"Processing: {file_path.name}")
                self.process_primed_file(file_path)
                self.archive_file(file_path)
                self.logger.info(f"Successfully processed: {file_path.name}")
            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)

    def run(self):
        self.logger.info("Starting Output Engine polling loop")
        try:
            while True:
                self.process_inbox()
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.logger.info("Output Engine stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in Output Engine: {e}", exc_info=True)
            raise


def main():
    module = OutputEngineModule()
    module.run()


if __name__ == "__main__":
    main()