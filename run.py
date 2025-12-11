#!/usr/bin/env python3
"""
Pipeline Supervisor
Launches and manages all pipeline modules as background processes.
"""

import sys
import time
import signal
import logging
import subprocess
from pathlib import Path
from typing import List, Dict
from datetime import datetime

try:
    import yaml
except ImportError:
    print("Missing dependency: pyyaml")
    print("Install with: pip install pyyaml")
    sys.exit(1)


class PipelineSupervisor:
    """Supervisor for managing all pipeline modules"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.processes: Dict[str, subprocess.Popen] = {}
        self.running = True

        # Set up logging
        self._setup_logging()

        # Module definitions
        self.modules = [
            {
                "name": "whisper",
                "script": "pipeline/1_whisper/whisper.py",
                "description": "Audio to transcript conversion"
            },
            {
                "name": "extractor",
                "script": "pipeline/2_extractor/extractor.py",
                "description": "Fact extraction from transcripts"
            },
            {
                "name": "categorizer",
                "script": "pipeline/3_categorizer/categorizer.py",
                "description": "Categorization into tasks, events, notes"
            },
            {
                "name": "preprocess",
                "script": "pipeline/4_preprocess/preprocess.py",
                "description": "Data normalization and schema enforcement"
            },
            {
                "name": "prime",
                "script": "pipeline/5_prime/prime.py",
                "description": "Analysis and actionable generation"
            },
            {
                "name": "output_engine",
                "script": "pipeline/6_output_engine/output_engine.py",
                "description": "Structured output formatting"
            },
            {
                "name": "synthesis",
                "script": "pipeline/7_synthesis/synthesis.py",
                "description": "Narrative and long-form generation"
            },
            {
                "name": "cloud_dispatch",
                "script": "pipeline/8_cloud_dispatch/dispatch.py",
                "description": "Cloud delivery simulation"
            }
        ]

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> Dict:
        """Load YAML configuration"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            return {}

    def _setup_logging(self):
        """Configure logging for supervisor"""
        log_file = Path("pipeline_supervisor.log")

        self.logger = logging.getLogger('Supervisor')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.running = False
        self.stop_all_modules()
        sys.exit(0)

    def start_module(self, module: Dict) -> bool:
        """Start a single module as a subprocess"""
        name = module['name']
        script = module['script']

        if name in self.processes:
            self.logger.warning(f"Module {name} is already running")
            return False

        try:
            self.logger.info(f"Starting module: {name} ({module['description']})")

            process = subprocess.Popen(
                [sys.executable, script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            self.processes[name] = process
            self.logger.info(f"Module {name} started with PID {process.pid}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to start module {name}: {e}")
            return False

    def stop_module(self, name: str) -> bool:
        """Stop a running module"""
        if name not in self.processes:
            self.logger.warning(f"Module {name} is not running")
            return False

        try:
            process = self.processes[name]
            self.logger.info(f"Stopping module: {name} (PID {process.pid})")

            process.terminate()

            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Module {name} did not terminate gracefully, forcing...")
                process.kill()
                process.wait()

            del self.processes[name]
            self.logger.info(f"Module {name} stopped")
            return True

        except Exception as e:
            self.logger.error(f"Failed to stop module {name}: {e}")
            return False

    def check_module_health(self, name: str) -> bool:
        """Check if a module is still running"""
        if name not in self.processes:
            return False

        return self.processes[name].poll() is None

    def restart_module(self, name: str) -> bool:
        """Restart a module"""
        self.logger.info(f"Restarting module: {name}")

        module = next((m for m in self.modules if m['name'] == name), None)
        if not module:
            self.logger.error(f"Module {name} not found in definitions")
            return False

        if name in self.processes:
            self.stop_module(name)

        return self.start_module(module)

    # ---------------------------------------------------------
    # 🔥 NEW FUNCTION: File transfer engine
    # ---------------------------------------------------------
    def transfer_outputs(self):
        """Move files from module outputs to the next module's inbox."""
        mappings = self.config.get("pipeline_flow", {}).get("transfer_mappings", {})

        for module_name, next_inbox in mappings.items():

            # Find module conf
            module_conf = self.config.get(module_name, {})
            output_dir = module_conf.get("output")
            if not output_dir:
                continue

            output_path = Path(output_dir)
            next_inbox_path = Path(next_inbox)

            if not output_path.exists():
                continue

            for file in output_path.iterdir():
                if file.is_file() and file.name != ".gitkeep":
                    dest = next_inbox_path / file.name
                    try:
                        file.rename(dest)
                        self.logger.info(f"Transferred {file} → {dest}")
                    except Exception as e:
                        self.logger.error(f"Failed to transfer {file}: {e}")

    # ---------------------------------------------------------
    # END NEW FUNCTION
    # ---------------------------------------------------------

    def start_all_modules(self):
        self.logger.info("Starting all pipeline modules...")
        for module in self.modules:
            self.start_module(module)
            time.sleep(1)
        self.logger.info("All modules started")

    def stop_all_modules(self):
        self.logger.info("Stopping all modules...")
        for module in reversed(self.modules):
            name = module['name']
            if name in self.processes:
                self.stop_module(name)
        self.logger.info("All modules stopped")

    def monitor_modules(self):
        """Monitor health AND perform automatic file transfers."""
        while self.running:

            # Restart crashed modules
            for module in self.modules:
                name = module["name"]
                if not self.check_module_health(name):
                    self.logger.warning(f"Module {name} stopped unexpectedly")
                    self.restart_module(name)

            # NEW: Move files down the pipeline
            if self.config.get("pipeline_flow", {}).get("auto_transfer", False):
                self.transfer_outputs()

            time.sleep(2)

    def print_status(self):
        print("\n" + "=" * 80)
        print("PIPELINE STATUS")
        print("=" * 80)
        print(f"{'Module':<20} {'Status':<15} {'PID':<10} {'Description':<35}")
        print("-" * 80)

        for module in self.modules:
            name = module['name']
            desc = module['description']

            if self.check_module_health(name):
                status = "RUNNING"
                pid = self.processes[name].pid
            else:
                status = "STOPPED"
                pid = "N/A"

            print(f"{name:<20} {status:<15} {str(pid):<10} {desc:<35}")

        print("=" * 80 + "\n")

    def run(self):
        self.logger.info("Pipeline Supervisor starting...")
        self.start_all_modules()

        time.sleep(2)
        self.print_status()

        try:
            self.monitor_modules()
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")
        finally:
            self.stop_all_modules()
            self.logger.info("Pipeline Supervisor stopped")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Supervisor")
    parser.add_argument('--config', default='config/config.yaml')
    parser.add_argument('--module')
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()

    supervisor = PipelineSupervisor(config_path=args.config)

    if args.module:
        module = next((m for m in supervisor.modules if m['name'] == args.module), None)
        if module:
            supervisor.start_module(module)
            print(f"Module {args.module} started. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                supervisor.stop_module(args.module)
    elif args.status:
        supervisor.print_status()
    else:
        supervisor.run()


if __name__ == "__main__":
    main()