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

        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Formatter
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

            # Wait for graceful shutdown
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

        process = self.processes[name]
        return process.poll() is None

    def restart_module(self, name: str) -> bool:
        """Restart a module"""
        self.logger.info(f"Restarting module: {name}")

        # Find module definition
        module = next((m for m in self.modules if m['name'] == name), None)
        if not module:
            self.logger.error(f"Module {name} not found in definitions")
            return False

        # Stop if running
        if name in self.processes:
            self.stop_module(name)

        # Start again
        return self.start_module(module)

    def start_all_modules(self):
        """Start all modules in sequence"""
        self.logger.info("Starting all pipeline modules...")

        for module in self.modules:
            self.start_module(module)
            time.sleep(1)  # Brief delay between starts

        self.logger.info("All modules started")

    def stop_all_modules(self):
        """Stop all running modules"""
        self.logger.info("Stopping all modules...")

        # Stop in reverse order
        for module in reversed(self.modules):
            name = module['name']
            if name in self.processes:
                self.stop_module(name)

        self.logger.info("All modules stopped")

    def monitor_modules(self):
        """Monitor module health and restart if needed"""
        while self.running:
            for module in self.modules:
                name = module['name']

                if not self.check_module_health(name):
                    self.logger.warning(f"Module {name} is not running!")

                    if name in self.processes:
                        # Process crashed
                        process = self.processes[name]
                        returncode = process.poll()
                        self.logger.error(f"Module {name} crashed with return code {returncode}")
                        del self.processes[name]

                        # Attempt restart
                        self.logger.info(f"Attempting to restart {name}...")
                        self.start_module(module)

            time.sleep(10)  # Check every 10 seconds

    def print_status(self):
        """Print status of all modules"""
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
        """Main supervisor loop"""
        self.logger.info("Pipeline Supervisor starting...")

        # Start all modules
        self.start_all_modules()

        # Print initial status
        time.sleep(2)
        self.print_status()

        try:
            # Monitor modules
            self.monitor_modules()

        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")

        finally:
            self.stop_all_modules()
            self.logger.info("Pipeline Supervisor stopped")


def main():
    """Entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Supervisor")
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--module',
        help='Start only a specific module'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Print status and exit'
    )

    args = parser.parse_args()

    supervisor = PipelineSupervisor(config_path=args.config)

    if args.module:
        # Start only specified module
        module = next((m for m in supervisor.modules if m['name'] == args.module), None)
        if module:
            supervisor.start_module(module)
            print(f"Module {args.module} started. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                supervisor.stop_module(args.module)
        else:
            print(f"Module {args.module} not found")
            print(f"Available modules: {', '.join(m['name'] for m in supervisor.modules)}")

    elif args.status:
        # Just print status
        supervisor.print_status()

    else:
        # Run full supervisor
        supervisor.run()


if __name__ == "__main__":
    main()
