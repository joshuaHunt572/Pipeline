#!/usr/bin/env python3
"""
Pipeline Supervisor + Router

Starts all modules and (optionally) auto-transfers files from each module's
output folder into the next module's inbox based on config.yaml pipeline_flow.

Key fixes:
- Avoids subprocess PIPE deadlocks by redirecting stdout/stderr to per-module logs.
- Auto-transfer watches module.output, and falls back to module.final_output if output is missing.
- Prevents loops by only moving "forward" using pipeline_flow.transfer_mappings.
"""

import sys
import time
import signal
import logging
import subprocess
import shutil
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

try:
    import yaml
except ImportError:
    print("Missing dependency: pyyaml")
    print("Install with: pip install pyyaml")
    sys.exit(1)


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class PipelineSupervisor:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.processes: Dict[str, subprocess.Popen] = {}
        self.running = True

        self._setup_logging()

        # Module definitions (script paths are relative to repo root)
        self.modules = [
            {"name": "whisper",        "script": "pipeline/1_whisper/whisper.py",                 "description": "Audio to transcript conversion"},
            {"name": "extractor",      "script": "pipeline/2_extractor/extractor.py",             "description": "Fact extraction from transcripts"},
            {"name": "categorizer",    "script": "pipeline/3_categorizer/categorizer.py",         "description": "Categorization into tasks, events, notes"},
            {"name": "preprocess",     "script": "pipeline/4_preprocess/preprocess.py",          "description": "Data normalization and schema enforcement"},
            {"name": "prime",          "script": "pipeline/5_prime/prime.py",                     "description": "Analysis and actionable generation"},
            {"name": "output_engine",  "script": "pipeline/6_output_engine/output_engine.py",     "description": "Structured output formatting"},
            {"name": "synthesis",      "script": "pipeline/7_synthesis/synthesis.py",             "description": "Narrative and long-form generation"},
            {"name": "cloud_dispatch", "script": "pipeline/8_cloud_dispatch/dispatch.py",         "description": "Cloud delivery simulation"},
        ]

        # Router config
        flow = self.config.get("pipeline_flow", {}) or {}
        self.auto_transfer = bool(flow.get("auto_transfer", False))
        self.transfer_map: Dict[str, str] = (flow.get("transfer_mappings", {}) or {}).copy()

        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            return {}

    def _setup_logging(self):
        log_file = Path("pipeline_supervisor.log")
        self.logger = logging.getLogger("Supervisor")
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        # Prevent duplicate handlers if re-run in same interpreter
        if not self.logger.handlers:
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.running = False
        self.stop_all_modules()
        sys.exit(0)

    def _module_cfg(self, name: str) -> Dict:
        return self.config.get(name, {}) or {}

    def _module_stdout_log(self, name: str) -> Path:
        # Per-module supervisor capture log (separate from module's own logger)
        p = Path("logs") / f"{name}_subprocess.log"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def start_module(self, module: Dict) -> bool:
        name = module["name"]
        script = module["script"]

        if name in self.processes:
            self.logger.warning(f"Module {name} is already running")
            return False

        script_path = Path(script)
        if not script_path.exists():
            self.logger.error(f"Script not found for {name}: {script_path.resolve()}")
            return False

        try:
            self.logger.info(f"Starting module: {name} ({module['description']})")

            # Redirect stdout/stderr to file to avoid PIPE deadlocks
            out_log = self._module_stdout_log(name)
            f = open(out_log, "a", encoding="utf-8")

            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=f,
                stderr=f,
                text=True,
                cwd=str(Path.cwd())
            )

            self.processes[name] = process
            self.logger.info(f"Module {name} started with PID {process.pid} (stdout/stderr -> {out_log})")
            return True

        except Exception as e:
            self.logger.error(f"Failed to start module {name}: {e}")
            return False

    def stop_module(self, name: str) -> bool:
        if name not in self.processes:
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
            return True
        except Exception as e:
            self.logger.error(f"Failed to stop module {name}: {e}")
            return False

    def check_module_health(self, name: str) -> bool:
        if name not in self.processes:
            return False
        return self.processes[name].poll() is None

    def start_all_modules(self):
        self.logger.info("Starting all pipeline modules...")
        for module in self.modules:
            self.start_module(module)
            time.sleep(0.5)
        self.logger.info("All modules started")

    def stop_all_modules(self):
        self.logger.info("Stopping all modules...")
        for module in reversed(self.modules):
            name = module["name"]
            if name in self.processes:
                self.stop_module(name)
        self.logger.info("All modules stopped")

    # ----------------------------
    # Router (auto-transfer)
    # ----------------------------
    def _resolve_dir(self, path_str: str) -> Path:
        # config paths are relative to repo root (current working directory)
        return Path(path_str).resolve()

    def _get_source_out_dir(self, module_name: str) -> Optional[Path]:
        cfg = self._module_cfg(module_name)
        outp = cfg.get("output")
        finalp = cfg.get("final_output")
        # Prefer output, fallback to final_output (fixes output_engine, synthesis, etc.)
        chosen = outp or finalp
        if not chosen:
            return None
        p = self._resolve_dir(chosen)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _get_dest_inbox_dir(self, module_name: str) -> Optional[Path]:
        # module_name here is the NEXT module's name
        cfg = self._module_cfg(module_name)
        inbox = cfg.get("inbox")
        if not inbox:
            return None
        p = self._resolve_dir(inbox)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _safe_move(self, src: Path, dst_dir: Path):
        if not src.exists() or not src.is_file():
            return
        if src.name == ".gitkeep":
            return

        dst = dst_dir / src.name
        if dst.exists():
            # Avoid overwrite
            dst = dst_dir / f"{src.stem}_{_now_tag()}{src.suffix}"

        try:
            shutil.move(str(src), str(dst))
            self.logger.info(f"ROUTER moved: {src} -> {dst}")
        except Exception as e:
            self.logger.error(f"ROUTER failed move {src} -> {dst_dir}: {e}")

    def run_router_tick(self):
        if not self.auto_transfer:
            return
        if not self.transfer_map:
            return

        # For each mapping like: whisper -> pipeline/2_extractor/Extractor_Inbox
        for from_module, to_inbox_path in self.transfer_map.items():
            src_out = self._get_source_out_dir(from_module)
            if not src_out:
                continue

            dst_inbox = self._resolve_dir(to_inbox_path)
            dst_inbox.mkdir(parents=True, exist_ok=True)

            # Move JSON files forward (your pipeline artifacts are JSON)
            for f in src_out.iterdir():
                if f.is_file() and f.suffix.lower() == ".json":
                    self._safe_move(f, dst_inbox)

    def monitor_modules(self):
        self.logger.info(f"Auto-transfer is {'ON' if self.auto_transfer else 'OFF'}")
        while self.running:
            # Health check + restart
            for module in self.modules:
                name = module["name"]
                if not self.check_module_health(name):
                    if name in self.processes:
                        rc = self.processes[name].poll()
                        self.logger.error(f"Module {name} crashed (return code {rc}). Restarting...")
                        del self.processes[name]
                        self.start_module(module)

            # Router tick
            self.run_router_tick()

            time.sleep(2)

    def print_status(self):
        print("\n" + "=" * 80)
        print("PIPELINE STATUS")
        print("=" * 80)
        print(f"{'Module':<20} {'Status':<15} {'PID':<10} {'Description':<35}")
        print("-" * 80)

        for module in self.modules:
            name = module["name"]
            desc = module["description"]
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
        time.sleep(1)
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

    parser = argparse.ArgumentParser(description="Pipeline Supervisor + Router")
    parser.add_argument("--config", default="config/config.yaml", help="Path to configuration file")
    parser.add_argument("--status", action="store_true", help="Print status and exit")
    args = parser.parse_args()

    supervisor = PipelineSupervisor(config_path=args.config)

    if args.status:
        supervisor.print_status()
        return

    supervisor.run()


if __name__ == "__main__":
    main()