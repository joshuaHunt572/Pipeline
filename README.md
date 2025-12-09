# Multi-Module File-Based Processing Pipeline

A deterministic, file-based processing pipeline with 8 independent modules that process data through a complete workflow from audio transcription to cloud delivery.

## Overview

This pipeline is designed as a strictly-bounded, deterministic system where each module:
- Polls its inbox directory for new files
- Processes files one at a time
- Outputs predictable JSON structures
- Archives processed files
- Logs all operations locally

**No external APIs. No network calls. Pure file-based processing.**

## Pipeline Flow

```
1. Whisper       → Audio files to transcripts
2. Extractor     → Transcripts to facts and unknowns
3. Categorizer   → Facts to tasks, events, notes
4. Preprocess    → Normalize and validate data
5. Prime         → Generate analysis and actionables
6. Output Engine → Format into structured outputs
7. Synthesis     → Create narrative and summaries
8. Cloud Dispatch → Move to cloud storage (simulated)
```

## Directory Structure

```
/pipeline
    /1_whisper
        Whisper_Inbox/          # Drop audio files here
        Whisper_Output/         # Transcript JSON output
        whisper.py
    /2_extractor
        Extractor_Inbox/        # Receives transcripts
        Extractor_Output/       # Extracted facts JSON
        extractor.py
    /3_categorizer
        Categorizer_Inbox/      # Receives extracted data
        Categorizer_Output/     # Categorized JSON
        categorizer.py
    /4_preprocess
        Preprocess_Inbox/       # Receives preprocessed data
        Preprocessing_Output/   # Normalized JSON
        preprocess.py
    /5_prime
        Prime_Inbox/            # Receives preprocessed data
        Prime_Output/           # Analysis JSON
        Final_Output/prime/     # Final prime outputs
        prime.py
    /6_output_engine
        Output_Inbox/           # Receives primed data
        Final_Output/           # Structured outputs (tables, lists, etc.)
        output_engine.py
    /7_synthesis
        Synthesis_Inbox/        # Receives primed data
        Synthesized_Output/     # Synthesis JSON
        Final_Output/deliverables/  # Narratives, summaries
        synthesis.py
    /8_cloud_dispatch
        Dispatch_Inbox/         # Receives final deliverables
        Cloud_Results/          # Simulated cloud storage
        dispatch.py

/config
    config.yaml                 # Global configuration

/run.py                         # Supervisor script
/README.md                      # This file
```

## Installation

### Prerequisites

- Python 3.11 or higher
- pip

### Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `pydantic` - Data validation and schema enforcement
- `pyyaml` - YAML configuration parsing

## Running the Pipeline

### Option 1: Run All Modules (Recommended)

Start all modules with the supervisor:

```bash
python run.py
```

The supervisor will:
- Start all 8 modules as background processes
- Monitor their health
- Automatically restart crashed modules
- Display status information

### Option 2: Run Individual Modules

Start a single module:

```bash
python run.py --module whisper
```

Available modules:
- `whisper`
- `extractor`
- `categorizer`
- `preprocess`
- `prime`
- `output_engine`
- `synthesis`
- `cloud_dispatch`

### Option 3: Manual Module Execution

Run modules directly:

```bash
python pipeline/1_whisper/whisper.py
```

### Check Status

View pipeline status:

```bash
python run.py --status
```

## Module Behavior

### 1. Whisper Module

**Input:** Audio files (`.mp3`, `.wav`, `.m4a`, `.flac`, `.ogg`, `.aac`)

**Output:** JSON with transcript and metadata

```json
{
  "transcript": "...",
  "metadata": {
    "source_file": "audio.mp3",
    "timestamp": "2025-01-01T12:00:00",
    "file_size": 1024000,
    "processing_module": "whisper",
    "version": "1.0"
  }
}
```

**Note:** Currently uses placeholder transcription. Replace `process_audio_file()` with actual Whisper model integration.

### 2. Extractor Module

**Input:** Transcript JSON from Whisper

**Output:** Facts and unknown items

```json
{
  "facts": ["fact 1", "fact 2", ...],
  "unknown": ["unclear item 1", ...],
  "metadata": {
    "timestamp": "...",
    "processing_module": "extractor",
    "facts_count": 3,
    "unknown_count": 1
  }
}
```

**Note:** Replace `extract_facts()` with actual LLM-based extraction logic.

### 3. Categorizer Module

**Input:** Extracted facts JSON

**Output:** Categorized tasks, events, notes

```json
{
  "tasks": [
    {
      "task": "action item",
      "priority": "high",
      "status": "pending"
    }
  ],
  "events": [
    {
      "event": "meeting",
      "date": "2025-01-15",
      "type": "meeting"
    }
  ],
  "notes": ["general note", ...],
  "metadata": { ... }
}
```

**Note:** Replace `categorize_facts()` with actual LLM-based categorization.

### 4. Preprocess Module

**Input:** Categorized JSON

**Output:** Normalized and validated data

- Enforces schemas using Pydantic models
- Validates field types and formats
- Reports validation errors in metadata

### 5. Prime Module

**Input:** Preprocessed JSON

**Output:** Analysis and actionable items

```json
{
  "analysis": "detailed analysis text...",
  "actionable": [
    {
      "action": "specific action",
      "source": "tasks",
      "priority": "high",
      "deadline": "TBD"
    }
  ],
  "context": {
    "total_tasks": 5,
    "total_events": 2,
    "priority_breakdown": { ... }
  },
  "metadata": { ... }
}
```

**Note:** Replace `generate_analysis()` with actual LLM-based analysis.

### 6. Output Engine Module

**Input:** Primed JSON

**Output:** Structured text files

Generates:
- `*_table.txt` - Tabular format
- `*_list.txt` - Bulleted list format
- `*_summary.txt` - Comprehensive summary
- `*_data.json` - Original JSON

Creates organized directories: `Final_Output/structured_*`

### 7. Synthesis Module

**Input:** Primed JSON

**Output:** Long-form deliverables

Generates:
- `*_narrative.txt` - Full narrative document
- `*_executive_summary.txt` - Executive summary
- `*_recommendations.txt` - Actionable recommendations
- `*_synthesized.json` - Complete synthesis data

**Note:** Replace `generate_narrative()` with actual LLM-based synthesis.

### 8. Cloud Dispatch Module

**Input:** Any file type

**Output:** Cloud storage simulation

- Organizes files by date
- Creates dispatch manifest (`dispatch_manifest.jsonl`)
- Simulates cloud upload (copies to `Cloud_Results/`)
- Tracks file checksums and metadata

## Testing with Dummy Data

### Test Module 1 (Whisper)

```bash
# Create a dummy audio file
touch pipeline/1_whisper/Whisper_Inbox/test_audio.mp3

# Watch the output
tail -f pipeline/1_whisper/whisper.log
```

### Test Module 2 (Extractor)

```bash
# Create dummy transcript JSON
cat > pipeline/2_extractor/Extractor_Inbox/test_transcript.json << 'EOF'
{
  "transcript": "This is a test transcript with important facts.",
  "metadata": {
    "source_file": "test_audio.mp3",
    "timestamp": "2025-01-01T12:00:00"
  }
}
EOF

# Watch the output
tail -f pipeline/2_extractor/extractor.log
```

### Test Complete Pipeline

```bash
# Start all modules
python run.py

# In another terminal, drop a test audio file
touch pipeline/1_whisper/Whisper_Inbox/test.mp3

# Manually transfer outputs between modules to test flow
# Or set auto_transfer: true in config.yaml
```

## Expected JSON I/O Patterns

### Module Input/Output Chain

1. **Whisper:** Audio → `{transcript, metadata}`
2. **Extractor:** Transcript → `{facts[], unknown[], metadata}`
3. **Categorizer:** Facts → `{tasks[], events[], notes[], metadata}`
4. **Preprocess:** Categorized → Normalized `{tasks[], events[], notes[], metadata}`
5. **Prime:** Normalized → `{analysis, actionable[], context, metadata}`
6. **Output Engine:** Prime → Structured text files
7. **Synthesis:** Prime → `{narrative, executive_summary, recommendations[], metadata}`
8. **Cloud Dispatch:** Files → Cloud storage + manifest

### Metadata Propagation

Each module adds its own metadata while preserving source metadata:

```json
{
  "metadata": {
    "timestamp": "current processing time",
    "processing_module": "current module name",
    "version": "module version",
    "source_file": "original input file",
    "source_metadata": { /* previous module's metadata */ }
  }
}
```

## Extending or Replacing Logic

### Replace Placeholder Functions

Each module has clearly marked placeholder functions:

**Whisper:** `process_audio_file()` - Replace with Whisper API or model

**Extractor:** `extract_facts()` - Replace with LLM-based extraction

**Categorizer:** `categorize_facts()` - Replace with LLM-based categorization

**Prime:** `generate_analysis()` - Replace with LLM-based analysis

**Synthesis:** `generate_narrative()` - Replace with LLM-based synthesis

### Example: Integrating OpenAI

```python
# In extractor.py, replace extract_facts():

import openai

def extract_facts(self, transcript: str) -> ExtractorOutput:
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "Extract facts from this transcript."},
            {"role": "user", "content": transcript}
        ]
    )

    # Parse response and return ExtractorOutput
    ...
```

### Adding New Fields

1. Update Pydantic model in relevant module
2. Update processing logic
3. Downstream modules automatically receive new fields in `source_metadata`

## Configuration

Edit `config/config.yaml` to customize:

- **Poll intervals:** How often modules check for new files
- **Paths:** Custom directory locations
- **Log levels:** DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Output formats:** Enable/disable specific outputs
- **Auto-transfer:** Automatically move files to next module's inbox

## Logging

Each module logs to its own file:

- `pipeline/1_whisper/whisper.log`
- `pipeline/2_extractor/extractor.log`
- etc.

Supervisor logs to: `pipeline_supervisor.log`

## Error Handling

- Invalid JSON files are logged and skipped
- Failed processing leaves file in inbox for retry
- Validation errors are logged in metadata
- Supervisor automatically restarts crashed modules

## Architecture Principles

1. **Strict module boundaries** - No cross-module dependencies
2. **File-based communication** - No network, no APIs
3. **Deterministic behavior** - Same input = same output
4. **Self-contained modules** - Each module is runnable independently
5. **Placeholder logic** - Easy to swap with real implementations
6. **Atomic operations** - Temp files + atomic rename for safety
7. **Archive strategy** - Processed files moved to archive, not deleted

## Troubleshooting

### Module Not Processing Files

1. Check if module is running: `python run.py --status`
2. Check log file for errors
3. Verify file is in correct inbox directory
4. Check file permissions

### Files Stuck in Inbox

1. Check log file for processing errors
2. Verify JSON format is valid
3. Check file isn't locked by another process

### Supervisor Won't Start

1. Verify Python version: `python --version` (3.11+)
2. Check dependencies: `pip install -r requirements.txt`
3. Verify config file exists: `config/config.yaml`

## Development

### Running Tests

```bash
# Test individual module
python pipeline/1_whisper/whisper.py

# Test with dummy data
touch pipeline/1_whisper/Whisper_Inbox/test.mp3
```

### Adding a New Module

1. Create directory: `pipeline/9_newmodule/`
2. Create inbox/output directories
3. Copy template from existing module
4. Update `config.yaml` with new module settings
5. Add to `run.py` modules list
6. Update this README

## License

This is a pipeline implementation framework. Adapt as needed.

## Support

For issues or questions:
1. Check logs in respective module directories
2. Verify configuration in `config/config.yaml`
3. Review this README for expected behavior
