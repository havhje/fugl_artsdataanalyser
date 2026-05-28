---
description: Run the databehandling notebook CLI
argument-hint: "<INPUT_FIL_STI> [--filter-year YEAR] [--output OUTPUT]"
---
Run the databehandling notebook CLI for this project.

Usage:

```text
/databehandling <INPUT_FIL_STI> [--filter-year YEAR] [--output OUTPUT]
```

Possible arguments:

- `INPUT_FIL_STI` — required path to the input CSV file with bird data.
- `--filter-year YEAR` — optional integer; keep observations from this year onward. Default: `1990`.
- `--output OUTPUT` — optional output Parquet path. Default: `output.parquet`.
- `--help` or `-h` — show this usage and do not run anything.

Interactive missing-name workflow:

The databehandling CLI may discover species with missing Norwegian names. It prints the full missing-species table before asking for names interactively. Pi cannot answer interactive terminal prompts after the command has started, so the first run may fail when the first name prompt is reached.

If this happens, show the user the missing-species list and ask for Norwegian names. Tell the user they can provide the names in either of these formats:

1. Preferred, order-independent mapping/list. Any clear format is OK as long as each entry contains both the scientific/Latin species name and the Norwegian species name:

```text
Latin species name = Norwegian name
Latin species name: Norwegian name
- Latin species name — Norwegian name
```

Order does not matter for this format because Pi can match the Norwegian name to the scientific name.

2. Simple ordered list, one Norwegian name per line, in exactly the same order as the prompted missing-species list:

```text
Norwegian name 1
Norwegian name 2
Norwegian name 3
```

The exact order is only required when the user provides Norwegian names without the scientific/Latin species names. If the user includes both the scientific/Latin name and the Norwegian name for each species, accept the list even if the order or formatting is not exact. If the response is ambiguous, ask for clarification before rerunning.

User-provided arguments:

```text
$ARGUMENTS
```

Instructions for the agent:

1. If no arguments were provided, or if the arguments are `--help`/`-h`, list the usage and possible arguments above without running the command.
2. Parse either normal CLI-style arguments or natural-language arguments.
   - CLI-style examples: `data/fugl.csv --filter-year 1995 --output ./output.parquet`
   - Natural-language examples: `data/fugl.csv filter after 1995 output in current working directory`, `use data/fugl.csv from 2000 and write to results/fugl.parquet`
3. Extract:
   - Input CSV path: required. If no plausible CSV/input path is provided, ask for it and show the usage without running anything.
   - Filter year: optional. Understand phrases such as `filter after 1995`, `from 1995`, `fra 1995`, `etter 1995`, or explicit `--filter-year 1995`; convert to `--filter-year 1995`.
   - Output: optional. Understand explicit `--output PATH`, `output PATH`, `write to PATH`, `skriv til PATH`, and output-folder phrases. If the user asks for the output folder to be the current working directory, use `./output.parquet`. If the user provides a directory rather than a filename, use `DIRECTORY/output.parquet`.
4. Validate that the filter year is an integer and that the final output path ends with `.parquet`. If the arguments are invalid or ambiguous, explain the problem and show the usage without running the command.
5. If valid, run the databehandling CLI from the project root with shell-safe quoting:

```bash
uv run python databehandling/databehandling.py INPUT_FIL_STI [--filter-year YEAR] [--output OUTPUT]
```

6. Report the command that was run, the exit status, and any important stdout/stderr output.
7. If the command fails because it reached the interactive missing-name prompt:
   - Extract and show the missing-species list from stdout/stderr.
   - Ask the user for Norwegian names using the formats described in the "Interactive missing-name workflow" section.
   - Remind the user that a plain list of names must be in the exact same order as the prompted species list.
   - After the user provides names, build a newline-separated stdin payload in the missing-species prompt order and rerun the same command with input piped in, e.g.:

```bash
printf '%s\n' 'Norwegian name 1' 'Norwegian name 2' 'Norwegian name 3' | uv run python databehandling/databehandling.py INPUT_FIL_STI [--filter-year YEAR] [--output OUTPUT]
```

   - Use shell-safe quoting for both the command arguments and the piped names.
