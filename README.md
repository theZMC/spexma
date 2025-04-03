# spexma - Splunk Export Massager

A command-line tool to split Splunk CSV exports by sourcetype.

## Overview

`spexma` (Splunk Export Massager) is a Go application that takes data exported from Splunk as a CSV file, identifies unique sourcetypes, and creates separate CSV files for each sourcetype. The application is designed to handle large CSV files efficiently using buffered I/O, goroutines, and channels.

## Features

- Split Splunk export CSV files by sourcetype
- Efficient processing with buffered I/O for large files
- Use of goroutines and channels for parallel processing
- Live terminal display showing record counts per sourcetype
- Pretty terminal output using lipgloss

## Installation

```bash
go install github.com/thezmc/spexma/cmd/spexma@latest
```

Or clone the repository and build:

```bash
git clone https://github.com/thezmc/spexma.git
cd spexma
go build -o spexma ./cmd/spexma
```

## Usage

The application provides different subcommands for various operations:

```bash
spexma [command] [flags]
```

### Available Commands

- `split`: Split a Splunk CSV export by sourcetype
- `help`: Help about any command

### Global Flags

- `--help`: Show help for any command
- `--version`: Print the version number

### Split Command

The split command divides a Splunk CSV export into separate files by sourcetype:

```bash
spexma split [flags]
```

#### Flags

- `-i, --input-file string`: Input CSV file (required)
- `-o, --output-directory string`: Output directory for the split CSV files (defaults to same directory as input)
- `-c, --column string`: Column name for sourcetype (default "sourcetype")
- `-h, --help`: Help for split command

### Examples

```bash
# Split a CSV file using the default output directory
spexma split -i export.csv

# Split a CSV file and specify an output directory
spexma split -i export.csv -o ./splunk_data
```

This will read `export.csv` and create a new CSV file for each sourcetype found (e.g., `windowseventlog.csv`, `sysmon.csv`, etc.) in the same directory as the input file.

## Output

The application displays a real-time table of sourcetypes and record counts:

```
Splunk Export Massager (spexma)

Sourcetype                               Records
--------------------------------------------------------
windowseventlog                          13631
sysmon                                   32424
...
```

After processing, a summary is shown with total records per sourcetype.

## Implementation Details

- Uses a reader goroutine to process the input CSV
- Creates a writer goroutine for each unique sourcetype
- Uses channels to pass records between goroutines
- Uses buffered I/O for efficient reading and writing
- Updates display in real-time using ANSI terminal control sequences
- Gracefully handles errors and cleanup

## License

MIT
