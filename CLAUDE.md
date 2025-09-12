# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## System Overview

This is an RMS (Research Management System) data processing system for importing tag data from Excel files into MySQL database and synchronizing with OA (Office Automation) system. The system handles three types of data:

1. **Contracts** (合同签订清单) - Contract signing records
2. **Project Funds** (经费到账清单) - Fund arrival records  
3. **Transactions** (收支明细表) - Financial transaction details

## Core Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Test configuration and environment
python test_config.py
```

### Main Processing Commands

#### GUI Interface (Recommended)
```bash
# Launch GUI interface for user-friendly operation
python run_gui.py
# or
python gui_main.py
```

#### Command Line Interface
```bash
# Process contracts and project funds
python run_processor.py

# Process contracts and project funds with environment check only
python run_processor.py --check

# Process transactions from folder
python run_transactions_processor.py

# Process transactions with specific folder path
python run_transactions_processor.py --folder /path/to/folder

# Process transactions with verbose output
python run_transactions_processor.py --verbose

# Process transactions without OA sync
python run_transactions_processor.py --no-oa-sync
```

### Testing Commands
```bash
# Simple system test
python simple_test.py

# Full workflow test
python test_full_workflow.py

# Full process test (contracts & funds)
python test_full_process.py

# Transactions processor test
python test_transactions_processor.py

# Fund IDs cleaning test
python test_fundids_cleaning.py
```

## Architecture Overview

The system follows a modular architecture with clear separation of concerns:

### Core Modules

- **config.py**: Centralized configuration management with field mappings, data types, and business rules
- **main_processor.py**: Main orchestrator for contracts and project funds processing with folder support
- **transactions_processor.py**: Specialized processor for financial transaction records with folder support
- **data_cleaner.py**: Data cleaning and validation utilities
- **database_manager.py**: MySQL database operations with connection pooling
- **oa_sync_manager.py**: OA system integration and synchronization
- **logger_utils.py**: Comprehensive logging system with daily log files
- **gui_main.py**: Tkinter-based GUI interface for user-friendly operation
- **run_gui.py**: GUI launcher script

### Data Flow Architecture

1. **Excel Reading**: Handles .xls files with specific sheet structures and field mappings
2. **Data Cleaning**: Type conversion, null handling, and business rule validation
3. **Database Operations**: Upsert operations with primary key conflict resolution
4. **OA Synchronization**: Async REST API calls with retry mechanisms and master-sub table support

### Key Design Patterns

- **Configuration-driven**: All field mappings, data types, and business rules are externalized in config.py
- **Folder Processing**: Support for processing entire directories of Excel files automatically
- **GUI Interface**: User-friendly Tkinter interface for directory selection and real-time processing monitoring
- **Async Processing**: OA synchronization uses aiohttp for non-blocking operations
- **Batch Processing**: Database operations use configurable batch sizes for performance
- **Error Resilience**: Comprehensive error handling with detailed logging and retry mechanisms

## Database Schema

### Key Tables
- **contracts**: Contract records with contractid as primary key
- **projectfunds**: Fund records with compound key (fundid + funds_received + contractid)
- **transactions**: Transaction records with compound key (fundid + transactiondate + vouchernumber)

## OA Integration

The system supports two sync modes:
- **Simple sync**: Each record maps to single OA form
- **Master-sub table sync**: Records grouped by fundid, first record as master, rest as sub-records

Forms used:
- Contracts: formmain_0016 (hetongdangan)
- Project funds: formmain_0017 (jingfeidaozhangqingdan)  
- Transactions: formmain_0018/formson_0019 (shouzhimingxi)

## Configuration Notes

All configuration is centralized in config.py including:
- File paths for Excel files
- MySQL connection settings
- OA system endpoints and credentials
- Field mappings between Chinese/English column names
- Data type converters and business filtering rules
- Runtime settings (batch sizes, thread pools, feature flags)

## Special Processing Logic

### Excel Parsing
- **Contracts/Funds**: Standard tabular format with header row
- **Transactions**: Complex format with fund card info in row 2 (C2-H2), field names in row 3, data starting row 5

### Business Filtering
- Contracts: Filter by unit ("工程技术研究院", "冶金工程研究院") or fund ID prefixes (3932, 3832, 3934)
- Project funds: Filter by unit and require non-null fund account, received amount, and project ID
- Transactions: Parse fund card number and project info from merged cells

## Logging System

- Daily log files: `logs/rms_tag_on_YYYYMMDD.log`
- Structured logging with operation tracking, statistics, and error details
- Configurable log levels with rotation and size limits
- GUI provides real-time log display with color-coded severity levels

## GUI Interface Features

The new GUI interface (`gui_main.py`) provides:

### User Interface Components
- **Directory Selection**: Browse and select folders for contracts/funds and transactions
- **Processing Options**: Toggle contracts, transactions, and OA sync independently  
- **Real-time Logging**: Color-coded log display with auto-scrolling
- **Progress Monitoring**: Status bar and processing controls (start/stop)
- **Environment Testing**: Built-in database and OA connection testing

### Folder Processing Logic
- **Smart File Detection**: Automatically identifies file types by filename patterns
- **Batch Processing**: Processes all Excel files in specified directories
- **Error Handling**: Continues processing remaining files even if some fail
- **Detailed Reporting**: Shows per-file and summary statistics

### Usage Workflow
1. Launch GUI: `python run_gui.py`
2. Select directories for contracts/funds and transactions
3. Choose processing options (contracts, transactions, OA sync)
4. Click "开始处理" to start batch processing
5. Monitor real-time progress in log window
6. View final statistics and error reports