#!/bin/bash
###############################################################################
# Syncsort ETL Shell Script
# Description: Sample ETL script for data extraction, transformation, and loading
# Author: Auto-generated
# Date: 2025-11-04
###############################################################################

# Set environment variables
export SYNCSORT_HOME=/opt/syncsort
export DMX_HOME=${SYNCSORT_HOME}/dmx
export PATH=${DMX_HOME}/bin:${PATH}

# Configuration
LOG_DIR="./logs"
DATA_DIR="./data"
ERROR_LOG="${LOG_DIR}/etl_error_$(date +%Y%m%d_%H%M%S).log"
RUN_LOG="${LOG_DIR}/etl_run_$(date +%Y%m%d_%H%M%S).log"

# Create directories if they don't exist
mkdir -p ${LOG_DIR}
mkdir -p ${DATA_DIR}

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${RUN_LOG}
}

# Function to handle errors
handle_error() {
    echo "[ERROR] [$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a ${ERROR_LOG}
    exit 1
}

# Start ETL process
log_message "Starting ETL process..."

# Step 1: Data Extraction
log_message "Step 1: Extracting source data..."
# Example: Extract data from source (placeholder)
if [ -f "${DATA_DIR}/source_data.csv" ]; then
    log_message "Source data found: ${DATA_DIR}/source_data.csv"
else
    log_message "Warning: Source data not found. Creating placeholder."
    echo "id,name,value" > ${DATA_DIR}/source_data.csv
    echo "1,Sample,100" >> ${DATA_DIR}/source_data.csv
fi

# Step 2: Data Transformation using Syncsort DMX
log_message "Step 2: Transforming data with Syncsort DMX..."

# Define the Syncsort DMX job file path
DMX_JOB="./.sync/sample_transform.sync"

if [ -f "${DMX_JOB}" ]; then
    log_message "Executing Syncsort DMX job: ${DMX_JOB}"
    
    # Execute DMX job using dmxcli with parameters
    # Syntax: dmxcli -f <job_file> -p <parameters> -l <log_level>
    dmxcli -f "${DMX_JOB}" \
           -p "INPUT_DIR=${DATA_DIR}/input" \
           -p "OUTPUT_DIR=${DATA_DIR}/output" \
           -l INFO \
           >> ${RUN_LOG} 2>> ${ERROR_LOG}
    
    if [ $? -eq 0 ]; then
        log_message "DMX transformation completed successfully"
    else
        handle_error "DMX transformation failed. Check error log: ${ERROR_LOG}"
    fi
else
    log_message "DMX job file not found. Skipping transformation step."
fi

# Step 3: Data Loading
log_message "Step 3: Loading transformed data..."

# Example: Load data to target location
TARGET_DIR="${DATA_DIR}/output"
mkdir -p ${TARGET_DIR}

if [ -f "${DATA_DIR}/transformed_data.txt" ]; then
    cp ${DATA_DIR}/transformed_data.txt ${TARGET_DIR}/
    
    if [ $? -eq 0 ]; then
        log_message "Data loaded successfully to ${TARGET_DIR}"
    else
        handle_error "Failed to load data to target directory"
    fi
else
    log_message "Transformed data file not found. Nothing to load."
fi

# Step 4: Data Quality Checks
log_message "Step 4: Performing data quality checks..."

# Example quality check: count records
if [ -f "${TARGET_DIR}/transformed_data.txt" ]; then
    RECORD_COUNT=$(wc -l < ${TARGET_DIR}/transformed_data.txt)
    log_message "Total records processed: ${RECORD_COUNT}"
    
    if [ ${RECORD_COUNT} -gt 0 ]; then
        log_message "Quality check passed: Data contains records"
    else
        log_message "Warning: No records found in output file"
    fi
fi

# Cleanup temporary files
log_message "Cleaning up temporary files..."
# Add cleanup commands here as needed

# ETL process completed
log_message "ETL process completed successfully!"
log_message "Run log: ${RUN_LOG}"
log_message "Error log: ${ERROR_LOG}"

exit 0
