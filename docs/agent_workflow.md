# Agent Workflow: Financial Reconciliation

## Purpose
This document describes the step-by-step workflow for an AI-assisted financial reconciliation agent.

## Step 1: Ingest
- Load general ledger or transaction extracts (CSV)
- Normalize column names and data types

## Step 2: Validate
- Check for missing required fields
- Identify invalid dates or amounts
- Flag potential duplicates

## Step 3: Detect Imbalances
- Aggregate by fund and accounting period
- Calculate net balances
- Flag variances beyond defined thresholds

## Step 4: Explain Exceptions
- Identify top contributing transactions
- Compare against prior periods
- Provide structured explanation hints

## Step 5: Recommend Actions
- Suggest potential journal entries
- Highlight likely mapping or timing issues
- Identify required follow-up

## Step 6: Output
- Produce structured JSON output
- Preserve audit trail for review

