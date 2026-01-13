# Financial reconciliation automation (AI-assisted)

## Objective
Automate reconciliation between ledger and subledger exports using deterministic rules first, with AI used only for advisory suggestions.

## Inputs (v1)
- data/raw/ledger.csv
- data/raw/subledger.csv

## Outputs (v1)
- outputs/matched.csv
- outputs/unmatched_ledger.csv
- outputs/unmatched_subledger.csv
- outputs/exceptions.csv
- outputs/recon_summary.json

## Principles
- Deterministic matching is the system of record.
- AI never auto-matches; it only proposes suggestions with reasons + confidence.
- Every decision is traceable and audit-friendly.

## Matching approach (v1)
1. Standardize data (dates, amounts, text normalization)
2. Exact match: date + amount + normalized description (with optional date window)
3. Tolerance match: amount tolerance + high text similarity
4. AI suggestions for remaining unmatched items

