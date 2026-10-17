"""
NWB Export pipeline sub-package.

Provides:
- errors    : custom exception hierarchy (NwbExportError and subclasses)
- config    : pipeline constants (retry policy, retention limits, required fields)
- state_machine : allowed status transitions and transition guard

All database operations live in the parent u19_pipeline.nwb_production / nwb_export_enums
modules per Constitution Principle I (DataJoint-First) and Principle III (Structural Reuse).
"""
