"""
Alert engine and rules package.

Public API is unchanged: import AlertRule, AlertEngine, and build_default_alert_engine
from this package exactly as before.
"""
from __future__ import annotations

from medwarehouse.platform.services.alerts._rules import AlertRule
from medwarehouse.platform.services.alerts._engine import AlertEngine, VolumeBaselineStore
from medwarehouse.platform.services.alerts.inventory import (
    FreshnessAlertRule,
    VolumeAnomalyRule,
    QuarantineAlertRule,
    PipelineConsistencyRule,
)
from medwarehouse.platform.services.alerts.infra import (
    InfraServiceRule,
    WarehouseReachabilityRule,
)
from medwarehouse.platform.services.alerts.airflow import (
    AirflowFailureRule,
    AirflowStalenessRule,
)
from medwarehouse.platform.services.alerts.compliance import (
    ControlledSubstanceComplianceRule,
    SupplierLicenseExpiryRule,
)


def build_default_alert_engine() -> AlertEngine:
    baseline_store = VolumeBaselineStore(window_size=6)
    rules: list[AlertRule] = [
        # Inventory freshness (legacy path: metrics.pipeline.<stage>)
        FreshnessAlertRule(stage="bronze",    label="Bronze",    expected_seconds=600.0),
        FreshnessAlertRule(stage="silver",    label="Silver",    expected_seconds=1800.0),
        FreshnessAlertRule(stage="warehouse", label="Warehouse", expected_seconds=3600.0),
        # Procurement domain freshness (new path: metrics.pipeline.domains.procurement.<stage>)
        FreshnessAlertRule(domain="procurement", stage="bronze", label="Procurement Bronze", expected_seconds=600.0),
        FreshnessAlertRule(domain="procurement", stage="silver", label="Procurement Silver", expected_seconds=1800.0),
        # Sales domain freshness
        FreshnessAlertRule(domain="sales", stage="bronze", label="Sales Bronze", expected_seconds=600.0),
        FreshnessAlertRule(domain="sales", stage="silver", label="Sales Silver", expected_seconds=1800.0),
        # Volume anomaly detection (inventory only — uses the legacy metrics path)
        VolumeAnomalyRule(stage="bronze", label="Bronze", baseline_store=baseline_store),
        VolumeAnomalyRule(stage="silver", label="Silver", baseline_store=baseline_store),
        QuarantineAlertRule(warning_threshold=1, critical_threshold=5),
        # Infrastructure
        WarehouseReachabilityRule(),
        InfraServiceRule(service_name="kafka",    label="Kafka"),
        InfraServiceRule(service_name="postgres", label="PostgreSQL"),
        PipelineConsistencyRule(),
        # Airflow
        AirflowFailureRule(),
        AirflowStalenessRule(threshold_seconds=86400.0),
        # Compliance
        SupplierLicenseExpiryRule(),
        ControlledSubstanceComplianceRule(),
    ]
    return AlertEngine(rules)


__all__ = [
    "AlertEngine",
    "AlertRule",
    "AirflowFailureRule",
    "AirflowStalenessRule",
    "ControlledSubstanceComplianceRule",
    "FreshnessAlertRule",
    "InfraServiceRule",
    "PipelineConsistencyRule",
    "QuarantineAlertRule",
    "SupplierLicenseExpiryRule",
    "VolumeAnomalyRule",
    "VolumeBaselineStore",
    "WarehouseReachabilityRule",
    "build_default_alert_engine",
]
