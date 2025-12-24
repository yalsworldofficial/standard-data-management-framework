class FeedSpec:
    """Lightweight feed spec container (assumed pre-validated)"""

    def __init__(self, spec: dict):
        self.spec = spec
        self.primary_key = spec.get("primary_key")
        self.composite_key = spec.get("composite_key", [])
        self.partition_keys = spec.get("partition_keys", [])
        self.standard_checks = spec.get("standard_checks", [])
        self.comprehensive_checks = spec.get("comprehensive_checks", [])
