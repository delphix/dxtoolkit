from analytic_io_obj import AnalyticIOObj


class AnalyticNFSObj(AnalyticIOObj):
    """NFS analytics share the IO implementation; this stub keeps type mapping simple."""
    def __init__(self, dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug=None):
        super().__init__(dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug)
