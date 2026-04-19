#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging

try:
    from prometheus_client import REGISTRY
    from prometheus_client.exposition import write_to_textfile
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

_logger = logging.getLogger(__name__)


class TextfileExporter:
    """Writes Prometheus metrics to a .prom file for node_exporter textfile collector.

    No dedicated thread — write() is called by the standalone worker loop
    at the same cadence as update_metrics().
    Uses prometheus_client.write_to_textfile() which handles atomic writes.
    """

    def __init__(self, directory, worker_name):
        self._output_path = os.path.join(directory, f'imq_{worker_name}.prom')
        self._directory = directory

    def start(self):
        if not HAS_PROMETHEUS:
            _logger.warning("prometheus_client not available, textfile export disabled")
            return
        os.makedirs(self._directory, exist_ok=True)
        _logger.info(f"TextfileExporter: writing metrics to {self._output_path}")

    def write(self):
        if not HAS_PROMETHEUS:
            return
        try:
            write_to_textfile(self._output_path, REGISTRY)
        except Exception as e:
            _logger.error(f"TextfileExporter write error: {e}")

    def cleanup(self):
        """Remove .prom file on clean worker exit to avoid stale metrics."""
        try:
            if os.path.exists(self._output_path):
                os.remove(self._output_path)
                _logger.info(f"TextfileExporter: removed {self._output_path}")
        except Exception as e:
            _logger.warning(f"TextfileExporter cleanup error: {e}")
