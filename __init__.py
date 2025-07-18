from . import models
from . import controllers
from . import cli

# CRITICAL: Explicitly import the command class to ensure it's registered
from .cli.imq_worker import IMQWorker

from .ir_cron_patcher import patch_ir_cron ; patch_ir_cron()