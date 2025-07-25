from . import models
from . import controllers
from . import cli

# CRITICAL: Explicitly import the command classes to ensure they're registered
from .cli.imq_worker import IMQWorker
from .cli.imq_test import IMQTest

from .ir_cron_patcher import patch_ir_cron ; patch_ir_cron()