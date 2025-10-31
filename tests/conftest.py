import sys
import types


try:
    import common
except ModuleNotFoundError:
    common = types.ModuleType("common")
    sys.modules["common"] = common

fake_queue_module = types.ModuleType("common.queue")


def fake_queue(*args, **kwargs):
    return None


fake_queue_module.queue = fake_queue
sys.modules["common.queue"] = fake_queue_module

setattr(common, "queue", fake_queue_module)
