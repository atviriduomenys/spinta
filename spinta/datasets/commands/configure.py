from datetime import timedelta

from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset
from spinta.utils.config import asbool
from spinta.utils.units import toseconds


@commands.configure.register(Context, Dataset)
def configure(context: Context, dataset: Dataset):
    rc = context.get("rc")
    dataset_key = ("datasets", dataset.name)
    enable_retention_policy_key = (*dataset_key, "enable_retention_policy")
    accrual_periodicity_key = (*dataset_key, "accrual_periodicity")

    if rc.has(*enable_retention_policy_key):
        dataset.enable_retention_policy = rc.get(*enable_retention_policy_key, cast=asbool)

    if rc.has(*accrual_periodicity_key):
        dataset.accrual_periodicity = rc.get(*accrual_periodicity_key, default=None)

    if dataset.accrual_periodicity is not None and not isinstance(dataset.accrual_periodicity, timedelta):
        dataset.accrual_periodicity = timedelta(seconds=toseconds(dataset.accrual_periodicity))
