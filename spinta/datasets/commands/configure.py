from datetime import timedelta

from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset
from spinta.utils.units import toseconds


@commands.configure.register(Context, Dataset)
def configure(context: Context, dataset: Dataset):
    rc = context.get("rc")
    dataset_key = ("datasets", dataset.name)
    enable_retention_policy_key = (*dataset_key, "enable_retention_policy")
    accrual_periodicity_key = (*dataset_key, "accrual_periodicity")

    if rc.has(*enable_retention_policy_key):
        dataset.enable_retention_policy = rc.get(*enable_retention_policy_key)

    if rc.has(*accrual_periodicity_key):
        dataset.accrual_periodicity = rc.get(
            *accrual_periodicity_key,
            default=None,
            cast=lambda value: timedelta(seconds=toseconds(value)) if value is not None else None,
        )
