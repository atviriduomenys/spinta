# Health probe configuration

Spinta serves a `/health` endpoint that reports whether the service is
operational. It is meant for container, load balancer and monitoring probes,
and it requires no authentication.

The response follows the UAPI
[`health`](https://ivpk.github.io/uapi/#tag/utility/operation/apiHealth)
schema: a `healthy` flag for the whole service and a `dependencies` list, where
each item is a named dependency with its own `healthy` flag.

```json
{
  "healthy": true,
  "dependencies": [
    {"name": "spinta", "healthy": true},
    {"name": "disk", "healthy": true},
    {"name": "memory", "healthy": true}
  ]
}
```

The service is `healthy` only when every dependency is:

| Dependency | Healthy when                                                  |
| ---------- | ------------------------------------------------------------- |
| `spinta`   | Spinta answered the request at all                             |
| `disk`     | free disk space on `data_path` is at or above the threshold    |
| `memory`   | available memory is at or above the threshold                  |

## Reading the result

```{important}
An unhealthy service is reported in the body, not in the status code. The
endpoint always answers `200`, with `healthy: false` when something is wrong,
because UAPI declares `503` to be the `ServiceNotAvailable` error object.
Probes must therefore inspect the `healthy` field rather than the status code.
```

Only the flags are reported. Since the endpoint is not authenticated, it must
not disclose how the service is deployed, so paths, free space, thresholds and
errors are written to the log instead of to the response. When a check fails,
Spinta logs the details at `ERROR` level, for example:

```
Not enough free disk space on /var/lib/spinta: 1024 MB free, 2048 MB required.
```

## Configuration options

Both thresholds are absolute values in megabytes. The values are read from your
`config.yml`; if they are not set there, Spinta falls back to the defaults
defined in `spinta/config.py`.

| Option                         | Default | Meaning                                            |
| ------------------------------ | ------- | -------------------------------------------------- |
| `health.min_free_disk_space`   | `2048`  | MB of free disk space below which `disk` is unhealthy   |
| `health.min_free_memory`       | `256`   | MB of available memory below which `memory` is unhealthy |

```yaml
health:
  min_free_disk_space: 2048
  min_free_memory: 256
```

Or as environment variables:

```sh
SPINTA_HEALTH__MIN_FREE_DISK_SPACE=2048
SPINTA_HEALTH__MIN_FREE_MEMORY=256
```

### Choosing the thresholds

A threshold must be lower than the resources the machine is actually given,
otherwise the probe reports the service as unhealthy from the moment it starts.
The defaults are chosen against the minimum requirements for running an agent —
1 GB of RAM and 5 GB of free disk space — so that a machine provisioned to those
minimums is healthy, and the probe warns while there is still room to react.

Raise the thresholds together with the resources. On a host with a large data
volume 2 GB of free space is reached far too late to be a useful warning, and on
a container given more than 1 GB of memory the memory threshold can be raised in
proportion.

## What is measured

`disk` is the free space of the file system holding `data_path`, not of the
whole machine. If `data_path` has not been created yet, the closest existing
parent directory is measured, so a fresh installation is not reported as
unhealthy merely because the directory is missing.

`memory` is measured against the memory limit of the control group this process
belongs to — the limit given to a container, or a `MemoryMax=` set on a systemd
service. Parent groups constrain it as well, so what is reported is the smallest
amount left across the process' own group and all of its parents; memory a
parent has already given to its other children is not available to this process
either. Finally, the memory of the host bounds it too, because a control group
limit is a ceiling and not a reservation: a container with room to spare is
still killed when the machine itself runs out of memory, so the smaller of the
two is what gets reported. Reclaimable page cache is not counted as used, the same way
the kernel decides whether to kill the process. When no control group limits
this process, only the available memory of the host is left to go by.

```{note}
This distinction matters in containers. `/proc/meminfo`, which reports host
memory, is not namespaced, so a container reading it sees the memory of the whole
machine. A container limited to 1 GB and about to be killed for using it all
would otherwise look perfectly healthy.
```
