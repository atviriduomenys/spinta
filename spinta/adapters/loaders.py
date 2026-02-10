from typing import Iterable, Mapping


class ModelAdapterError(ValueError):
    """Raised when adapter input cannot be converted into a Model."""


class ModelAdapter:
    """Base adapter that normalizes client model rows into domain models."""

    def from_model(self, model: object):
        """
        Build a :class:`Model` from a higher-level in-memory model representation.

        This method is intended for clients that already have a parsed or constructed
        domain model (for example, a nested mapping / object graph that mirrors the
        resource tree described in the manifest). Implementations are expected to:

        * Interpret ``model`` as a hierarchical structure whose leaves correspond to
            manifest properties and whose internal nodes represent non-leaf paths.
        * Produce one logical model for each property, including properties on
            nested objects, so that each row can be converted into a :class:`ManifestRow`
            via :meth:`_to_manifest_row`.
        * Preserve property naming conventions for language variants (for example
            ``name@lt``), leaving flattening and selector handling to domain services.
        * Return a :class:`Manifest` whose rows collectively describe the entire tree
            encoded by ``model``.

        The base implementation does not perform any mapping and exists only to define
        the contract; concrete adapters must override this method with logic that
        extracts manifest rows from the given model.

        :param model: A client-defined, potentially nested object describing the
            resource tree and its properties to be expressed as a manifest.
        :returns: A fully constructed and validated :class:`Manifest` instance.
        :raises ManifestAdapterError: If the adapter does not support mapping from
            model objects or if the input cannot be converted into a manifest.
        """
        raise ModelAdapterError(
            "Parsing deferred: model mapping must be implemented by client adapter"
        )


class DataAdapterError(ValueError):
    """Raised when adapter input cannot be converted into data rows."""


class DataAdapter:
    """Protocol for client-provided data parsing adapters."""

    def load(self, *args, **kwargs: object) -> Iterable[Mapping[str, object]]:
        """Return row mappings aligned to manifest properties."""
        raise DataAdapterError("Parsing deferred: data parsing must be implemented by client adapter")
