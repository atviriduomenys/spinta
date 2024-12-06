.. default-role:: literal

Issues
######


Definition of done
******************

An issue is considered done when all of the following tasks are done:

- Code review by a colegue is done.

- Automated tests covers new functionality or reproduces a bug, that was fixed.
  See :ref:`writing-tests`.

- Documentation is up to date, we have following documentation pages:

  - `Spinta documentaiton`_ - technical documentation abould Spinta.
  - `Manifest specification`_
    (`old version <Manifest specification (old)_>`_) - DSA specification.
  - `UAPI specification`_ (UDTS) - universal API specificaiton.
  - `Spinta (open data manual)`_ - instructions for users who use Spinta for
    open data.
  - `Open data store`_ (Saugykla) - API service for open data distribution.

- Deployed to the test environment.

- Tested manually on the test environment.

  - Make sure automated manifest validation still works in these repositories:

    | https://github.com/atviriduomenys/manifest
    | https://github.com/ivpk/metadata

- Deployed on a production environment.


.. _Spinta documentaiton: https://spinta.readthedocs.io/en/latest/
.. _Manifest specification: https://ivpk.github.io/dsa/
.. _Manifest specification (old): https://atviriduomenys.readthedocs.io/dsa/index.html
.. _Spinta (open data manual): https://atviriduomenys.readthedocs.io/spinta.html
.. _Open data store: https://atviriduomenys.readthedocs.io/api/index.html
.. _UAPI specification: https://ivpk.github.io/uapi/

