.. default-role:: literal

Functions
#########

Functions in manifest are used for:

- Transforming and validating data using `apply` parameter.

- Describing database migration steps.


Using functions in Manifest
===========================


.. code-block:: yaml

    type: model
      properties:
        gov_org_id:
          type: string
          description: Government issues organization identifier
          prepare:
            - .check(.len() == 9, "Invalid company registration number.")
            - .check(.re("\d+"))
