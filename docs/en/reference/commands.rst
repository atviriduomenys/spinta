.. default-role:: literal

########
Commands
########

***************
Startup process
***************

Initialization
==============

During initialization phase Spinta components are initialized from
data stored in variuos external sources.


load(Config)
------------
.. TODO: Rename `load` to `init`.
.. TODO: Pass `RawConfig` as a third argument `load(Config, Config, RawConfig)`,
         configuration can be initialized from different sources.

Initialize :ref:`Config` component from :ref:`RawConfig`. Values from
:ref:`RawConfig` are parsed and converted to native Python types and stored
as :ref:`Config` attributes.

load(Store)
-----------

load(Manifest)
--------------


Linking
=======

link(Manifest)
--------------


Waiting
=======

wait(Store)
-----------


Preparation
===========

prepare(Manifest)
-----------------


****************
Request handling
****************

Request
=======

Interpret request data and decide what commands should be called to process
the request.

prepare(UrlParams, Version, Request)
------------------------------------

Initialize :ref:`UrlParams` from an HTTP request object.


Auth
====

Check if client has access to requested resource.


.. _authorize:

authorize(Action, Namespace | Model | Property)
-----------------------------------------------

Check if a client has access to do an action on a resource.


Read
====

.. _getall_request_ns:

getall(Request, Namespace)
--------------------------

Get objects from all models in a given namespace.

- :ref:`authorize`
- :ref:`getall_ns`
- :ref:`prepare_data_for_response_model`
- :ref:`render`


.. _getall_ns:

getall(Namespace)
-----------------

Get objects from all models in a given namespace.

- :ref:`getall_model_backend`


.. _getall_request_model_backend:

getall(Request, Model, Backend)
-------------------------------

Get objects from a given model. This command interprets given request and
returns a response in requested format.

- :ref:`authorize`
- :ref:`getall_model_backend`
- :ref:`prepare_data_for_response_model`
- :ref:`render`


.. _getall_model_backend:

getall(Model, Backend)
----------------------

This command build and executes backend query, executes it and returns back
the results.

- :ref:`cast_backend_to_python_model`


Encode
======


.. _cast_backend_to_python_model:

cast_backend_to_python(Model, Backend, Any)
-------------------------------------------

Converts backend model data to python-native model data.

- :ref:`cast_backend_to_python_dtype`


.. _cast_backend_to_python_prop:

cast_backend_to_python(Property, Backend, Any)
----------------------------------------------

Converts backend property (subresource) data to python-native data.

This is used for `/ns/Model/<ID>/property` endpoints.

- :ref:`cast_backend_to_python_dtype`


.. _cast_backend_to_python_dtype:

cast_backend_to_python(DataType, Backend, Any)
----------------------------------------------

Converts backend types to python-native types for a data type. Should call
itself for complex data types like `object` and `array`.

- :ref:`cast_backend_to_python_dtype`


Response
========

.. _prepare_data_for_response_model:

prepare_data_for_response(Model, Format, Any)
---------------------------------------------

Converts python-native data types to a requested response format. This
expects to get python-native data, which is prepared using
:ref:`cast_backend_to_python_model` command.

- :ref:`prepare_dtype_for_response`


prepare_data_for_response(DataType, Format, Any)
------------------------------------------------

Convert python-native data types to a given format, for subresources.

- :ref:`prepare_dtype_for_response`


.. _prepare_dtype_for_response:

prepare_dtype_for_response(DataType, Format, Any)
-------------------------------------------------

Converts python-native data types to a requested response format.


.. _render:

render(Request, Namespace | Model | Property, Format)
-----------------------------------------------------

Takes python-native data and return Starlette's HTTP response.
