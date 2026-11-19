.. default-role:: literal

UML diagram
###########

Below is a simplified UML class diagram of the main Spinta components,
matching the current code (verified against `spinta/components.py`,
`spinta/backends/components.py`, `spinta/datasets/components.py`,
`spinta/manifests/components.py`, and `spinta/core/config.py`).

.. mermaid::

   classDiagram
      direction TB

      class Context {
         +name
         +parent: Context
         +fork(name) Context
         +set(name, value)
         +get(name)
         +bind(name, factory)
         +attach(name, factory)
      }

      class Store {
         +config: Config
         +manifest: Manifest
         +internal: InternalManifest
         +backends: Dict~str, Backend~
         +keymaps: dict
         +accesslog: AccessLog
      }
      Store o-- Config

      class Config {
         +rc: RawConfig
         +components: dict
         +commands: dict
         +exporters: dict
      }
      Config o-- RawConfig

      class RawConfig {
         +sources: List~ConfigSource~
         +read(sources)
         +fork(sources) RawConfig
         +get(key) Any
         +lock()
      }
      class ConfigSource
      class PyDict
      class Path
      class CliArgs
      class EnvVars
      class EnvFile
      ConfigSource <|-- PyDict
      PyDict <|-- Path
      PyDict <|-- CliArgs
      ConfigSource <|-- EnvVars
      EnvVars <|-- EnvFile
      RawConfig o-- ConfigSource

      class Manifest {
         +type: str
         +name: str
         +backend: Backend
         +keymap: KeyMap
         +mode: Mode
         +sync: List~Manifest~
         +get_objects() dict
      }
      class TabularManifest {
         +format: TabularFormat
      }
      class CsvManifest
      class AsciiManifest
      class XlsxManifest
      class GsheetsManifest
      Manifest <|-- TabularManifest
      TabularManifest <|-- CsvManifest
      TabularManifest <|-- AsciiManifest
      TabularManifest <|-- XlsxManifest
      TabularManifest <|-- GsheetsManifest
      class YamlManifest
      class InlineManifest
      class InternalManifest
      class InternalSQLManifest
      Manifest <|-- YamlManifest
      YamlManifest <|-- InternalManifest
      Manifest <|-- InternalSQLManifest
      class BackendManifest
      Manifest <|-- BackendManifest
      class SqlManifest

      Manifest --> Backend
      Manifest --> KeyMap

      class Backend {
         +Engine
         +type: str
         +name: str
         +engine: Engine
         +query_builder_class
         +result_builder_class
         +transaction() contextmanager
         +begin() contextmanager
         +supports(feature) bool
      }
      class PostgreSQL
      class FileSystem
      class Memory
      class ExternalBackend
      Backend <|-- PostgreSQL
      Backend <|-- FileSystem
      Backend <|-- Memory
      Backend <|-- ExternalBackend
      ExternalBackend : model_requires_source

      class Node {
         +type: str
         +name: str
         +parent: Node
         +manifest: Manifest
         +node_type()
         +model_type()
      }
      class MetaData {
         +eid
         +id
      }
      class ExtraMetaData {
         +id
      }
      Node <|-- MetaData
      Node <|-- ExtraMetaData

      class Namespace {
         +names: dict
         +models: dict
         +backend: Backend
         +parent
      }
      class Model {
         +ns: Namespace
         +properties: dict
         +external: Entity
         +base: Base
         +scopes: dict
         +page: PageInfo
         +mode: Mode
         +keymap: KeyMap
         +backend: Backend
      }
      class Base {
         +model: Model
         +parent: Model
         +pk: List~Property~
      }
      class Property {
         +place: str
         +name
         +dtype: DataType
         +model: Model
         +external: Attribute
         +enum: EnumValue
         +prepare
      }
      class Dataset {
         +resources: dict
         +ns: Namespace
         +prefixes: dict
      }
      class Resource {
         +dataset: Dataset
         +backend: ExternalBackend
         +models: dict
      }
      class Entity {
         +dataset: Dataset
         +resource: Resource
         +model: Model
         +pkeys: List~Property~
         +prepare: Expr
      }
      class Attribute {
         +prop: Property
         +name
         +prepare: Expr
      }
      class External
      External <|-- Resource
      External <|-- Entity
      External <|-- Attribute
      MetaData <|-- Namespace
      MetaData <|-- Model
      MetaData <|-- Dataset
      ExtraMetaData <|-- Property
      ExtraMetaData <|-- Base

      Store o-- Manifest
      Store o-- Backend
      Store o-- KeyMap
      Store o-- AccessLog
      Manifest *-- Backend : backend
      Manifest o-- KeyMap : keymap
      Manifest o-- MetaData : nodes
      Model --> Namespace : ns
      Model *-- Property : properties
      Model --> Entity : external
      Model --> Base : base
      Model --> KeyMap : keymap
      Model --> Backend : backend
      Dataset --> Resource : resources
      Resource --> ExternalBackend : backend
      Entity --> Dataset : dataset
      Entity --> Resource : resource
      Entity --> Model : model
      Entity *-- Property : pkeys
      Attribute --> Property : prop
      Base --> Model : model
      Base *-- Property : pk
      Property --> DataType : dtype
      Property --> Attribute : external

      class DataType {
         +name: str
      }
      class PrimaryKey
      class String
      class Integer
      class Object
      class Ref
      class Array
      class Geometry
      class File
      DataType <|-- PrimaryKey
      DataType <|-- String
      DataType <|-- Integer
      DataType <|-- Object
      DataType <|-- Ref
      DataType <|-- Array
      DataType <|-- Geometry
      DataType <|-- File
      DataType *-- Property : prop

      class AccessLog
      class FileAccessLog
      class PythonAccessLog
      AccessLog <|-- FileAccessLog
      AccessLog <|-- PythonAccessLog

      class KeyMap {
         +name
         +encode(name, value) str
         +decode(name, key) Any
      }
      class SqlAlchemyKeyMap
      class RedisKeyMap
      KeyMap <|-- SqlAlchemyKeyMap
      KeyMap <|-- RedisKeyMap

      Context --> RawConfig : rc
      Context --> Config : config
      Context --> Store : store

Note that `Context` references `Config` and `Store` by name — you can
fetch them via `context.get("config")` and `context.get("store")`.
`Store` has `Store.manifest` (the main manifest) and `Store.internal`
(the internal manifest, like `internal_sql` manifest stored in a
backend, used mainly for migrations). Manifests that load from other
manifests via `sync` config key are stored in `Manifest.sync`.

An autodoc-generated listing of the component classes is available in
:doc:`/reference/components` — complementing this hand-drawn overview,
and one concept page per major class (see :doc:`/contrib/architecture/concepts/index`)
describes each class in detail and maps it to concepts in external
specifications (UAPI, DSA, DCAT, RFC 6749, `Conceptual model`_).

.. _Conceptual model: https://raw.githubusercontent.com/atviriduomenys/
   katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg
