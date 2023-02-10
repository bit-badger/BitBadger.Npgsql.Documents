This package provides a set of functions that provide a document database interface to a data store backed by PostgreSQL.

_NOTE: This is very much alpha software; reviewing the source and its comments is the best way to learn what it does. The statement above is an aspirational goal for the project; there are likely areas where it currently falls short._

## Getting Started

The main step is to set the data source with which the library will create connections. Construct an `NpgsqlDataSource` instance, and provide it to the library:

```fsharp
open Npgsql.FSharp.Documents

// ...
let dataSource = // ....

Configuration.useDataSource dataSource
// ...
```

By default, the library uses a System.Text.Json-based serializer configured to use the FSharp.SystemTextJson converter. To provide a different serializer (different options, more converters, etc.), construct it to implement `IDocumentSerializer` and provide it via `Configuration.useSerializer`.