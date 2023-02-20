This package provides a set of functions that provide a document database interface to a data store backed by PostgreSQL. This library is targeted toward F# usage; for C#, see `Npgsql.Documents`.

_NOTE: This is beta software; reviewing the source and its comments is the best way to learn what it does._

## Features

- Select, insert, update, save (upsert), delete, count, and check existence of documents, and create tables and indexes for these documents
- Addresses documents via ID; via equality on any property by using JSON containment queries; or via condition on any property using JSON Path queries
- Accesses documents as your domain models (<abbr title="Plain Old CLR Objects">POCO</abbr>s)
- Uses `Task`-based async for all data access functions
- Uses building blocks for more complex queries

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

## Using

Retrieve all customers:

```fsharp
// parameter is table name
// returns Task<Customer list>
let! customers = all<Customer> "customer"
```

Select a customer by ID:

```fsharp
// parameters are table name and ID
// returns Task<Customer option>
let! customer = Find.byId<Customer> "customer" "123"
```

Count customers in Atlanta:

```fsharp
// parameters are table name and object used for JSON containment query
// return Task<int>
let! customerCount = Count.byContains "customer" {| City = "Atlanta" |}
```

Delete customers in Chicago: _(no offense, Second City; just an example...)_

```fsharp
// parameters are table name and JSON Path expression
do! Delete.byJsonPath "customer" """$.City ? (@ == "Chicago")"""
```
