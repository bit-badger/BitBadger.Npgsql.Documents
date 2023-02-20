This package provides a set of functions that provide a document database interface to a data store backed by PostgreSQL. This library is targeted toward C# usage; for F#, see `BitBadger.Npgsql.FSharp.Documents`.

## Features

- Select, insert, update, save (upsert), delete, count, and check existence of documents, and create tables and indexes for these documents
- Addresses documents via ID; via equality on any property by using JSON containment queries; or via condition on any property using JSON Path queries
- Accesses documents as your domain models (<abbr title="Plain Old CLR Objects">POCO</abbr>s)
- Uses `Task`-based async for all data access functions
- Uses building blocks for more complex queries

## Getting Started

The main step is to set the data source with which the library will create connections. Construct an `NpgsqlDataSource` instance, and provide it to the library:

```csharp
using BitBadger.Npgsql.Documents

// ...
var dataSource = // ....

Configuration.UseDataSource(dataSource);
// ...
```

By default, the library uses a System.Text.Json-based serializer configured to use the FSharp.SystemTextJson converter (which will have no noticeable effect for C# uses). To provide a different serializer (different options, more converters, etc.), construct it to implement `IDocumentSerializer` and provide it via `Configuration.UseSerializer`.

## Using

Retrieve all customers:

```csharp
// parameter is table name
// returns Task<Customer list>
var customers = await Document.All<Customer>("customer");
```

Select a customer by ID:

```csharp
// parameters are table name and ID
// returns Task<Customer option>
var customer = await Document.Find.ById<Customer>("customer", "123");
```

Count customers in Atlanta:

```csharp
// parameters are table name and object used for JSON containment query
// return Task<int>
var customerCount = await Document.Count.ByContains("customer", new { City = "Atlanta" });
```

Delete customers in Chicago: _(no offense, Second City; just an example...)_

```csharp
// parameters are table name and JSON Path expression
await Document.Delete.ByJsonPath("customer", "$.City ? (@ == \"Chicago\")");
```

## More Information

The [project site](https://bitbadger.solutions/open-source/postgres-documents/) has full details on how to use this library.
