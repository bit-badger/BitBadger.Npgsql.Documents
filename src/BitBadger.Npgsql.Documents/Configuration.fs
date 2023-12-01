/// Configuration options for the document store
module BitBadger.Npgsql.Documents.Configuration

open Npgsql
open BitBadger.Npgsql.FSharp.Documents

/// Specify the serializer to use for document serialization/deserialization
let UseSerializer(ser: IDocumentSerializer) =
    Configuration.useSerializer ser

/// Retrieve the currently configured serializer
let Serializer() =
    Configuration.serializer ()

/// Register a data source to use for query execution (disposes the current one if it exists)
let UseDataSource(source: NpgsqlDataSource) =
    Configuration.useDataSource source

/// Retrieve the currently configured data source
let DataSource() =
    Configuration.dataSource ()

/// Set the ID field name to use for documents
let UseIdField(name: string) =
    Configuration.useIdField name

/// Retrieve the currently configured ID field name
let IdField() =
    Configuration.idField ()