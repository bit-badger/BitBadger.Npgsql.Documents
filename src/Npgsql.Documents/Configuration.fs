/// Configuration options for the document store
module Npgsql.Documents.Configuration

open Npgsql
open Npgsql.FSharp.Documents

/// Specify the serializer to use for document serialization/deserialization
let UseSerializer (ser : IDocumentSerializer) =
    Configuration.useSerializer ser

/// Register a data source to use for query execution (disposes the current one if it exists)
let UseDataSource (source : NpgsqlDataSource) =
    Configuration.useDataSource source
        