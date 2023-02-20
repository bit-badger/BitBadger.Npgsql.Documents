module BitBadger.Npgsql.Documents.Definition

open Npgsql.FSharp

/// Alias for F# document module
module FS = BitBadger.Npgsql.FSharp.Documents

/// Convert the C# index type representation to the F# one
let private convertIndexType idxType =
    match idxType with
    | DocumentIndex.Full -> FS.DocumentIndex.Full
    | DocumentIndex.Optimized -> FS.DocumentIndex.Optimized
    | it -> invalidOp $"Index type {it} invalid"

/// SQL statement to create a document table
let CreateTable (name : string) =
    FS.Definition.createTable name

/// SQL statement to create an index on documents in the specified table
let CreateIndex (name : string, idxType : DocumentIndex) =
    FS.Definition.createIndex name (convertIndexType idxType)

/// Definitions that take SqlProps as their last parameter
module WithProps =
    
    /// Create a document table
    let EnsureTable (name : string, sqlProps : Sql.SqlProps) =
        FS.Definition.WithProps.ensureTable name sqlProps

    /// Create an index on documents in the specified table
    let EnsureIndex (name : string, idxType : DocumentIndex, sqlProps : Sql.SqlProps) =
        FS.Definition.WithProps.ensureIndex name (convertIndexType idxType) sqlProps

/// Create a document table
let EnsureTable name =
    WithProps.EnsureTable (name, FS.fromDataSource ())

let EnsureIndex (name, idxType) =
    WithProps.EnsureIndex (name, idxType, FS.fromDataSource ())


