module Npgsql.Documents.Document

open System.Threading.Tasks
open Npgsql.FSharp

module FS = Documents

/// Create a domain item from a document, specifying the field in which the document is found
let FromDocument<'T> (field : string, row : RowReader) : 'T =
    FS.fromDocument field row
    
/// Create a domain item from a document
let FromData<'T> (row : RowReader) : 'T =
    FS.fromData row

/// Versions of queries that accept SqlProps as the last parameter
module WithProps =
    
    /// Retrieve all documents in the given table
    let All<'T> (tableName : string, sqlProps : Sql.SqlProps) : Task<ResizeArray<'T>> = backgroundTask {
        let! result = FS.WithProps.all tableName sqlProps
        return ResizeArray result
    }

    /// Insert a new document
    let Insert<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.insert tableName docId document sqlProps

    /// Update a document
    let Update<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.update tableName docId document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let Save<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.save tableName docId document sqlProps

    /// Queries to count documents
    module Count =
        
        /// Count all documents in a table
        let All (tableName : string, sqlProps : Sql.SqlProps) : Task<int> =
            FS.WithProps.Count.all tableName sqlProps
        
        /// Count matching documents using a JSON containment query (@>)
        let ByContains (tableName : string, criteria : obj, sqlProps : Sql.SqlProps) : Task<int> =
            FS.WithProps.Count.byContains tableName criteria sqlProps

        /// Count matching documents using a JSON Path match query (@?)
        let ByJsonPath (tableName : string, jsonPath : string, sqlProps : Sql.SqlProps) : Task<int> =
            FS.WithProps.Count.byJsonPath tableName jsonPath sqlProps
    
    /// Queries to determine if documents exist
    module Exists =

        /// Determine if a document exists for the given ID
        let ById (tableName : string, docId : string, sqlProps : Sql.SqlProps) : Task<bool> =
            FS.WithProps.Exists.byId tableName docId sqlProps

        /// Determine if a document exists using a JSON containment query (@>)
        let ByContains (tableName : string, criteria : obj, sqlProps : Sql.SqlProps) : Task<bool> =
            FS.WithProps.Exists.byContains tableName criteria sqlProps

        /// Determine if a document exists using a JSON Path match query (@?)
        let ByJsonPath (tableName : string, jsonPath : string, sqlProps : Sql.SqlProps) : Task<bool> =
            FS.WithProps.Exists.byJsonPath tableName jsonPath sqlProps

    /// Queries to determine if documents exist
    module Find =
        
        /// Retrieve a document by its ID (may return null)
        let ById<'T when 'T : null> (tableName : string, docId : string, sqlProps : Sql.SqlProps)
                : Task<'T> = backgroundTask {
            let! result = FS.WithProps.Find.byId tableName docId sqlProps
            return Option.toObj result
        }

        /// Execute a JSON containment query (@>)
        let ByContains<'T> (tableName : string, criteria : obj, sqlProps : Sql.SqlProps)
                : Task<ResizeArray<'T>> = backgroundTask {
            let! result = FS.WithProps.Find.byContains tableName criteria sqlProps
            return ResizeArray result
        }

        /// Execute a JSON Path match query (@?)
        let ByJsonPath<'T> (tableName : string, jsonPath : string, sqlProps : Sql.SqlProps)
                : Task<ResizeArray<'T>> = backgroundTask {
            let! result = FS.WithProps.Find.byJsonPath tableName jsonPath sqlProps
            return ResizeArray result
        }

    /// Queries to delete documents
    module Delete =
        
        /// Delete a document by its ID
        let ById (tableName : string, docId : string, sqlProps : Sql.SqlProps) =
            FS.WithProps.Delete.byId tableName docId sqlProps

        /// Delete documents by matching a JSON contains query (@>)
        let ByContains (tableName : string, criteria : obj, sqlProps : Sql.SqlProps) =
            FS.WithProps.Delete.byContains tableName criteria sqlProps

        /// Delete documents by matching a JSON Path match query (@?)
        let ByJsonPath (tableName : string, jsonPath : string, sqlProps : Sql.SqlProps) =
            FS.WithProps.Delete.byJsonPath tableName jsonPath sqlProps


/// Retrieve all documents in the given table
let All<'T> (tableName : string) =
    WithProps.All<'T> (tableName, FS.fromDataSource ())

/// Insert a new document
let Insert<'T> (tableName : string, docId : string, document : 'T) =
    WithProps.Insert (tableName, docId, document, FS.fromDataSource ())

/// Update a document
let Update<'T> (tableName : string, docId : string, document : 'T) =
    WithProps.Update<'T> (tableName, docId, document, FS.fromDataSource ())

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save<'T> (tableName : string, docId : string, document : 'T) =
    WithProps.Save<'T> (tableName, docId, document, FS.fromDataSource ())


/// Queries to count documents
module Count =
    
    /// Count all documents in a table
    let All (tableName : string) =
        WithProps.Count.All (tableName, FS.fromDataSource ())
    
    /// Count matching documents using a JSON containment query (@>)
    let ByContains (tableName : string, criteria : obj) =
        WithProps.Count.ByContains (tableName, criteria, FS.fromDataSource ())

    /// Count matching documents using a JSON Path match query (@?)
    let ByJsonPath (tableName : string, jsonPath : string) =
        WithProps.Count.ByJsonPath (tableName, jsonPath, FS.fromDataSource ())


/// Queries to determine if documents exist
module Exists =

    /// Determine if a document exists for the given ID
    let ById (tableName : string, docId : string) =
        WithProps.Exists.ById (tableName, docId, FS.fromDataSource ())
    
    /// Determine if a document exists using a JSON containment query (@>)
    let ByContains (tableName : string, criteria : obj) =
        WithProps.Exists.ByContains (tableName, criteria, FS.fromDataSource ())

    /// Determine if a document exists using a JSON Path match query (@?)
    let ByJsonPath (tableName : string, jsonPath : string) =
        WithProps.Exists.ByJsonPath (tableName, jsonPath, FS.fromDataSource ())


/// Queries to retrieve documents
module Find =
    
    /// Retrieve a document by its ID
    let ById<'T when 'T : null> (tableName : string, docId : string) =
        WithProps.Find.ById<'T> (tableName, docId, FS.fromDataSource ())

    /// Execute a JSON containment query (@>)
    let ByContains<'T> (tableName : string, criteria : obj) =
        WithProps.Find.ByContains<'T> (tableName, criteria, FS.fromDataSource ())

    let ByJsonPath<'T> (tableName : string, jsonPath : string) =
        WithProps.Find.ByJsonPath<'T> (tableName, jsonPath, FS.fromDataSource ())


/// Queries to delete documents
module Delete =
    
    /// Delete a document by its ID
    let ById (tableName : string, docId : string) =
        WithProps.Delete.ById (tableName, docId, FS.fromDataSource ())

    /// Delete documents by matching a JSON contains query (@>)
    let ByContains (tableName : string, criteria : obj) =
        WithProps.Delete.ByContains (tableName, criteria, FS.fromDataSource ())

    /// Delete documents by matching a JSON Path match query (@?)
    let ByJsonPath (tableName : string, jsonPath : string) =
        WithProps.Delete.ByJsonPath (tableName, jsonPath, FS.fromDataSource ())
