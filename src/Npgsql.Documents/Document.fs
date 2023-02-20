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
    
    /// Insert a new document
    let Insert<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.insert tableName docId document sqlProps

    /// Insert a new document
    let InsertFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.insert tableName (idFunc.Invoke document) document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let Save<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.save tableName docId document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let SaveFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T, sqlProps : Sql.SqlProps) =
        FS.WithProps.save tableName (idFunc.Invoke document) document sqlProps

    /// Commands to count documents
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
    
    /// Commands to determine if documents exist
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

    /// Commands to determine if documents exist
    module Find =
        
        /// Retrieve all documents in the given table
        let All<'T> (tableName : string, sqlProps : Sql.SqlProps) : Task<ResizeArray<'T>> = backgroundTask {
            let! result = FS.WithProps.Find.all tableName sqlProps
            return ResizeArray result
        }

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

    /// Commands to update documents
    module Update =
        
        /// Update a document
        let Full<'T> (tableName : string, docId : string, document : 'T, sqlProps : Sql.SqlProps) =
            FS.WithProps.Update.full tableName docId document sqlProps

        /// Update a document
        let FullFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T,
                          sqlProps : Sql.SqlProps) =
            FS.WithProps.Update.full tableName (idFunc.Invoke document) document sqlProps

        /// Update a partial document
        let PartialById (tableName : string, docId : string, partial : obj, sqlProps : Sql.SqlProps) =
            FS.WithProps.Update.partialById tableName docId partial sqlProps

        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        let PartialByContains (tableName : string, criteria : obj, partial : obj, sqlProps : Sql.SqlProps) =
            FS.WithProps.Update.partialByContains tableName criteria partial sqlProps
        
        /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
        let PartialByJsonPath (tableName : string, jsonPath : string, partial : obj, sqlProps : Sql.SqlProps) =
            FS.WithProps.Update.partialByJsonPath tableName jsonPath partial sqlProps

    /// Commands to delete documents
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
    
    /// Commands to execute custom SQL queries
    module Custom =

        open System
        open System.Collections.Generic

        /// Execute a query that returns one or no results
        let Single<'T when 'T : null> (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>,
                                       deserFunc : Func<RowReader, 'T>, sqlProps : Sql.SqlProps)
                : Task<'T> = backgroundTask {
            let! results =
                Sql.query query sqlProps
                |> Sql.parameters (List.ofSeq parameters)
                |> Sql.executeAsync (fun row -> deserFunc.Invoke row)
            return match List.tryHead results with Some it -> it | None -> null
        }

        /// Execute a query that returns a list of results
        let List<'T> (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>,
                      deserFunc : Func<RowReader, 'T>, sqlProps : Sql.SqlProps)
                : Task<ResizeArray<'T>> = backgroundTask {
            let! results =
                Sql.query query sqlProps
                |> Sql.parameters (List.ofSeq parameters)
                |> Sql.executeAsync (fun row -> deserFunc.Invoke row)
            return ResizeArray results
        }

        /// Execute a query that returns no results
        let NonQuery (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>,
                      sqlProps : Sql.SqlProps) = backgroundTask {
            let! _ =
                Sql.query query sqlProps
                |> Sql.parameters (FSharp.Collections.List.ofSeq parameters)
                |> Sql.executeNonQueryAsync
            ()
        }


/// Insert a new document
let Insert<'T> (tableName : string, docId : string, document : 'T) =
    WithProps.Insert (tableName, docId, document, FS.fromDataSource ())

/// Insert a new document
let InsertFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T) =
    WithProps.InsertFunc (tableName, idFunc, document, FS.fromDataSource ())

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save<'T> (tableName : string, docId : string, document : 'T) =
    WithProps.Save<'T> (tableName, docId, document, FS.fromDataSource ())

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let SaveFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T) =
    WithProps.SaveFunc<'T> (tableName, idFunc, document, FS.fromDataSource ())


/// Commands to count documents
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


/// Commands to determine if documents exist
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


/// Commands to retrieve documents
module Find =
    
    /// Retrieve all documents in the given table
    let All<'T> (tableName : string) =
        WithProps.Find.All<'T> (tableName, FS.fromDataSource ())

    /// Retrieve a document by its ID
    let ById<'T when 'T : null> (tableName : string, docId : string) =
        WithProps.Find.ById<'T> (tableName, docId, FS.fromDataSource ())

    /// Execute a JSON containment query (@>)
    let ByContains<'T> (tableName : string, criteria : obj) =
        WithProps.Find.ByContains<'T> (tableName, criteria, FS.fromDataSource ())

    let ByJsonPath<'T> (tableName : string, jsonPath : string) =
        WithProps.Find.ByJsonPath<'T> (tableName, jsonPath, FS.fromDataSource ())


/// Commands to update documents
module Update =
    
    /// Update a document
    let Full<'T> (tableName : string, docId : string, document : 'T) =
        WithProps.Update.Full (tableName, docId, document, FS.fromDataSource ())

    /// Update a document
    let FullFunc<'T> (tableName : string, idFunc : System.Func<'T, string>, document : 'T) =
        WithProps.Update.FullFunc (tableName, idFunc, document, FS.fromDataSource ())

    /// Update a partial document
    let PartialById (tableName : string, docId : string, partial : obj) =
        WithProps.Update.PartialById (tableName, docId, partial, FS.fromDataSource ())

    /// Update partial documents using a JSON containment query in the WHERE clause (@>)
    let PartialByContains (tableName : string, criteria : obj, partial : obj) =
        WithProps.Update.PartialByContains (tableName, criteria, partial, FS.fromDataSource ())
    
    /// Update partial documents using a JSON Path match query in the WHERE clause (@?)
    let PartialByJsonPath (tableName : string, jsonPath : string, partial : obj) =
        WithProps.Update.PartialByJsonPath (tableName, jsonPath, partial, FS.fromDataSource ())


/// Commands to delete documents
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


/// Commands to execute custom SQL queries
module Custom =

    open System
    open System.Collections.Generic

    /// Execute a query that returns one or no results
    let Single<'T when 'T : null> (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>,
                                   deserFunc : Func<RowReader, 'T>) =
        WithProps.Custom.Single (query, parameters, deserFunc, FS.fromDataSource ())

    /// Execute a query that returns a list of results
    let List<'T> (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>, deserFunc : Func<RowReader, 'T>) =
        WithProps.Custom.List (query, parameters, deserFunc, FS.fromDataSource ())

    /// Execute a query that returns no results
    let NonQuery (query : string, parameters : IEnumerable<Tuple<string, SqlValue>>) =
        WithProps.Custom.NonQuery (query, parameters, FS.fromDataSource ())
