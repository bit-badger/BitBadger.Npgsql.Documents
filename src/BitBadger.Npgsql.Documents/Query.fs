/// Query construction functions
module BitBadger.Npgsql.Documents.Query

/// Alias for F# document module
module FS = BitBadger.Npgsql.FSharp.Documents

/// Create a SELECT clause to retrieve the document data from the given table
let SelectFromTable (tableName : string) =
    FS.Query.selectFromTable tableName

/// Create a WHERE clause fragment to implement an ID-based query
let WhereById (paramName : string) =
    FS.Query.whereById paramName

/// Create a WHERE clause fragment to implement a @> (JSON contains) condition
let WhereDataContains (paramName : string) =
    FS.Query.whereDataContains paramName

/// Create a WHERE clause fragment to implement a @? (JSON Path match) condition
let WhereJsonPathMatches (paramName : string) =
    FS.Query.whereJsonPathMatches paramName

/// Create a JSONB document parameter
let JsonbDocParam (it : obj) =
    FS.Query.jsonbDocParam it

/// Create ID and data parameters for a query
let DocParameters<'T> (docId : string, doc : 'T) =
    FS.Query.docParameters docId doc

/// Query to insert a document
let Insert (tableName : string) =
    FS.Query.insert tableName

/// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save tableName =
    FS.Query.save tableName

/// Queries for counting documents
module Count =
    
    /// Query to count all documents in a table
    let All tableName =
        FS.Query.Count.all tableName
    
    /// Query to count matching documents using a JSON containment query (@>)
    let ByContains tableName =
        FS.Query.Count.byContains tableName
    
    /// Query to count matching documents using a JSON Path match (@?)
    let ByJsonPath tableName =
        FS.Query.Count.byJsonPath tableName

/// Queries for determining document existence
module Exists =

    /// Query to determine if a document exists for the given ID
    let ById tableName =
        FS.Query.Exists.byId tableName

    /// Query to determine if documents exist using a JSON containment query (@>)
    let ByContains tableName =
        FS.Query.Exists.byContains tableName
    
    /// Query to determine if documents exist using a JSON Path match (@?)
    let ByJsonPath tableName =
        FS.Query.Exists.byJsonPath tableName

/// Queries for retrieving documents
module Find =

    /// Query to retrieve a document by its ID
    let ById tableName =
        FS.Query.Find.byId tableName
    
    /// Query to retrieve documents using a JSON containment query (@>)
    let ByContains tableName =
        FS.Query.Find.byContains tableName
    
    /// Query to retrieve documents using a JSON Path match (@?)
    let ByJsonPath tableName =
        FS.Query.Find.byJsonPath tableName

/// Queries to update documents
module Update =

    /// Query to update a document
    let Full tableName =
        FS.Query.Update.full tableName

    /// Query to update a document
    let PartialById tableName =
        FS.Query.Update.partialById tableName
    
    /// Query to update partial documents matching a JSON containment query (@>)
    let PartialByContains tableName =
        FS.Query.Update.partialByContains tableName

    /// Query to update partial documents matching a JSON containment query (@>)
    let PartialByJsonPath tableName =
        FS.Query.Update.partialByJsonPath tableName

/// Queries to delete documents
module Delete =
    
    /// Query to delete a document by its ID
    let ById tableName =
        FS.Query.Delete.byId tableName

    /// Query to delete documents using a JSON containment query (@>)
    let ByContains tableName =
        FS.Query.Delete.byContains tableName

    /// Query to delete documents using a JSON Path match (@?)
    let ByJsonPath tableName =
        FS.Query.Delete.byJsonPath tableName
