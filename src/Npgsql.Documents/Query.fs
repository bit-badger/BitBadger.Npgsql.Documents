/// Query construction functions
module Npgsql.Documents.Query

/// Alias for F# document module
module FS = Npgsql.FSharp.Documents

/// Create a SELECT clause to retrieve the document data from the given table
let SelectFromTable (tableName : string) =
    FS.Query.selectFromTable tableName

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

/// Query to update a document
let Update (tableName : string) =
    FS.Query.update tableName

/// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save tableName =
    FS.Query.save tableName
