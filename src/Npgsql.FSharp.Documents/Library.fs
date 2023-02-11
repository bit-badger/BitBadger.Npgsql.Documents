module Npgsql.FSharp.Documents

/// The required document serialization implementation
type IDocumentSerializer =
    
    /// Serialize an object to a JSON string
    abstract Serialize<'T> : 'T -> string
    
    /// Deserialize a JSON string into an object
    abstract Deserialize<'T> : string -> 'T


/// The type of index to generate for the document
type DocumentIndex =
    /// A GIN index with standard operations (all operators supported)
    | Full
    /// A GIN index with JSONPath operations (optimized for @>, @?, @@ operators)
    | Optimized


/// Configuration for document handling
module Configuration =

    open System.Text.Json
    open System.Text.Json.Serialization

    /// The default JSON serializer options to use with the stock serializer
    let private jsonDefaultOpts =
        let o = JsonSerializerOptions ()
        o.Converters.Add (JsonFSharpConverter ())
        o
    
    /// The serializer to use for document manipulation
    let mutable internal serializer =
        { new IDocumentSerializer with
            member _.Serialize<'T> (it : 'T) : string =
                JsonSerializer.Serialize (it, jsonDefaultOpts)
            member _.Deserialize<'T> (it : string) : 'T =
                JsonSerializer.Deserialize<'T> (it, jsonDefaultOpts)
        }
    
    /// Register a serializer to use for translating documents to domain types
    let useSerializer ser =
        serializer <- ser

    /// The data source to use for query execution
    let mutable private dataSourceValue : Npgsql.NpgsqlDataSource option = None

    /// Register a data source to use for query execution
    let useDataSource source =
        dataSourceValue <- Some source
    
    let internal dataSource () =
        match dataSourceValue with
        | Some source -> source
        | None -> invalidOp "Please provide a data source before attempting data access"


/// Shorthand to retrieve the data source
let private fromDataSource () =
    Configuration.dataSource () |> Sql.fromDataSource


/// Data definition
[<RequireQualifiedAccess>]
module Definition =

    /// SQL statement to create a document table
    let createTable name =
        $"CREATE TABLE IF NOT EXISTS %s{name} (id TEXT NOT NULL PRIMARY KEY, data JSONB NOT NULL)"
    
    /// SQL statement to create an index on documents in the specified table
    let createIndex (name : string) idxType =
        let extraOps = match idxType with Full -> "" | Optimized -> " jsonb_path_ops"
        let tableName = name.Split(".") |> Array.last
        $"CREATE INDEX IF NOT EXISTS idx_{tableName} ON {name} USING GIN (data{extraOps})"
    
    /// Definitions that take SqlProps as their last parameter
    module WithProps =
        
        /// Create a document table
        let ensureTable name sqlProps = backgroundTask {
            let! _ = sqlProps |> Sql.query (createTable name) |> Sql.executeNonQueryAsync
            ()
        }

        /// Create an index on documents in the specified table
        let ensureIndex name idxType sqlProps = backgroundTask {
            let! _ = sqlProps |> Sql.query (createIndex name idxType) |> Sql.executeNonQueryAsync
            ()
        }
    
    /// Create a document table
    let ensureTable name =
        WithProps.ensureTable name (fromDataSource ())
    
    let ensureIndex name idxType =
        WithProps.ensureIndex name idxType (fromDataSource ())


/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    /// Create a SELECT clause to retrieve the document data from the given table
    let selectFromTable tableName =
        $"SELECT data FROM %s{tableName}"
    
    /// Create a WHERE clause fragment to implement a @> (JSON contains) condition
    let whereDataContains paramName =
        $"data @> %s{paramName}"
    
    /// Create a WHERE clause fragment to implement a @? (JSON Path match) condition
    let whereJsonPathMatches paramName =
        $"data @? %s{paramName}"
    
    /// Create a JSONB document parameter
    let jsonbDocParam (it : obj) =
        Sql.jsonb (Configuration.serializer.Serialize it)

    /// Create ID and data parameters for a query
    let docParameters<'T> docId (doc : 'T) =
        [ "@id", Sql.string docId; "@data", jsonbDocParam doc ]
    
    /// Query to insert a document
    let insert tableName =
        $"INSERT INTO %s{tableName} (id, data) VALUES (@id, @data)"

    /// Query to update a document
    let update tableName =
        $"UPDATE %s{tableName} SET data = @data WHERE id = @id"

    /// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save tableName =
        $"INSERT INTO %s{tableName} (id, data) VALUES (@id, @data) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data"


/// Create a domain item from a document, specifying the field in which the document is found
let fromDocument<'T> field (row : RowReader) : 'T =
    Configuration.serializer.Deserialize<'T> (row.string field)
    
/// Create a domain item from a document
let fromData<'T> row : 'T =
    fromDocument "data" row

/// Execute a non-query statement to manipulate a document
let private executeNonQuery query docId (document : 'T) sqlProps = backgroundTask {
    let! _ =
        sqlProps
        |> Sql.query query
        |> Sql.parameters (Query.docParameters docId document)
        |> Sql.executeNonQueryAsync
    ()
}


open System.Threading.Tasks

/// Versions of queries that accept SqlProps as the last parameter
module WithProps =
    
    /// Retrieve all documents in the given table
    let all<'T> tableName sqlProps : Task<'T list> =
        sqlProps
        |> Sql.query (Query.selectFromTable tableName)
        |> Sql.executeAsync fromData<'T>

    /// Insert a new document
    let insert<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (Query.insert tableName) docId document sqlProps

    /// Update a document
    let update<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (Query.update tableName) docId document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (Query.save tableName) docId document sqlProps

    /// Queries to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count matching documents using a JSON containment query (@>)
        let byContains tableName (criteria : obj) sqlProps : Task<int> =
            sqlProps
            |> Sql.query $"""SELECT COUNT(id) AS it FROM %s{tableName} WHERE {Query.whereDataContains "@it"}"""
            |> Sql.parameters [ "@it", Query.jsonbDocParam criteria ]
            |> Sql.executeRowAsync (fun row -> row.int "it")

        /// Count matching documents using a JSON Path match query (@?)
        let byJsonPath tableName jsonPath sqlProps : Task<int> =
            sqlProps
            |> Sql.query $"""SELECT COUNT(id) AS it FROM %s{tableName} WHERE {Query.whereJsonPathMatches "@path"}"""
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeRowAsync (fun row -> row.int "it")
    
    /// Queries to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =

        /// Determine if a document exists for the given ID
        let byId tableName docId sqlProps : Task<bool> =
            sqlProps
            |> Sql.query $"SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE id = @id) AS it"
            |> Sql.parameters [ "@id", Sql.string docId ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

        /// Determine if a document exists using a JSON containment query (@>)
        let byContains tableName (criteria : obj) sqlProps : Task<bool> =
            sqlProps
            |> Sql.query $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {Query.whereDataContains "@it"}) AS it"""
            |> Sql.parameters [ "@it", Query.jsonbDocParam criteria ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

        /// Determine if a document exists using a JSON Path match query (@?)
        let byJsonPath tableName jsonPath sqlProps : Task<bool> =
            sqlProps
            |> Sql.query
                $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {Query.whereJsonPathMatches "@path"}) AS it"""
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeRowAsync (fun row -> row.bool "it")

    /// Queries to determine if documents exist
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve a document by its ID
        let byId<'T> tableName docId sqlProps : Task<'T option> = backgroundTask {
            let! results =
                sqlProps
                |> Sql.query $"{Query.selectFromTable tableName} WHERE id = @id"
                |> Sql.parameters [ "@id", Sql.string docId ]
                |> Sql.executeAsync fromData<'T>
            return List.tryHead results
        }

        /// Execute a JSON containment query (@>)
        let byContains<'T> tableName (criteria : obj) sqlProps : Task<'T list> =
            sqlProps
            |> Sql.query $"""{Query.selectFromTable tableName} WHERE {Query.whereDataContains "@it"}"""
            |> Sql.parameters [ "@it", Query.jsonbDocParam criteria ]
            |> Sql.executeAsync fromData<'T>

        /// Execute a JSON Path match query (@?)
        let byJsonPath<'T> tableName jsonPath sqlProps : Task<'T list> =
            sqlProps
            |> Sql.query $"""{Query.selectFromTable tableName} WHERE {Query.whereJsonPathMatches "@path"}"""
            |> Sql.parameters [ "@path", Sql.string jsonPath ]
            |> Sql.executeAsync fromData<'T>

    /// Queries to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        let byId tableName docId sqlProps =
            executeNonQuery $"DELETE FROM %s{tableName} WHERE id = @id" docId {||} sqlProps

        /// Delete documents by matching a JSON contains query (@>)
        let byContains tableName (criteria : obj) sqlProps =
            executeNonQuery $"""DELETE FROM %s{tableName} WHERE {Query.whereDataContains "@data"}""" "" criteria
                sqlProps

        /// Delete documents by matching a JSON Path match query (@?)
        let byJsonPath tableName path sqlProps = backgroundTask {
            let _ =
                sqlProps
                |> Sql.query $"""DELETE FROM %s{tableName} WHERE {Query.whereJsonPathMatches "@path"}"""
                |> Sql.parameters [ "@path", Sql.string path ]
                |> Sql.executeNonQueryAsync
            ()
        }


/// Retrieve all documents in the given table
let all<'T> tableName =
    WithProps.all<'T> tableName (fromDataSource ())

/// Insert a new document
let insert<'T> tableName docId (document : 'T) =
    WithProps.insert tableName docId document (fromDataSource ())

/// Update a document
let update<'T> tableName docId (document : 'T) =
    WithProps.update<'T> tableName docId document (fromDataSource ())

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let save<'T> tableName docId (document : 'T) =
    WithProps.save<'T> tableName docId document (fromDataSource ())


/// Queries to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count matching documents using a JSON containment query (@>)
    let byContains tableName criteria =
        WithProps.Count.byContains tableName criteria (fromDataSource ())

    /// Count matching documents using a JSON Path match query (@?)
    let byJsonPath tableName jsonPath =
        WithProps.Count.byJsonPath tableName jsonPath (fromDataSource ())


/// Queries to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    let byId tableName docId =
        WithProps.Exists.byId tableName docId (fromDataSource ())
    
    /// Determine if a document exists using a JSON containment query (@>)
    let byContains tableName criteria =
        WithProps.Exists.byContains tableName criteria (fromDataSource ())

    /// Determine if a document exists using a JSON Path match query (@?)
    let byJsonPath tableName jsonPath =
        WithProps.Exists.byJsonPath tableName jsonPath (fromDataSource ())


/// Queries to retrieve documents
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve a document by its ID
    let byId<'T> tableName docId =
        WithProps.Find.byId<'T> tableName docId (fromDataSource ())

    /// Execute a JSON containment query (@>)
    let byContains<'T> tableName criteria =
        WithProps.Find.byContains<'T> tableName criteria (fromDataSource ())

    let byJsonPath<'T> tableName jsonPath =
        WithProps.Find.byJsonPath<'T> tableName jsonPath (fromDataSource ())


/// Queries to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    let byId tableName docId =
        WithProps.Delete.byId tableName docId (fromDataSource ())

    /// Delete documents by matching a JSON contains query (@>)
    let byContains tableName (doc : obj) =
        WithProps.Delete.byContains tableName doc (fromDataSource ())

    /// Delete documents by matching a JSON Path match query (@?)
    let byJsonPath tableName path =
        WithProps.Delete.byJsonPath tableName path (fromDataSource ())
