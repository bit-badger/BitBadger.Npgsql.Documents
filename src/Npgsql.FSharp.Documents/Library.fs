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


/// Data definition
[<RequireQualifiedAccess>]
module Definition =

    /// SQL statement to create a document table
    let createTable name =
        $"CREATE TABLE IF NOT EXISTS %s{name} (id TEXT NOT NULL PRIMARY KEY, data JSONB NOT NULL)"
    
    /// Create a document table
    let ensureTable name sqlProps = backgroundTask {
        let! _ = sqlProps |> Sql.query (createTable name) |> Sql.executeNonQueryAsync
        ()
    }

    /// SQL statement to create an index on documents in the specified table
    let createIndex (name : string) idxType =
        let extraOps = match idxType with Full -> "" | Optimized -> " jsonb_path_ops"
        let tableName = name.Split(".") |> Array.last
        $"CREATE INDEX IF NOT EXISTS idx_{tableName} ON {name} USING GIN (data{extraOps})"
    
    /// Create an index on documents in the specified table
    let ensureIndex (name : string) idxType sqlProps = backgroundTask {
        let! _ = sqlProps |> Sql.query (createIndex name idxType) |> Sql.executeNonQueryAsync
        ()
    }

/// Create a domain item from a document, specifying the field in which the document is found
let fromDocument<'T> field (row : RowReader) : 'T =
    Configuration.serializer.Deserialize<'T> (row.string field)
    
/// Create a domain item from a document
let fromData<'T> row : 'T =
    fromDocument "data" row

/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    open System.Threading.Tasks

    // ~~ BUILDING BLOCKS ~~

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
    
    // ~~ DOCUMENT RETRIEVAL QUERIES ~~

    let private fromDataSource () =
        Configuration.dataSource () |> Sql.fromDataSource

    /// Retrieve all documents in the given table
    let allWithProps<'T> tableName sqlProps : Task<'T list> =
        sqlProps
        |> Sql.query $"SELECT data FROM %s{tableName}"
        |> Sql.executeAsync fromData<'T>
    
    /// Retrieve all documents in the given table
    let all<'T> tableName =
        allWithProps<'T> tableName (fromDataSource ())
    
    /// Count matching documents using a JSON containment query (@>)
    let countByContainsWithProps tableName (criteria : obj) sqlProps : Task<int> =
        sqlProps
        |> Sql.query $"""SELECT COUNT(id) AS row_count FROM %s{tableName} WHERE {whereDataContains "@criteria"}"""
        |> Sql.parameters [ "@criteria", jsonbDocParam criteria ]
        |> Sql.executeRowAsync (fun row -> row.int "row_count")
    
    /// Count matching documents using a JSON containment query (@>)
    let countByContains tableName (criteria : obj) =
        countByContainsWithProps tableName criteria (fromDataSource ())
    
    /// Count matching documents using a JSON Path match query (@?)
    let countByJsonPathWithProps tableName jsonPath sqlProps : Task<int> =
        sqlProps
        |> Sql.query $"""SELECT COUNT(id) AS row_count FROM %s{tableName} WHERE {whereJsonPathMatches "@jsonPath"}"""
        |> Sql.parameters [ "@jsonPath", Sql.string jsonPath ]
        |> Sql.executeRowAsync (fun row -> row.int "row_count")
    
    /// Count matching documents using a JSON Path match query (@?)
    let countByJsonPath tableName jsonPath =
        countByJsonPathWithProps tableName jsonPath (fromDataSource ())
    
    /// Determine if a document exists for the given ID
    let existsByIdWithProps tableName docId sqlProps : Task<bool> =
        sqlProps
        |> Sql.query $"SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE id = @id) AS xist"
        |> Sql.parameters [ "@id", Sql.string docId ]
        |> Sql.executeRowAsync (fun row -> row.bool "xist")
    
    /// Determine if a document exists for the given ID
    let existsById tableName docId =
        existsByIdWithProps tableName docId (fromDataSource ())
        
    /// Determine if a document exists using a JSON containment query (@>)
    let existsByContainsWithProps tableName (criteria : obj) sqlProps : Task<bool> =
        sqlProps
        |> Sql.query $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereDataContains "@criteria"}) AS xist"""
        |> Sql.parameters [ "@criteria", jsonbDocParam criteria ]
        |> Sql.executeRowAsync (fun row -> row.bool "xist")
    
    /// Determine if a document exists using a JSON containment query (@>)
    let existsByContains tableName (criteria : obj) =
        existsByContainsWithProps tableName criteria (fromDataSource ())
    
    /// Determine if a document exists using a JSON Path match query (@?)
    let existsByJsonPathWithProps tableName jsonPath sqlProps : Task<bool> =
        sqlProps
        |> Sql.query $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereJsonPathMatches "@jsonPath"}) AS xist"""
        |> Sql.parameters [ "@criteria", Sql.string jsonPath ]
        |> Sql.executeRowAsync (fun row -> row.bool "xist")
    
    /// Determine if a document exists using a JSON Path match query (@?)
    let existsByJsonPath tableName jsonPath =
        existsByJsonPathWithProps tableName jsonPath (fromDataSource ())
    
    /// Execute a JSON containment query (@>)
    let findByContainsWithProps<'T> tableName value sqlProps : Task<'T list> =
        sqlProps
        |> Sql.query $"""{selectFromTable tableName} WHERE {whereDataContains "@criteria"}"""
        |> Sql.parameters [ "@criteria", jsonbDocParam value ]
        |> Sql.executeAsync fromData<'T>
    
    /// Execute a JSON containment query (@>)
    let findByContains<'T> tableName value =
        findByContainsWithProps<'T> tableName value (fromDataSource ())
    
    /// Execute a JSON Path match query (@?)
    let findByJsonPathWithProps<'T> tableName jsonPath sqlProps : Task<'T list> =
        sqlProps
        |> Sql.query $"""{selectFromTable tableName} WHERE {whereJsonPathMatches "@jsonPath"}"""
        |> Sql.parameters [ "@jsonPath", Sql.string jsonPath ]
        |> Sql.executeAsync fromData<'T>
    
    let findByJsonPath<'T> tableName jsonPath =
        findByJsonPathWithProps<'T> tableName jsonPath (fromDataSource ())
    
    /// Retrieve a document by its ID
    let tryByIdWithProps<'T> tableName idValue sqlProps : Task<'T option> = backgroundTask {
        let! results =
            sqlProps
            |> Sql.query $"{selectFromTable tableName} WHERE id = @id"
            |> Sql.parameters [ "@id", Sql.string idValue ]
            |> Sql.executeAsync fromData<'T>
        return List.tryHead results
    }

    /// Retrieve a document by its ID
    let tryById<'T> tableName idValue =
        tryByIdWithProps<'T> tableName idValue (fromDataSource ())

    // ~~ DOCUMENT MANIPULATION QUERIES ~~

    /// Execute a non-query statement to manipulate a document
    let private executeNonQuery query docId (document : 'T) sqlProps = backgroundTask {
        let! _ =
            sqlProps
            |> Sql.query query
            |> Sql.parameters (docParameters docId document)
            |> Sql.executeNonQueryAsync
        ()
    }

    /// Query to insert a document
    let insertQuery tableName =
        $"INSERT INTO %s{tableName} (id, data) VALUES (@id, @data)"
    
    /// Insert a new document
    let insertWithProps<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (insertQuery tableName) docId document sqlProps

    /// Insert a new document
    let insert<'T> tableName docId (document : 'T) =
        insertWithProps tableName docId document (fromDataSource ())
    
    /// Query to update a document
    let updateQuery tableName =
        $"UPDATE %s{tableName} SET data = @data WHERE id = @id"
    
    /// Update a document
    let updateWithProps<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (updateQuery tableName) docId document sqlProps

    /// Update a document
    let update<'T> tableName docId (document : 'T) =
        updateWithProps<'T> tableName docId document (fromDataSource ())
    
    /// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let saveQuery tableName =
        $"INSERT INTO %s{tableName} (id, data) VALUES (@id, @data) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data"
    
    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let saveWithProps<'T> tableName docId (document : 'T) sqlProps =
        executeNonQuery (saveQuery tableName) docId document sqlProps

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save<'T> tableName docId (document : 'T) =
        saveWithProps<'T> tableName docId document (fromDataSource ())
    
    /// Delete a document by its ID
    let deleteByIdWithProps tableName docId sqlProps =
        executeNonQuery $"DELETE FROM %s{tableName} WHERE id = @id" docId {||} sqlProps
    
    /// Delete a document by its ID
    let deleteById tableName docId =
        deleteByIdWithProps tableName docId (fromDataSource ())
    
    /// Delete documents by matching a JSON contains query (@>)
    let deleteByContainsWithProps tableName (doc : obj) sqlProps =
        executeNonQuery $"""DELETE FROM %s{tableName} WHERE {whereDataContains "@data"}""" "" doc sqlProps
    
    /// Delete documents by matching a JSON contains query (@>)
    let deleteByContains tableName (doc : obj) =
        deleteByContainsWithProps tableName doc (fromDataSource ())
    
    /// Delete documents by matching a JSON Path match query (@?)
    let deleteByJsonPathWithProps tableName path sqlProps = backgroundTask {
        let _ =
            sqlProps
            |> Sql.query $"""DELETE FROM %s{tableName} WHERE {whereJsonPathMatches "@data"}"""
            |> Sql.parameters [ "@data", Sql.string path ]
            |> Sql.executeNonQueryAsync
        ()
    }

    /// Delete documents by matching a JSON Path match query (@?)
    let deleteByJsonPath tableName path =
        deleteByJsonPathWithProps tableName path (fromDataSource ())
    