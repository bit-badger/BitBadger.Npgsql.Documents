module FSharpTests

open System
open Expecto
open Npgsql
open Npgsql.FSharp
open Npgsql.FSharp.Documents
open ThrowawayDb.Postgres

type SubDocument =
    {   Foo : string
        Bar : string
    }

type JsonDocument =
    {   Id : string
        Value : string
        NumValue : int
        Sub : SubDocument option
    }

/// The name of the table used for testing
let tableName = "test_table"

/// Database connection values
module Db =
    
    /// The host for the database
    let dbHost =
        match Environment.GetEnvironmentVariable "Npgsql.Documents.DbHost" with
        | host when String.IsNullOrWhiteSpace host -> "localhost"
        | host -> host

    /// The port for the database
    let dbPort =
        match Environment.GetEnvironmentVariable "Npgsql.Documents.DbPort" with
        | port when String.IsNullOrWhiteSpace port -> 5432
        | port -> Int32.Parse port

    /// The database itself
    let dbDatabase =
        match Environment.GetEnvironmentVariable "Npgsql.Documents.DbDatabase" with
        | db when String.IsNullOrWhiteSpace db -> "postgres"
        | db -> db

    /// The user to use in connecting to the database
    let dbUser =
        match Environment.GetEnvironmentVariable "Npgsql.Documents.DbUser" with
        | user when String.IsNullOrWhiteSpace user -> "postgres"
        | user -> user
    
    /// The password to use for the database
    let dbPassword =
        match Environment.GetEnvironmentVariable "Npgsql.Documents.DbPwd" with
        | pwd when String.IsNullOrWhiteSpace pwd -> "postgres"
        | pwd -> pwd

    /// The overall connection string
    let connStr =
        Sql.host dbHost
        |> Sql.port dbPort
        |> Sql.database dbDatabase
        |> Sql.username dbUser
        |> Sql.password dbPassword
        |> Sql.formatConnectionString
    
    /// Create a data source using the derived connection string
    let mkDataSource cStr =
        (NpgsqlDataSourceBuilder cStr).Build ()


/// Build the throwaway database
let buildDatabase () =
    
    let database = ThrowawayDatabase.Create(Db.connStr)

    database.ConnectionString
    |> Sql.connect
    |> Sql.query (Definition.createTable tableName)
    |> Sql.executeNonQuery
    |> ignore

    Configuration.useDataSource (Db.mkDataSource database.ConnectionString)

    database

/// Tests which do not hit the database
let unitTests =
    testList "Unit Tests" [
        testList "Configuration" [
            test "useDataSource disposes existing source" {
                use db1 = ThrowawayDatabase.Create Db.connStr
                let source = Db.mkDataSource db1.ConnectionString
                Configuration.useDataSource source

                use db2 = ThrowawayDatabase.Create Db.connStr
                Configuration.useDataSource (Db.mkDataSource db2.ConnectionString)
                Expect.throws (fun () -> source.OpenConnection () |> ignore) "Data source should have been disposed"
            }
        ]
        testList "Definition" [
            test "createTable succeeds" {
                Expect.equal (Definition.createTable tableName)
                    $"CREATE TABLE IF NOT EXISTS {tableName} (id TEXT NOT NULL PRIMARY KEY, data JSONB NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "createIndex succeeds for full index" {
                Expect.equal (Definition.createIndex "schema.tbl" Full)
                    "CREATE INDEX IF NOT EXISTS idx_tbl ON schema.tbl USING GIN (data)"
                    "CREATE INDEX statement not constructed correctly"
            }
            test "createIndex succeeds for JSONB Path Ops index" {
                Expect.equal (Definition.createIndex tableName Optimized)
                    $"CREATE INDEX IF NOT EXISTS idx_{tableName} ON {tableName} USING GIN (data jsonb_path_ops)"
                    "CREATE INDEX statement not constructed correctly"
            }
        ]
        testList "Query" [
            test "selectFromTable succeeds" {
                Expect.equal (Query.selectFromTable tableName) $"SELECT data FROM {tableName}"
                    "SELECT statement not correct"
            }
            test "whereDataContains succeeds" {
                Expect.equal (Query.whereDataContains "@test") "data @> @test" "WHERE clause not correct"
            }
            test "whereJsonPathMatches succeds" {
                Expect.equal (Query.whereJsonPathMatches "@path") "data @? @path::jsonpath" "WHERE clause not correct"
            }
            test "jsonbDocParam succeeds" {
                Expect.equal (Query.jsonbDocParam {| Hello = "There" |}) (Sql.jsonb "{\"Hello\":\"There\"}")
                    "JSONB document not serialized correctly"
            }
            test "docParameters succeeds" {
                let parameters = Query.docParameters "abc123" {| Testing = 456 |}
                let expected = [
                    "@id", Sql.string "abc123"
                    "@data", Sql.jsonb "{\"Testing\":456}"
                ]
                Expect.equal parameters expected "There should have been 2 parameters, one string and one JSONB"
            }
            test "insert succeeds" {
                Expect.equal (Query.insert tableName) $"INSERT INTO {tableName} (id, data) VALUES (@id, @data)"
                    "INSERT statement not correct"
            }
            test "update succeeds" {
                Expect.equal (Query.update tableName) $"UPDATE {tableName} SET data = @data WHERE id = @id"
                    "UPDATE statement not correct"
            }
            test "save succeeds" {
                Expect.equal (Query.save tableName)
                    $"INSERT INTO {tableName} (id, data) VALUES (@id, @data) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
        ]
    ]

let integrationTests =
    let documents = [
        { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
        { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
        { Id = "three"; Value = ""; NumValue = 4; Sub = None }
        { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
        { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! insert tableName doc.Id doc
    }
    testList "Integration Tests" [
        testList "Definition" [
            testTask "ensureTable succeeds" {
                use db = buildDatabase ()
                let tableExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists = tableExists ()
                Expect.isFalse exists "The table should not exist already"

                do! Definition.ensureTable "ensured"
                let! exists' = tableExists ()
                Expect.isTrue exists' "The table should now exist"
            }
            testTask "ensureIndex succeeds" {
                use db = buildDatabase ()
                let indexExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists = indexExists ()
                Expect.isFalse exists "The index should not exist already"

                do! Definition.ensureTable "ensured"
                do! Definition.ensureIndex "ensured" Optimized
                let! exists' = indexExists ()
                Expect.isTrue exists' "The index should now exist"
                // TODO: check for GIN(jsonp_path_ops), write test for "full" index that checks for their absence
            }
        ]
        testList "all" [
            testTask "succeeds when there is data" {
                use db = buildDatabase ()

                do! insert tableName "abc" { Foo = "one"; Bar = "two" }
                do! insert tableName "def" { Foo = "three"; Bar = "four" }
                do! insert tableName "ghi" { Foo = "five"; Bar = "six" }

                let! results = all<SubDocument> tableName
                let expected = [
                    { Foo = "one"; Bar = "two" }
                    { Foo = "three"; Bar = "four" }
                    { Foo = "five"; Bar = "six" }
                ]
                Expect.equal results expected "There should have been 3 documents returned"
            }
            testTask "succeeds when there is no data" {
                use db = buildDatabase ()
                let! results = all<SubDocument> tableName
                Expect.equal results [] "There should have been no documents returned"
            }
        ]
        testList "insert" [
            testTask "succeeds" {
                use db = buildDatabase ()
                let! before = all<SubDocument> tableName
                Expect.equal before [] "There should be no documents in the table"

                do! insert tableName "turkey" { Foo = "gobble"; Bar = "gobble" }
                let! after = all<SubDocument> tableName
                Expect.equal after [ { Foo = "gobble"; Bar = "gobble"} ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db = buildDatabase ()
                do! insert tableName "test" { Foo = "blah"; Bar = "" }
                Expect.throws (fun () ->
                    insert tableName "test" {Foo = "oof"; Bar = "" } |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "update" [
            testTask "succeeds when a document is updated" {
                use db = buildDatabase ()
                do! insert tableName "test" { Foo = "green"; Bar = "" }

                let! before = Find.byId<SubDocument> tableName "test"
                if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                Expect.equal before.Value { Foo = "green"; Bar = "" } "The document is not correct"

                do! update tableName "test" { Foo = "blue"; Bar = "red" }
                let! after = Find.byId<SubDocument> tableName "test"
                if Option.isNone after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value { Foo = "blue"; Bar = "red" } "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use db = buildDatabase ()

                let! before = Find.byId<SubDocument> tableName "test"
                Expect.isNone before "There should not have been a document returned"
                
                // This not raising an exception is the test
                do! update tableName "test" { Foo = "blue"; Bar = "red" }
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use db = buildDatabase ()
                let! before = all<SubDocument> tableName
                Expect.equal before [] "There should be no documents in the table"

                do! save tableName "test" { Foo = "a"; Bar = "b" }
                let! after = all<SubDocument> tableName
                Expect.equal after [ { Foo = "a"; Bar = "b"} ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db = buildDatabase ()
                do! insert tableName "test" { Foo = "a"; Bar = "b" }

                let! before = Find.byId<SubDocument> tableName "test"
                if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                Expect.equal before.Value { Foo = "a"; Bar = "b" } "The document is not correct"

                do! save tableName "test" { Foo = "c"; Bar = "d" }
                let! after = Find.byId<SubDocument> tableName "test"
                if Option.isNone after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value { Foo = "c"; Bar = "d" } "The updated document is not correct"
            }
        ]
        testList "Count" [
            testTask "all succeeds" {
                use db = buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.all tableName
                Expect.equal theCount 5 "There should have been 5 matching documents"
            }
            testTask "byContains succeeds" {
                use db = buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.byContains tableName {| Value = "purple" |}
                Expect.equal theCount 2 "There should have been 2 matching documents"
            }
            testTask "byJsonPath succeeds" {
                use db = buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.byJsonPath tableName "$.NumValue ? (@ > 5)"
                Expect.equal theCount 3 "There should have been 3 matching documents"
            }
        ]
        testList "Exists" [
            testList "byId" [
                testTask "succeeds when a document exists" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byId tableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byId tableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents exist" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byContains tableName {| NumValue = 10 |}
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byContains tableName {| Nothing = "none" |}
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents exist" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath tableName """$.Sub.Foo ? (@ == "green")"""
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath tableName "$.NumValue ? (@ > 1000)"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "byId" [
                testTask "succeeds when a document is found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.byId<JsonDocument> tableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.byId<JsonDocument> tableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byContains tableName {| Sub = {| Foo = "green" |} |}
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byContains tableName {| Value = "mauve" |}
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath tableName "$.NumValue ? (@ < 15)"
                    Expect.equal (List.length docs) 3 "There should have been 3 documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath tableName "$.NumValue ? (@ < 0)"
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byId tableName "four"
                    let! remaining = Count.all tableName
                    Expect.equal remaining 4 "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byId tableName "thirty"
                    let! remaining = Count.all tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byContains tableName {| Value = "purple" |}
                    let! remaining = Count.all tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byContains tableName {| Value = "crimson" |}
                    let! remaining = Count.all tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byJsonPath tableName """$.Sub.Foo ? (@ == "green")"""
                    let! remaining = Count.all tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = buildDatabase ()
                    do! loadDocs ()

                    let! remaining = Count.all tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                    do! Delete.byJsonPath tableName "$.NumValue ? (@ > 100)"
                }
            ]
        ]
    ]
    |> testSequenced


let all = testList "Npgsql.FSharp.Documents" [ unitTests; integrationTests ]
