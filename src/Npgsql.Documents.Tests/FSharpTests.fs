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
        Sub : SubDocument
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
        |> Sql.password dbPassword
        |> Sql.formatConnectionString


/// Build the throwaway database
let buildDatabase () =
    
    let database = ThrowawayDatabase.Create(Db.connStr)

    database.ConnectionString
    |> Sql.connect
    |> Sql.query (Definition.createTable tableName)
    |> Sql.executeNonQuery
    |> ignore

    (NpgsqlDataSourceBuilder database.ConnectionString).Build ()
    |> Configuration.useDataSource

    database

/// Tests which do not hit the database
let unitTests =
    testList "Unit Tests" [
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
                Expect.equal (Query.whereJsonPathMatches "@path") "data @? @path" "WHERE clause not correct"
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
                Expect.equal (Option.get before) { Foo = "green"; Bar = "" } "The document is not correct"

                do! update tableName "test" { Foo = "blue"; Bar = "red" }
                let! after = Find.byId<SubDocument> tableName "test"
                if Option.isNone after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal (Option.get after) { Foo = "blue"; Bar = "red" } "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use db = buildDatabase ()

                let! before = Find.byId<SubDocument> tableName "test"
                Expect.isNone before "There should not have been a document returned"
                
                // This not raising an exception is the test
                do! update tableName "test" { Foo = "blue"; Bar = "red" }
            }
        ]
    ]


let all = testList "Npgsql.FSharp.Documents" [ unitTests ]
