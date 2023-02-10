module FSharpTests

open Expecto
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

/// Build the throwaway database
let buildDatabase () =
    
    let database = ThrowawayDatabase.Create("")

    database.ConnectionString
    |> Sql.connect
    |> Sql.query (Definition.createTable tableName)
    |> Sql.executeNonQuery
    |> ignore

    database

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
        ]

    ]

let all = testList "Npgsql.FSharp.Documents" [ unitTests ]
