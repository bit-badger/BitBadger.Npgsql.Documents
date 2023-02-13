module FSharpTests

open System
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
        Sub : SubDocument option
    }

/// Tests which do not hit the database
let unitTests =
    testList "Unit" [
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
                Expect.equal (Definition.createTable Db.tableName)
                    $"CREATE TABLE IF NOT EXISTS {Db.tableName} (id TEXT NOT NULL PRIMARY KEY, data JSONB NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "createIndex succeeds for full index" {
                Expect.equal (Definition.createIndex "schema.tbl" Full)
                    "CREATE INDEX IF NOT EXISTS idx_tbl ON schema.tbl USING GIN (data)"
                    "CREATE INDEX statement not constructed correctly"
            }
            test "createIndex succeeds for JSONB Path Ops index" {
                Expect.equal (Definition.createIndex Db.tableName Optimized)
                    $"CREATE INDEX IF NOT EXISTS idx_{Db.tableName} ON {Db.tableName} USING GIN (data jsonb_path_ops)"
                    "CREATE INDEX statement not constructed correctly"
            }
        ]
        testList "Query" [
            test "selectFromTable succeeds" {
                Expect.equal (Query.selectFromTable Db.tableName) $"SELECT data FROM {Db.tableName}"
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
                Expect.equal (Query.insert Db.tableName) $"INSERT INTO {Db.tableName} (id, data) VALUES (@id, @data)"
                    "INSERT statement not correct"
            }
            test "save succeeds" {
                Expect.equal (Query.save Db.tableName)
                    $"INSERT INTO {Db.tableName} (id, data) VALUES (@id, @data) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "all succeeds" {
                    Expect.equal (Query.Count.all Db.tableName) $"SELECT COUNT(id) AS it FROM {Db.tableName}"
                        "Count query not correct"
                }
                test "byContains succeeds" {
                    Expect.equal (Query.Count.byContains Db.tableName)
                        $"SELECT COUNT(id) AS it FROM {Db.tableName} WHERE data @> @criteria"
                        "JSON containment count query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Count.byJsonPath Db.tableName)
                        $"SELECT COUNT(id) AS it FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "JSON Path match count query not correct"
                }
            ]
            testList "Exists" [
                test "byId succeeds" {
                    Expect.equal (Query.Exists.byId Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE id = @id) AS it"
                        "ID existence query not correct"
                }
                test "byContains succeeds" {
                    Expect.equal (Query.Exists.byContains Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data @> @criteria) AS it"
                        "JSON containment exists query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Exists.byJsonPath Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data @? @path::jsonpath) AS it"
                        "JSON Path match existence query not correct"
                }
            ]
            testList "Find" [
                test "byId succeeds" {
                    Expect.equal (Query.Find.byId Db.tableName) $"SELECT data FROM {Db.tableName} WHERE id = @id"
                        "SELECT by ID query not correct"
                }
                test "byContains succeeds" {
                    Expect.equal (Query.Find.byContains Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data @> @criteria"
                        "SELECT by JSON containment query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Find.byJsonPath Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "SELECT by JSON Path match query not correct"
                }
            ]
            testList "Update" [
                test "full succeeds" {
                    Expect.equal (Query.Update.full Db.tableName)
                        $"UPDATE {Db.tableName} SET data = @data WHERE id = @id" "UPDATE full statement not correct"
                }
                test "partialById succeeds" {
                    Expect.equal (Query.Update.partialById Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE id = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "partialByContains succeeds" {
                    Expect.equal (Query.Update.partialByContains Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE data @> @criteria"
                        "UPDATE partial by JSON containment statement not correct"
                }
                test "partialByJsonPath succeeds" {
                    Expect.equal (Query.Update.partialByJsonPath Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE data @? @path::jsonpath"
                        "UPDATE partial by JSON Path statement not correct"
                }
            ]
            testList "Delete" [
                test "byId succeeds" {
                    Expect.equal (Query.Delete.byId Db.tableName) $"DELETE FROM {Db.tableName} WHERE id = @id"
                        "DELETE by ID query not correct"
                }
                test "byContains succeeds" {
                    Expect.equal (Query.Delete.byContains Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data @> @criteria"
                        "DELETE by JSON containment query not correct"
                }
                test "byJsonPath succeeds" {
                    Expect.equal (Query.Delete.byJsonPath Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "DELETE by JSON Path match query not correct"
                }
            ]
        ]
    ]

let isTrue<'T> (_ : 'T) = true

let integrationTests =
    let documents = [
        { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
        { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
        { Id = "three"; Value = ""; NumValue = 4; Sub = None }
        { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
        { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! insert Db.tableName doc.Id doc
    }
    testList "Integration" [
        testList "Definition" [
            testTask "ensureTable succeeds" {
                use db = Db.buildDatabase ()
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
                use db = Db.buildDatabase ()
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
                use db = Db.buildDatabase ()

                do! insert Db.tableName "abc" { Foo = "one"; Bar = "two" }
                do! insert Db.tableName "def" { Foo = "three"; Bar = "four" }
                do! insert Db.tableName "ghi" { Foo = "five"; Bar = "six" }

                let! results = all<SubDocument> Db.tableName
                let expected = [
                    { Foo = "one"; Bar = "two" }
                    { Foo = "three"; Bar = "four" }
                    { Foo = "five"; Bar = "six" }
                ]
                Expect.equal results expected "There should have been 3 documents returned"
            }
            testTask "succeeds when there is no data" {
                use db = Db.buildDatabase ()
                let! results = all<SubDocument> Db.tableName
                Expect.equal results [] "There should have been no documents returned"
            }
        ]
        testList "insert" [
            testTask "succeeds" {
                use db = Db.buildDatabase ()
                let! before = all<SubDocument> Db.tableName
                Expect.equal before [] "There should be no documents in the table"

                do! insert Db.tableName "turkey" { Foo = "gobble"; Bar = "gobble" }
                let! after = all<SubDocument> Db.tableName
                Expect.equal after [ { Foo = "gobble"; Bar = "gobble"} ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db = Db.buildDatabase ()
                do! insert Db.tableName "test" { Foo = "blah"; Bar = "" }
                Expect.throws (fun () ->
                    insert Db.tableName "test" {Foo = "oof"; Bar = "" } |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use db = Db.buildDatabase ()
                let! before = all<SubDocument> Db.tableName
                Expect.equal before [] "There should be no documents in the table"

                do! save Db.tableName "test" { Foo = "a"; Bar = "b" }
                let! after = all<SubDocument> Db.tableName
                Expect.equal after [ { Foo = "a"; Bar = "b"} ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db = Db.buildDatabase ()
                do! insert Db.tableName "test" { Foo = "a"; Bar = "b" }

                let! before = Find.byId<SubDocument> Db.tableName "test"
                if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                Expect.equal before.Value { Foo = "a"; Bar = "b" } "The document is not correct"

                do! save Db.tableName "test" { Foo = "c"; Bar = "d" }
                let! after = Find.byId<SubDocument> Db.tableName "test"
                if Option.isNone after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value { Foo = "c"; Bar = "d" } "The updated document is not correct"
            }
        ]
        testList "Count" [
            testTask "all succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.all Db.tableName
                Expect.equal theCount 5 "There should have been 5 matching documents"
            }
            testTask "byContains succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.byContains Db.tableName {| Value = "purple" |}
                Expect.equal theCount 2 "There should have been 2 matching documents"
            }
            testTask "byJsonPath succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.byJsonPath Db.tableName "$.NumValue ? (@ > 5)"
                Expect.equal theCount 3 "There should have been 3 matching documents"
            }
        ]
        testList "Exists" [
            testList "byId" [
                testTask "succeeds when a document exists" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byId Db.tableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byId Db.tableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byContains Db.tableName {| NumValue = 10 |}
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byContains Db.tableName {| Nothing = "none" |}
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath Db.tableName """$.Sub.Foo ? (@ == "green")"""
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.byJsonPath Db.tableName "$.NumValue ? (@ > 1000)"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "byId" [
                testTask "succeeds when a document is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.byId<JsonDocument> Db.tableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.byId<JsonDocument> Db.tableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byContains<JsonDocument> Db.tableName {| Sub = {| Foo = "green" |} |}
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byContains<JsonDocument> Db.tableName {| Value = "mauve" |}
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath<JsonDocument> Db.tableName "$.NumValue ? (@ < 15)"
                    Expect.equal (List.length docs) 3 "There should have been 3 documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.byJsonPath<JsonDocument> Db.tableName "$.NumValue ? (@ < 0)"
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
        ]
        testList "Update" [
            testList "full" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Update.full Db.tableName "one" { Foo = "blue"; Bar = "red" }
                    let! after = Find.byId<SubDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value { Foo = "blue"; Bar = "red" } "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.full Db.tableName "test" { Foo = "blue"; Bar = "red" }
                }
            ]
            testList "partialById" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.partialById Db.tableName "one" {| NumValue = 44 |}
                    let! after = Find.byId<JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialById Db.tableName "test" {| Foo = "green" |}
                }
            ]
            testList "partialByContains" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.partialByContains Db.tableName {| Value = "purple" |} {| NumValue = 77 |}
                    let! after = Count.byContains Db.tableName {| NumValue = 77 |}
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByContains Db.tableName {| Value = "burgundy" |} {| Foo = "green" |}
                }
            ]
            testList "partialByJsonPath" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.partialByJsonPath Db.tableName "$.NumValue ? (@ > 10)" {| NumValue = 1000 |}
                    let! after = Count.byJsonPath Db.tableName "$.NumValue ? (@ > 999)"
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByContains Db.tableName {| Value = "burgundy" |} {| Foo = "green" |}
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byId Db.tableName "four"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 4 "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byId Db.tableName "thirty"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byContains" [
                testTask "succeeds when documents are deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byContains Db.tableName {| Value = "purple" |}
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byContains Db.tableName {| Value = "crimson" |}
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "byJsonPath" [
                testTask "succeeds when documents are deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byJsonPath Db.tableName """$.Sub.Foo ? (@ == "green")"""
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.byJsonPath Db.tableName "$.NumValue ? (@ > 100)"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
        ]
    ]
    |> testSequenced


let all = testList "FSharp.Documents" [ unitTests; integrationTests ]
