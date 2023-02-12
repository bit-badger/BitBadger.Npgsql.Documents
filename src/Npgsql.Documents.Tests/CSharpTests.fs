module CSharpTests

open Expecto
open Npgsql.Documents
open Npgsql.FSharp

[<AllowNullLiteral>]
type SubDocument () =
    member val Foo = "" with get, set
    member val Bar = "" with get, set

[<AllowNullLiteral>]
type JsonDocument() =
    member val Id = "" with get, set
    member val Value = "" with get, set
    member val NumValue = 0 with get, set
    member val Sub : SubDocument option = None with get, set


/// Tests which do not hit the database
let unitTests =
    testList "Unit" [
        testList "Definition" [
            test "CreateTable succeeds" {
                Expect.equal (Definition.CreateTable Db.tableName)
                    $"CREATE TABLE IF NOT EXISTS {Db.tableName} (id TEXT NOT NULL PRIMARY KEY, data JSONB NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "CreateIndex succeeds for full index" {
                Expect.equal (Definition.CreateIndex ("schema.tbl", DocumentIndex.Full))
                    "CREATE INDEX IF NOT EXISTS idx_tbl ON schema.tbl USING GIN (data)"
                    "CREATE INDEX statement not constructed correctly"
            }
            test "CreateIndex succeeds for JSONB Path Ops index" {
                Expect.equal (Definition.CreateIndex (Db.tableName, DocumentIndex.Optimized))
                    $"CREATE INDEX IF NOT EXISTS idx_{Db.tableName} ON {Db.tableName} USING GIN (data jsonb_path_ops)"
                    "CREATE INDEX statement not constructed correctly"
            }
        ]
        testList "Query" [
            test "SelectFromTable succeeds" {
                Expect.equal (Query.SelectFromTable Db.tableName) $"SELECT data FROM {Db.tableName}"
                    "SELECT statement not correct"
            }
            test "WhereDataContains succeeds" {
                Expect.equal (Query.WhereDataContains "@test") "data @> @test" "WHERE clause not correct"
            }
            test "WhereJsonPathMatches succeds" {
                Expect.equal (Query.WhereJsonPathMatches "@path") "data @? @path::jsonpath" "WHERE clause not correct"
            }
            test "JsonbDocParam succeeds" {
                Expect.equal (Query.JsonbDocParam {| Hello = "There" |}) (Sql.jsonb "{\"Hello\":\"There\"}")
                    "JSONB document not serialized correctly"
            }
            test "DocParameters succeeds" {
                let parameters = Query.DocParameters ("abc123", {| Testing = 456 |})
                let expected = [
                    "@id", Sql.string "abc123"
                    "@data", Sql.jsonb "{\"Testing\":456}"
                ]
                Expect.equal parameters expected "There should have been 2 parameters, one string and one JSONB"
            }
            test "Insert succeeds" {
                Expect.equal (Query.Insert Db.tableName) $"INSERT INTO {Db.tableName} (id, data) VALUES (@id, @data)"
                    "INSERT statement not correct"
            }
            test "Update succeeds" {
                Expect.equal (Query.Update Db.tableName) $"UPDATE {Db.tableName} SET data = @data WHERE id = @id"
                    "UPDATE statement not correct"
            }
            test "Save succeeds" {
                Expect.equal (Query.Save Db.tableName)
                    $"INSERT INTO {Db.tableName} (id, data) VALUES (@id, @data) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
        ]
    ]

let isTrue<'T> (_ : 'T) = true

open Document

let integrationTests =
    let documents = [
        JsonDocument(Id = "one", Value = "FIRST!", NumValue = 0)
        JsonDocument(Id = "two", Value = "again", NumValue = 10, Sub = Some (SubDocument (Foo = "green", Bar = "blue")))
        JsonDocument(Id = "three", Value = "", NumValue = 4)
        JsonDocument(Id = "four", Value = "purple", NumValue = 17, Sub = Some (SubDocument (Foo = "green", Bar = "red")))
        JsonDocument(Id = "five", Value = "purple", NumValue = 18)
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! Insert (Db.tableName, doc.Id, doc)
    }
    testList "Integration" [
        testList "Definition" [
            testTask "EnsureTable succeeds" {
                use db = Db.buildDatabase ()
                let tableExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists = tableExists ()
                Expect.isFalse exists "The table should not exist already"

                do! Definition.EnsureTable "ensured"
                let! exists' = tableExists ()
                Expect.isTrue exists' "The table should now exist"
            }
            testTask "EnsureIndex succeeds" {
                use db = Db.buildDatabase ()
                let indexExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists = indexExists ()
                Expect.isFalse exists "The index should not exist already"

                do! Definition.EnsureTable "ensured"
                do! Definition.EnsureIndex ("ensured", DocumentIndex.Optimized)
                let! exists' = indexExists ()
                Expect.isTrue exists' "The index should now exist"
                // TODO: check for GIN(jsonp_path_ops), write test for "full" index that checks for their absence
            }
        ]
        testList "Document.All" [
            testTask "succeeds when there is data" {
                use db = Db.buildDatabase ()

                do! Insert (Db.tableName, "abc", SubDocument (Foo = "one", Bar = "two"))
                do! Insert (Db.tableName, "def", SubDocument (Foo = "three", Bar = "four"))
                do! Insert (Db.tableName, "ghi", SubDocument (Foo = "five", Bar = "six"))

                let! results = All<SubDocument> Db.tableName
                Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
            }
            testTask "succeeds when there is no data" {
                use db = Db.buildDatabase ()
                let! results = All<SubDocument> Db.tableName
                Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
            }
        ]
        testList "Document.Insert" [
            testTask "succeeds" {
                use db = Db.buildDatabase ()
                let! before = All<SubDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"

                do! Insert (Db.tableName, "turkey", SubDocument (Foo = "gobble", Bar = "gobble"))
                let! after = All<SubDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db = Db.buildDatabase ()
                do! Insert (Db.tableName, "test", SubDocument (Foo = "blah", Bar = ""))
                Expect.throws (fun () ->
                    Insert (Db.tableName, "test", SubDocument (Foo = "oof", Bar = ""))
                    |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "Document.Update" [
            testTask "succeeds when a document is updated" {
                use db = Db.buildDatabase ()
                do! Insert (Db.tableName, "test", SubDocument (Foo = "green", Bar = ""))

                let! before = Find.ById<SubDocument> (Db.tableName, "test")
                if isNull before then Expect.isTrue false "There should have been a document returned"
                Expect.equal (before :> SubDocument).Foo "green" "The document is not correct"
                Expect.equal (before :> SubDocument).Bar "" "The document is not correct"

                do! Update (Db.tableName, "test", SubDocument (Foo = "blue", Bar = "red"))
                let! after = Find.ById<SubDocument> (Db.tableName, "test")
                if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal (after :> SubDocument).Foo "blue" "The updated document is not correct"
                Expect.equal (after :> SubDocument).Bar "red" "The updated document is not correct"
            }
            testTask "succeeds when no document is updated" {
                use db = Db.buildDatabase ()

                let! before = Find.ById<SubDocument> (Db.tableName, "test")
                Expect.isNull before "There should not have been a document returned"
                
                // This not raising an exception is the test
                do! Update (Db.tableName, "test", SubDocument(Foo = "blue", Bar = "red"))
            }
        ]
        testList "Document.Save" [
            testTask "succeeds when a document is inserted" {
                use db = Db.buildDatabase ()
                let! before = All<SubDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"

                do! Save (Db.tableName, "test", SubDocument (Foo = "a", Bar = "b"))
                let! after = All<SubDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db = Db.buildDatabase ()
                do! Insert (Db.tableName, "test", SubDocument (Foo = "a", Bar = "b"))

                let! before = Find.ById<SubDocument> (Db.tableName, "test")
                if isNull before then Expect.isTrue false "There should have been a document returned"
                Expect.equal (before :> SubDocument).Foo "a" "The document is not correct"
                Expect.equal (before :> SubDocument).Bar "b" "The document is not correct"

                do! Save (Db.tableName, "test", SubDocument (Foo = "c", Bar = "d"))
                let! after = Find.ById<SubDocument> (Db.tableName, "test")
                if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal (after :> SubDocument).Foo "c" "The updated document is not correct"
                Expect.equal (after :> SubDocument).Bar "d" "The updated document is not correct"
            }
        ]
        testList "Document.Count" [
            testTask "All succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.All Db.tableName
                Expect.equal theCount 5 "There should have been 5 matching documents"
            }
            testTask "ByContains succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.ByContains (Db.tableName, {| Value = "purple" |})
                Expect.equal theCount 2 "There should have been 2 matching documents"
            }
            testTask "byJsonPath succeeds" {
                use db = Db.buildDatabase ()
                do! loadDocs ()

                let! theCount = Count.ByJsonPath (Db.tableName, "$.NumValue ? (@ > 5)")
                Expect.equal theCount 3 "There should have been 3 matching documents"
            }
        ]
        testList "Document.Exists" [
            testList "ById" [
                testTask "succeeds when a document exists" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ById (Db.tableName, "three")
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ById (Db.tableName, "seven")
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "ByContains" [
                testTask "succeeds when documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ByContains (Db.tableName, {| NumValue = 10 |})
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ByContains (Db.tableName, {| Nothing = "none" |})
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "ByJsonPath" [
                testTask "succeeds when documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ByJsonPath (Db.tableName, """$.Sub.Foo ? (@ == "green")""")
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! exists = Exists.ByJsonPath (Db.tableName, "$.NumValue ? (@ > 1000)")
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Document.Find" [
            testList "ById" [
                testTask "succeeds when a document is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.ById<JsonDocument> (Db.tableName, "two")
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.ById<JsonDocument> (Db.tableName, "three hundred eighty-seven")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "ByContains" [
                testTask "succeeds when documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.ByContains<JsonDocument> (Db.tableName, {| Sub = {| Foo = "green" |} |})
                    Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.ByContains<JsonDocument> (Db.tableName, {| Value = "mauve" |})
                    Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "ByJsonPath" [
                testTask "succeeds when documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.ByJsonPath<JsonDocument> (Db.tableName, "$.NumValue ? (@ < 15)")
                    Expect.hasCountOf docs 3u isTrue "There should have been 3 documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Find.ByJsonPath<JsonDocument> (Db.tableName, "$.NumValue ? (@ < 0)")
                    Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
                }
            ]
        ]
        testList "Document.Delete" [
            testList "ById" [
                testTask "succeeds when a document is deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ById (Db.tableName, "four")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 4 "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ById (Db.tableName, "thirty")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "ByContains" [
                testTask "succeeds when documents are deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ByContains (Db.tableName, {| Value = "purple" |})
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ByContains (Db.tableName, {| Value = "crimson" |})
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
            testList "ByJsonPath" [
                testTask "succeeds when documents are deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ByJsonPath (Db.tableName, """$.Sub.Foo ? (@ == "green")""")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 3 "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Delete.ByJsonPath (Db.tableName, "$.NumValue ? (@ > 100)")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5 "There should have been 5 documents remaining"
                }
            ]
        ]
    ]
    |> testSequenced


let all = testList "Documents" [ unitTests; integrationTests ]
