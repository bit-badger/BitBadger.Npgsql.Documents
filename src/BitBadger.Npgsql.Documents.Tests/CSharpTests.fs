module CSharpTests

open System
open BitBadger.Npgsql.Documents
open Expecto
open Npgsql.FSharp
open ThrowawayDb.Postgres

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
                    $"CREATE TABLE IF NOT EXISTS {Db.tableName} (data JSONB NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "CreateKey succeeds" {
                Expect.equal (Definition.CreateKey Db.tableName)
                    $"CREATE UNIQUE INDEX IF NOT EXISTS idx_{Db.tableName}_key ON {Db.tableName} ((data -> 'id'))"
                    "CREATE INDEX for key statement not constructed properly"
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
            test "WhereById succeeds" {
                Expect.equal (Query.WhereById "@id") "data -> 'id' = @id" "WHERE clause not correct"
            }
            test "WhereDataContains succeeds" {
                Expect.equal (Query.WhereDataContains "@test") "data @> @test" "WHERE clause not correct"
            }
            test "WhereJsonPathMatches succeeds" {
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
                Expect.equal (Query.Insert Db.tableName) $"INSERT INTO {Db.tableName} VALUES (@data)"
                    "INSERT statement not correct"
            }
            test "Save succeeds" {
                Expect.equal (Query.Save Db.tableName)
                    $"INSERT INTO {Db.tableName} VALUES (@data) ON CONFLICT (data) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "All succeeds" {
                    Expect.equal (Query.Count.All Db.tableName) $"SELECT COUNT(*) AS it FROM {Db.tableName}"
                        "Count query not correct"
                }
                test "ByContains succeeds" {
                    Expect.equal (Query.Count.ByContains Db.tableName)
                        $"SELECT COUNT(*) AS it FROM {Db.tableName} WHERE data @> @criteria"
                        "JSON containment count query not correct"
                }
                test "ByJsonPath succeeds" {
                    Expect.equal (Query.Count.ByJsonPath Db.tableName)
                        $"SELECT COUNT(*) AS it FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "JSON Path match count query not correct"
                }
            ]
            testList "Exists" [
                test "ById succeeds" {
                    Expect.equal (Query.Exists.ById Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data -> 'id' = @id) AS it"
                        "ID existence query not correct"
                }
                test "ByContains succeeds" {
                    Expect.equal (Query.Exists.ByContains Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data @> @criteria) AS it"
                        "JSON containment exists query not correct"
                }
                test "ByJsonPath succeeds" {
                    Expect.equal (Query.Exists.ByJsonPath Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data @? @path::jsonpath) AS it"
                        "JSON Path match existence query not correct"
                }
            ]
            testList "Find" [
                test "ById succeeds" {
                    Expect.equal (Query.Find.ById Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data -> 'id' = @id" "SELECT by ID query not correct"
                }
                test "ByContains succeeds" {
                    Expect.equal (Query.Find.ByContains Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data @> @criteria"
                        "SELECT by JSON containment query not correct"
                }
                test "ByJsonPath succeeds" {
                    Expect.equal (Query.Find.ByJsonPath Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "SELECT by JSON Path match query not correct"
                }
            ]
            testList "Update" [
                test "Full succeeds" {
                    Expect.equal (Query.Update.Full Db.tableName)
                        $"UPDATE {Db.tableName} SET data = @data WHERE data -> 'id' = @id"
                        "UPDATE full statement not correct"
                }
                test "PartialById succeeds" {
                    Expect.equal (Query.Update.PartialById Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE data -> 'id' = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "PartialByContains succeeds" {
                    Expect.equal (Query.Update.PartialByContains Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE data @> @criteria"
                        "UPDATE partial by JSON containment statement not correct"
                }
                test "PartialByJsonPath succeeds" {
                    Expect.equal (Query.Update.PartialByJsonPath Db.tableName)
                        $"UPDATE {Db.tableName} SET data = data || @data WHERE data @? @path::jsonpath"
                        "UPDATE partial by JSON Path statement not correct"
                }
            ]
            testList "Delete" [
                test "ById succeeds" {
                    Expect.equal (Query.Delete.ById Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data -> 'id' = @id"
                        "DELETE by ID query not correct"
                }
                test "ByContains succeeds" {
                    Expect.equal (Query.Delete.ByContains Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data @> @criteria"
                        "DELETE by JSON containment query not correct"
                }
                test "ByJsonPath succeeds" {
                    Expect.equal (Query.Delete.ByJsonPath Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data @? @path::jsonpath"
                        "DELETE by JSON Path match query not correct"
                }
            ]
        ]
    ]

let isTrue<'T> (_ : 'T) = true

open Document

module FS = BitBadger.Npgsql.FSharp.Documents

let integrationTests =
    let documents = [
        JsonDocument (Id = "one", Value = "FIRST!", NumValue = 0)
        JsonDocument (Id = "two", Value = "again", NumValue = 10,
                      Sub = Some (SubDocument (Foo = "green", Bar = "blue")))
        JsonDocument (Id = "three", Value = "", NumValue = 4)
        JsonDocument (Id = "four", Value = "purple", NumValue = 17,
                      Sub = Some (SubDocument (Foo = "green", Bar = "red")))
        JsonDocument (Id = "five", Value = "purple", NumValue = 18)
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! Insert (Db.tableName, doc)
    }
    testList "Integration" [
        testList "Configuration" [
            test "UseDataSource disposes existing source" {
                use db1 = ThrowawayDatabase.Create Db.connStr
                let source = Db.mkDataSource db1.ConnectionString
                Configuration.UseDataSource source

                use db2 = ThrowawayDatabase.Create Db.connStr
                Configuration.UseDataSource (Db.mkDataSource db2.ConnectionString)
                Expect.throws (fun () -> source.OpenConnection () |> ignore) "Data source should have been disposed"
            }
            test "DataSource returns configured data source" {
                use db = ThrowawayDatabase.Create Db.connStr
                let source = Db.mkDataSource db.ConnectionString
                Configuration.UseDataSource source

                Expect.isTrue (obj.ReferenceEquals (source, Configuration.DataSource ()))
                    "Data source should have been the same"
            }
            test "UseSerializer succeeds" {
                try
                    Configuration.UseSerializer
                        { new IDocumentSerializer with
                            member _.Serialize<'T> (it : 'T) : string = """{"Overridden":true}"""
                            member _.Deserialize<'T> (it : string) : 'T = Unchecked.defaultof<'T>
                        }
                    
                    let serialized = Configuration.Serializer().Serialize (SubDocument(Foo = "howdy", Bar = "bye"))
                    Expect.equal serialized """{"Overridden":true}""" "Specified serializer was not used"
                    
                    let deserialized = Configuration.Serializer().Deserialize<obj> """{"Something":"here"}"""
                    Expect.isNull deserialized "Specified serializer should have returned null"
                finally
                    Configuration.UseSerializer FS.Configuration.defaultSerializer
            }
            test "Serializer returns configured serializer" {
                Expect.isTrue
                    (obj.ReferenceEquals (FS.Configuration.defaultSerializer, Configuration.Serializer ()))
                    "Serializer should have been the same"
            }
            test "UseIdField / IdField succeeds" {
                Configuration.UseIdField "Id"
                Expect.equal (Configuration.IdField ()) "Id" "useIdField did not set the ID field"
            }
        ]
        testList "Definition" [
            testTask "EnsureTable succeeds" {
                use db = Db.buildDatabase ()
                let tableExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ensured') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                let keyExists () =
                    Sql.connect db.ConnectionString
                    |> Sql.query "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_ensured_key') AS it"
                    |> Sql.executeRowAsync (fun row -> row.bool "it")
                
                let! exists     = tableExists ()
                let! alsoExists = keyExists ()
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"

                do! Definition.EnsureTable "ensured"
                let! exists'     = tableExists ()
                let! alsoExists' = keyExists   ()
                Expect.isTrue exists'     "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
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
        testList "Document.Insert" [
            testTask "succeeds" {
                use db = Db.buildDatabase ()
                let! before = Find.All<SubDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"

                do! Insert (Db.tableName,
                            JsonDocument(Id = "turkey", Sub = Some (SubDocument (Foo = "gobble", Bar = "gobble"))))
                let! after = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use db = Db.buildDatabase ()
                let testDoc = JsonDocument (Id = "test", Sub = Some (SubDocument (Foo = "blah", Bar = "")))
                do! Insert (Db.tableName, testDoc)
                Expect.throws (fun () -> Insert (Db.tableName, testDoc) |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "Document.Save" [
            testTask "succeeds when a document is inserted" {
                use db = Db.buildDatabase ()
                let! before = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"

                do! Save (Db.tableName, JsonDocument(Id = "test"))
                let! after = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use db = Db.buildDatabase ()
                let testDoc = JsonDocument (Id = "test", Sub = Some (SubDocument (Foo = "a", Bar = "b")))
                do! Insert (Db.tableName, testDoc)

                let! before = Find.ById<JsonDocument> (Db.tableName, "test")
                if isNull before then Expect.isTrue false "There should have been a document returned"
                let before = before :> JsonDocument
                Expect.equal before.Id "test" "The document is not correct"
                Expect.isSome before.Sub "There should have been a sub-document"
                Expect.equal before.Sub.Value.Foo "a" "The document is not correct"
                Expect.equal before.Sub.Value.Bar "b" "The document is not correct"

                do! Save (Db.tableName, JsonDocument (Id = "test"))
                let! after = Find.ById<JsonDocument> (Db.tableName, "test")
                if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                let after = after :> JsonDocument
                Expect.equal after.Id "test" "The updated document is not correct"
                Expect.isNone after.Sub "The updated document is not correct"
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
            testList "All" [
                testTask "succeeds when there is data" {
                    use db = Db.buildDatabase ()

                    do! Insert (Db.tableName, SubDocument (Foo = "one", Bar = "two"))
                    do! Insert (Db.tableName, SubDocument (Foo = "three", Bar = "four"))
                    do! Insert (Db.tableName, SubDocument (Foo = "five", Bar = "six"))

                    let! results = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use db = Db.buildDatabase ()
                    let! results = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
                }
            ]
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
            testList "FirstByContains" [
                testTask "succeeds when a document is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByContains<JsonDocument> (Db.tableName, {| Value = "again" |})
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByContains<JsonDocument> (Db.tableName, {| Sub = {| Foo = "green" |} |})
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByContains<JsonDocument> (Db.tableName, {| Value = "absent" |})
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "firstByJsonPath" [
                testTask "succeeds when a document is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByJsonPath<JsonDocument> (Db.tableName, """$.Value ? (@ == "FIRST!")""")
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByJsonPath<JsonDocument> (Db.tableName, """$.Sub.Foo ? (@ == "green")""")
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc = Find.FirstByJsonPath<JsonDocument> (Db.tableName, """$.Id ? (@ == "nope")""")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
        ]
        testList "Document.Update" [
            testList "Full" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! Insert (Db.tableName,
                                JsonDocument (Id = "test", Sub = Some (SubDocument (Foo = "green", Bar = ""))))

                    let! before = Find.ById<JsonDocument> (Db.tableName, "test")
                    if isNull before then Expect.isTrue false "There should have been a document returned"
                    let before = before :> JsonDocument
                    Expect.equal before.Id "test" "The document is not correct"
                    Expect.isSome before.Sub "The document is not correct"
                    Expect.equal before.Sub.Value.Foo "green" "The document is not correct"
                    Expect.equal before.Sub.Value.Bar "" "The document is not correct"

                    do! Update.Full (Db.tableName, "test",
                                     JsonDocument (Id = "test", Sub = Some (SubDocument (Foo = "blue", Bar = "red"))))
                    let! after = Find.ById<JsonDocument> (Db.tableName, "test")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "test" "The updated document is not correct"
                    Expect.isSome after.Sub "The updated document is not correct"
                    Expect.equal after.Sub.Value.Foo "blue" "The updated document is not correct"
                    Expect.equal after.Sub.Value.Bar "red" "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.Full (Db.tableName, "test", {| Foo = "blue"; Bar = "red" |})
                }
            ]
            testList "FullFunc" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! Insert (Db.tableName, JsonDocument (Id = "green", Value = "lime"))

                    let! before = Find.ById<JsonDocument> (Db.tableName, "green")
                    if isNull before then Expect.isTrue false "There should have been a document returned"
                    let before = before :> JsonDocument
                    Expect.equal before.Id "green" "The document is not correct"
                    Expect.equal before.Value "lime" "The document is not correct"

                    do! Update.FullFunc (Db.tableName, System.Func<JsonDocument, string> _.Id,
                                         JsonDocument (Id = "green", Value = "primary"))
                    let! after = Find.ById<JsonDocument> (Db.tableName, "green")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "green" "The updated document is not correct"
                    Expect.equal after.Value "primary" "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.FullFunc (Db.tableName, System.Func<SubDocument, string> (fun _ -> "test"),
                                         SubDocument(Foo = "blue", Bar = "red"))
                }
            ]
            testList "PartialById" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.PartialById (Db.tableName, "one", {| NumValue = 44 |})
                    let! after = Find.ById<JsonDocument> (Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal (after :> JsonDocument).NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialById (Db.tableName, "test", {| Foo = "green" |})
                }
            ]
            testList "PartialByContains" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.PartialByContains (Db.tableName, {| Value = "purple" |}, {| NumValue = 77 |})
                    let! after = Count.ByContains (Db.tableName, {| NumValue = 77 |})
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialByContains (Db.tableName, {| Value = "burgundy" |}, {| Foo = "green" |})
                }
            ]
            testList "PartialByJsonPath" [
                testTask "succeeds when a document is updated" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()
                    
                    do! Update.PartialByJsonPath (Db.tableName, "$.NumValue ? (@ > 10)", {| NumValue = 1000 |})
                    let! after = Count.ByJsonPath (Db.tableName, "$.NumValue ? (@ > 999)")
                    Expect.equal after 2 "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use db = Db.buildDatabase ()

                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialByContains (Db.tableName, {| Value = "burgundy" |}, {| Foo = "green" |})
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
        testList "Document.Custom" [
            testList "Single" [
                testTask "succeeds when a row is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc =
                        Custom.Single ($"SELECT data FROM {Db.tableName} WHERE data -> 'Id' = @id",
                                       Seq.singleton (Tuple.Create ("@id", Sql.string "one")), FromData<JsonDocument>)
                    Expect.isNotNull doc "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! doc =
                        Custom.Single ($"SELECT data FROM {Db.tableName} WHERE data -> 'Id' = @id",
                                       Seq.singleton (Tuple.Create ("@id", Sql.string "eighty")),
                                       FromData<JsonDocument>)
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "List" [
                testTask "succeeds when data is found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs = Custom.List (Query.SelectFromTable(Db.tableName), Seq.empty, FromData<JsonDocument>)
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    let! docs =
                        Custom.List ($"SELECT data FROM {Db.tableName} WHERE data @? @path::jsonpath",
                                     Seq.singleton (Tuple.Create ("@path", Sql.string "$.NumValue ? (@ > 100)")),
                                     FromData<JsonDocument>)
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "NonQuery" [
                testTask "succeeds when operating on data" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Custom.NonQuery ($"DELETE FROM {Db.tableName}", Seq.empty)

                    let! remaining = Count.All(Db.tableName)
                    Expect.equal remaining 0 "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use db = Db.buildDatabase ()
                    do! loadDocs ()

                    do! Custom.NonQuery ($"DELETE FROM {Db.tableName} WHERE data @? @path::jsonpath",
                                         Seq.singleton (Tuple.Create ("@path", Sql.string "$.NumValue ? (@ > 100)")))

                    let! remaining = Count.All(Db.tableName)
                    Expect.equal remaining 5 "There should be 5 documents remaining in the table"
                }
            ]
            testTask "Scalar succeeds" {
                use db = Db.buildDatabase ()

                let! value =
                    Custom.Scalar ($"SELECT false AS test_value", Seq.empty,
                                   Func<RowReader, bool>(fun row -> row.bool "test_value"))
                Expect.isFalse value "The query should have returned the value false"
            }
        ]
    ]
    |> testSequenced


let all = testList "Documents" [ unitTests; integrationTests ]
