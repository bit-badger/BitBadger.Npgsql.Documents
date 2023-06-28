open Expecto

let allTests = testList "BitBadger.Npgsql" [ FSharpTests.all; CSharpTests.all ]

[<EntryPoint>]
let main args = runTestsWithArgs defaultConfig args allTests
