open Expecto

let allTests = testList "Npgsql" [ FSharpTests.all; CSharpTests.all ]

[<EntryPoint>]
let main args = runTestsWithArgs defaultConfig args allTests
