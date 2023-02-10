open Expecto

let allTests = testList "All" [ FSharpTests.all ]

[<EntryPoint>]
let main args = runTestsWithArgs defaultConfig args allTests
