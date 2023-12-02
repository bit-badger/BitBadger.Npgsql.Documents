open System
open BitBadger.Npgsql.FSharp.Documents
open Npgsql
open Npgsql.FSharp

let args = Environment.GetCommandLineArgs()
if args.Length < 2 then
    printfn "Usage: ./MigrateToV2[.exe] [table_name] [id_field]\n"
    printfn "See https://bitbadger.solutions/open-source/postgres-documents/migrate-to-v2.html for more info"
    exit -1

let connStr = Environment.GetEnvironmentVariable "PGDOC_CONN_STR"
if isNull connStr then
    printfn "Environment PGDOC_CONN_STR not set; cannot continue"
    exit -1

let prompt =
    defaultArg (Option.ofObj (Environment.GetEnvironmentVariable "PGDOC_I_KNOW_WHAT_I_AM_DOING")) "false"
    |> (bool.Parse >> not)

try
    let dataSource = NpgsqlDataSourceBuilder(connStr).Build()
    let table      = args[1]
    let idField    = if args.Length < 3 then "Id" else args[2]

    printfn $"""Converting {table} to v2, using ID field "{idField}":"""
    let proceed =
        if prompt then
            printf "   Continue? [y/N] "
            let response = Console.ReadKey()
            response.Key = ConsoleKey.Y
        else
            true
    
    if proceed then
        printfn " - Creating unique ID index..."

        let keyQuery = (Definition.createKey table).Replace("'Id'", $"'{idField}'")
        printfn $"{keyQuery}"
        dataSource
        |> Sql.fromDataSource
        |> Sql.query keyQuery
        |> Sql.executeNonQuery
        |> ignore

        printfn " - Dropping old ID column..."
        dataSource
        |> Sql.fromDataSource
        |> Sql.query $"ALTER TABLE {table} DROP COLUMN id"
        |> Sql.executeNonQuery
        |> ignore
        
        printfn $"\n{table} converted successfully"
    else
        printfn $"\n{table} migration canceled"
with
| :? NpgsqlException as ex ->
    printfn $"PostgreSQL Exception: {ex.SqlState}"
    printfn $"  {ex.Message}"
    exit -1

exit 0
