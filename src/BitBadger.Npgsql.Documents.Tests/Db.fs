/// Database access utilities
module Db

open System
open Npgsql
open Npgsql.FSharp
open ThrowawayDb.Postgres

/// The name of the table used for testing
let tableName = "test_table"

/// The host for the database
let dbHost =
    match Environment.GetEnvironmentVariable "BitBadger.Npgsql.Documents.DbHost" with
    | host when String.IsNullOrWhiteSpace host -> "localhost"
    | host -> host

/// The port for the database
let dbPort =
    match Environment.GetEnvironmentVariable "BitBadger.Npgsql.Documents.DbPort" with
    | port when String.IsNullOrWhiteSpace port -> 5432
    | port -> Int32.Parse port

/// The database itself
let dbDatabase =
    match Environment.GetEnvironmentVariable "BitBadger.Npgsql.Documents.DbDatabase" with
    | db when String.IsNullOrWhiteSpace db -> "postgres"
    | db -> db

/// The user to use in connecting to the database
let dbUser =
    match Environment.GetEnvironmentVariable "BitBadger.Npgsql.Documents.DbUser" with
    | user when String.IsNullOrWhiteSpace user -> "postgres"
    | user -> user

/// The password to use for the database
let dbPassword =
    match Environment.GetEnvironmentVariable "BitBadger.Npgsql.Documents.DbPwd" with
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
    NpgsqlDataSourceBuilder(cStr).Build()


open BitBadger.Npgsql.FSharp.Documents

/// Build the throwaway database
let buildDatabase () =
    
    let database = ThrowawayDatabase.Create connStr

    let sqlProps = Sql.connect database.ConnectionString
    
    sqlProps
    |> Sql.query (Definition.createTable tableName)
    |> Sql.executeNonQuery
    |> ignore
    sqlProps
    |> Sql.query (Definition.createKey tableName)
    |> Sql.executeNonQuery
    |> ignore

    Configuration.useDataSource (mkDataSource database.ConnectionString)

    database
