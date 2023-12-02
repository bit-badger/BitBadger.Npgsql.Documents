namespace BitBadger.Npgsql.Documents

/// The type of index to generate for the document
[<Struct>]
type DocumentIndex =
    /// A GIN index with standard operations (all operators supported)
    | Full = 0
    /// A GIN index with JSONPath operations (optimized for @>, @?, @@ operators)
    | Optimized = 1
