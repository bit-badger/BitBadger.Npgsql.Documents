namespace BitBadger.Npgsql.Documents

/// The required document serialization implementation
type IDocumentSerializer =
    
    /// Serialize an object to a JSON string
    abstract Serialize<'T> : 'T -> string
    
    /// Deserialize a JSON string into an object
    abstract Deserialize<'T> : string -> 'T
