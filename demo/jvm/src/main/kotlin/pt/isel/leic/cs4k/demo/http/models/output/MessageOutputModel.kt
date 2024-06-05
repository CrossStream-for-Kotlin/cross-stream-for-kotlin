package pt.isel.leic.cs4k.demo.http.models.output

data class MessageOutputModel(
    val subscribedNode: String,
    val message: String,
    val publishingNode: String
)
