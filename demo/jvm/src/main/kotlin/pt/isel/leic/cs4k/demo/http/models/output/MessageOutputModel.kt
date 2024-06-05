package pt.isel.leic.cs4k.demo.http.models.output

import com.fasterxml.jackson.annotation.JsonInclude

data class MessageOutputModel(
    val message: String,
    @JsonInclude(JsonInclude.Include.NON_NULL) val subscribedNode: String? = null,
    @JsonInclude(JsonInclude.Include.NON_NULL) val publishingNode: String? = null
)
