package pt.isel.leic.cs4k.demo.http.models.output

import com.fasterxml.jackson.annotation.JsonInclude
import org.springframework.http.ResponseEntity
import java.net.URI

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ErrorResponse(
    val type: URI,
    val title: String? = null,
    val detail: String? = null,
    val instance: URI? = null
) {

    companion object {

        const val DOC_LOCATION = "https://cs4k-demo.com/errors/"
        private const val MEDIA_TYPE = "application/problem+json"

        fun response(
            status: Int,
            type: URI,
            title: String? = null,
            detail: String? = null,
            instance: URI? = null
        ): ResponseEntity<Any> {
            return ResponseEntity
                .status(status)
                .header("Content-Type", MEDIA_TYPE)
                .body(ErrorResponse(type, title, detail, instance))
        }
    }

    object Type {
        val INTERNAL_SERVER_ERROR = URI(DOC_LOCATION + "internalServerError")
    }

    object Title {
        const val INTERNAL_SERVER_ERROR = "Unknown error occurred."
    }
}
