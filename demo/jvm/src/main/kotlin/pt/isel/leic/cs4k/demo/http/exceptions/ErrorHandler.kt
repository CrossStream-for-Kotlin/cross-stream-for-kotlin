package pt.isel.leic.cs4k.demo.http.exceptions

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import pt.isel.leic.cs4k.demo.http.models.output.ErrorResponse

@ControllerAdvice
class ErrorHandler {

    @ExceptionHandler(Exception::class)
    fun handler(exception: Exception): ResponseEntity<*> {
        return ErrorResponse.response(
            500,
            ErrorResponse.Type.INTERNAL_SERVER_ERROR,
            ErrorResponse.Title.INTERNAL_SERVER_ERROR
        )
    }
}
