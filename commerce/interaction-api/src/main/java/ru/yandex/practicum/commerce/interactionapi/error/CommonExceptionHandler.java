package ru.yandex.practicum.commerce.interactionapi.error;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

@Slf4j
public abstract class CommonExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleInternalError(Exception e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error");
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleBadRequest(RuntimeException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.BAD_REQUEST, "Bad Request");
    }

    protected void logError(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        log.error(sw.toString());
    }

    protected static ErrorResponse toErrorResponse(Exception e, HttpStatus status, String message) {
        return ErrorResponse.builder()
                .cause(e.getCause())
                .stackTrace(Arrays.asList(e.getStackTrace()))
                .httpStatus(status.toString())
                .userMessage(e.getMessage())
                .message(message)
                .suppressed(Arrays.asList(e.getSuppressed()))
                .build();
    }
}
