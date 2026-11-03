package ru.yandex.practicum.commerce.order.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionapi.error.CommonExceptionHandler;
import ru.yandex.practicum.commerce.interactionapi.error.ErrorResponse;
import ru.yandex.practicum.commerce.order.exception.NoOrderFoundException;

@Slf4j
@RestControllerAdvice
public class OrderExceptionHandler extends CommonExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleNoOrderFoundException(NoOrderFoundException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.BAD_REQUEST, "Bad Request");
    }
}
