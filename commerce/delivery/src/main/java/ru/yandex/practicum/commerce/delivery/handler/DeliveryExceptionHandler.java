package ru.yandex.practicum.commerce.delivery.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.delivery.exception.NoDeliveryFoundException;
import ru.yandex.practicum.commerce.interactionapi.error.CommonExceptionHandler;
import ru.yandex.practicum.commerce.interactionapi.error.ErrorResponse;

@Slf4j
@RestControllerAdvice
public class DeliveryExceptionHandler extends CommonExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNoDeliveryFoundException(NoDeliveryFoundException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.NOT_FOUND, "Not Found");
    }
}
