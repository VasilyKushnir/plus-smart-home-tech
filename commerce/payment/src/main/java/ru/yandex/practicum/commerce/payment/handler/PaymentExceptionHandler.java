package ru.yandex.practicum.commerce.payment.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionapi.error.CommonExceptionHandler;
import ru.yandex.practicum.commerce.interactionapi.error.ErrorResponse;
import ru.yandex.practicum.commerce.payment.exception.NoPaymentFoundException;
import ru.yandex.practicum.commerce.payment.exception.NotEnoughInfoInOrderToCalculateException;

@Slf4j
@RestControllerAdvice
public class PaymentExceptionHandler extends CommonExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleNotEnoughInfoInOrderToCalculateException(NotEnoughInfoInOrderToCalculateException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.BAD_REQUEST, "Bad Request");
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNoPaymentFoundException(NoPaymentFoundException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.NOT_FOUND, "Not Found");
    }
}
