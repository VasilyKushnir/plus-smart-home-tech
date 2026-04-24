package ru.yandex.practicum.commerce.shoppingcart.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionapi.error.CommonExceptionHandler;
import ru.yandex.practicum.commerce.interactionapi.error.ErrorResponse;
import ru.yandex.practicum.commerce.shoppingcart.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.shoppingcart.exception.NotAuthorizedUserException;

@Slf4j
@RestControllerAdvice
public class ShoppingCartExceptionHandler extends CommonExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ErrorResponse handleNotAuthorizedUserException(NotAuthorizedUserException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.UNAUTHORIZED, "Unauthorized");
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleNoProductsInShoppingCartException(NoProductsInShoppingCartException e) {
        log.error(e.getMessage());
        logError(e);
        return toErrorResponse(e, HttpStatus.BAD_REQUEST, "Bad Request");
    }
}
