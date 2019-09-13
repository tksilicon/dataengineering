package com.turing.dataengineering.exceptions;

import java.io.FileNotFoundException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Exception handler for all exceptions.
 */
@RestControllerAdvice
public class TurningAppExceptionHandler extends ResponseEntityExceptionHandler {

	/**
	 * Exception handler 
	 */
	@ExceptionHandler(StorageFileNotFoundException.class)
	public ResponseEntity<Object> handleStorageFileNotFound(StorageFileNotFoundException ex) {
		ApiErrorResponse errorResponse = new ApiErrorResponse("DAT_02", ex.getMessage(),
				ex.getCause().getMessage(), String.valueOf(HttpStatus.BAD_REQUEST));

		return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
	}

	/**
	 * Exception FileNotFoundException
	 */
	@ExceptionHandler(FileNotFoundException.class)
	public ResponseEntity<Object> handleFileNotFoundException(FileNotFoundException ex) {
		ApiErrorResponse errorResponse = new ApiErrorResponse("DAT_03", ex.getMessage(),
				ex.getCause().getMessage(), String.valueOf(HttpStatus.BAD_REQUEST));

		return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
	}
   

	
	/**
     * Handle other exception
     */
	@ExceptionHandler(Exception.class)
	public final ResponseEntity<Object> handleAllExceptions(Exception ex, WebRequest request) {
		ApiErrorResponse errorResponse = new ApiErrorResponse("DAT_04", ex.getMessage(),
				ex.getCause().getMessage(), String.valueOf(HttpStatus.NOT_FOUND));
	  return new ResponseEntity<>(errorResponse , HttpStatus.NOT_FOUND);
	}

    
}
