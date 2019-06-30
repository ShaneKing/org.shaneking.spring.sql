package org.shaneking.spring.sql.exception;

public class SqlException extends RuntimeException {
  public SqlException(Throwable cause) {
    super(cause);
  }

  public SqlException(String message) {
    super(message);
  }
}
