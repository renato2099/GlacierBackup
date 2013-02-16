package org.bg.amazon.glacier;

/**
 * Custom exception class for the Glacier Service
 */
public class GlacierException extends RuntimeException {


  /**
   * Default serial version number 
   */
  private static final long serialVersionUID = 1L;

  /**
   * Constructor for the GlacierException class
   * @param message
   */
  public GlacierException(String message) {
    super(message);
  }
}
