package Utilities

object Logging extends App {

  println(classOf[Logging].getName)

  val log = org.apache.logging.log4j.LogManager.getLogger(classOf[Logging].getName)
  log.debug("Hello, this is a debug message")
  log.info("Hello, this is an info message")
  log.warn("This is a warning.")
  log.error("This is an error!")

}

class Logging
//empty class just to give name to our Logger


